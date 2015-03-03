/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.query.basequeryableprocedure.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.extdatasource.thrift.TBinaryPredicate;
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.thrift.TColumnValue;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.ins.thrift.gen.AppService;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.intents.common.ImpalaAppToken;
import ezbake.intents.common.RedisUtils;
import ezbake.query.basequeryableprocedure.BaseQueryableProcedure;
import ezbake.query.basequeryableprocedure.GetPageResult;
import ezbake.query.basequeryableprocedure.Page;
import ezbake.query.basequeryableprocedure.PrepareStats;
import ezbake.query.intents.BinaryOperator;
import ezbake.query.intents.BinaryPredicate;
import ezbake.query.intents.ColumnValue;
import ezbake.query.intents.IntentType;
import ezbake.thrift.ThriftClientPool;

public class EzbakeAppHandler {
    private static Logger appLog = LoggerFactory.getLogger(EzbakeAppHandler.class);

    private ThriftClientPool pool;
    private RedisUtils redisUtils;

    /**
     * @param config
     */
    public EzbakeAppHandler(EzConfiguration config) {
        pool = new ThriftClientPool(config.getProperties());
        redisUtils = new RedisUtils(config);
        appLog.info("EzbakeAppHandler initialized");
    }

    public void close() {
        if (this.pool != null) {
            this.pool.close();
        }
    }

    /**
     * @param tableName
     * @param initString
     * @param predicates
     * @param securityToken
     * @return
     * @throws EzbakeAppHandlerException
     */
    public PrepareStats prepare(
            String tableName, String initString, List<List<TBinaryPredicate>> predicates, EzSecurityToken securityToken)
            throws EzbakeAppHandlerException {
        appLog.info(
                "prepare called with tableName({}) initString({}) predicates({})", tableName, initString, predicates);

        AppService app;

        try {
            app = getAppForIntent(tableName);
        } catch (TException e) {
            appLog.error("getAppsForIntent failed", e);
            throw new EzbakeAppHandlerException("ERROR: Failed to get apps");
        }

        // no such app
        if (app == null) {
            throw new EzbakeAppHandlerException("ERROR: Failed to find app");
        }

        BaseQueryableProcedure.Client c;
        try {
            c = getClient(app);
        } catch (TException e) {
            appLog.error("get BaseQueryableProcedure client failed", e);
            throw new EzbakeAppHandlerException("ERROR: Failed to retrieve Client");
        }

        PrepareStats stats = null;

        try {
            stats = c.prepare(tableName, initString, convertPredicates(predicates), securityToken);
        } catch (TException e) {
            throw new EzbakeAppHandlerException(e);
        } finally {
            if (c != null) {
                pool.returnToPool(c);
                c = null;
            }
        }

        return stats;
    }

    /**
     * @param scanHandle
     * @return
     */
    public GetPageResult getNext(String scanHandle) throws TException {
        BaseQueryableProcedure.Client c = null;
        ImpalaAppToken token = redisUtils.getImpalaAppToken(scanHandle);
        GetPageResult pageResult = null;

        appLog.info("getNext with the scanHandle: {}, offset: {}", scanHandle, token.getOffset());

        try {
            AppService app = getAppForIntent(token.getTableName());
            if (app == null) {
                return pageResult;
            }
            c = getClient(app);
            Page page = new Page();
            page.setOffset(token.getOffset());
            page.setPagesize(token.getBatchsize());

            IntentType intent = IntentType.valueOf(token.getIntent().toUpperCase());
            appLog.debug("Intent is : {}", intent.toString());

            List<List<TBinaryPredicate>> predicates = removeSecUuidPredicate(token.getPredicates());
            List<List<BinaryPredicate>> converted = convertPredicates(predicates);

            appLog.debug("Predicates with the secuuid: {}", token.getPredicates());
            appLog.debug("Predicates : {}", predicates);
            appLog.debug("Converted to : {}", converted);

            List<String> columnnames = new ArrayList<String>();
            TTableSchema schema = token.getTableSchema();
            List<TColumnDesc> list = schema.getCols();

            for (TColumnDesc columnDesc : list) {
                columnnames.add(columnDesc.getName());
            }

            pageResult = c.getPage(intent, page, columnnames, converted, token.getUserToken());
        } finally {
            if (c != null) {
                pool.returnToPool(c);
                c = null;
            }
        }

        return pageResult;
    }

    /**
     * @param predicates
     * @return
     */
    private List<List<TBinaryPredicate>> removeSecUuidPredicate(List<List<TBinaryPredicate>> predicates) {
        List<List<TBinaryPredicate>> list = new ArrayList<List<TBinaryPredicate>>();
        boolean hasSecUuid = false;

        Iterator<List<TBinaryPredicate>> iter = predicates.iterator();
        while (iter.hasNext()) {
            List<TBinaryPredicate> predicateList = iter.next();
            Iterator<TBinaryPredicate> predicateIter = predicateList.iterator();

            while (predicateIter.hasNext()) {
                TBinaryPredicate binaryPredicate = predicateIter.next();
                if ("secuuid".equalsIgnoreCase(binaryPredicate.getCol().getName())) {
                    hasSecUuid = true;
                    break;
                }
            }

            if (!hasSecUuid) {
                list.add(predicateList);
            }

            hasSecUuid = false;
        }

        return list;
    }

    private AppService getAppForIntent(String tableName) throws TException {

        InternalNameService.Client client = null;
        String appName = tableName.substring(0, tableName.lastIndexOf("_"));
        String intentName = tableName.substring(tableName.lastIndexOf("_") + 1);

        try {
            client = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
            Set<AppService> apps = client.appsThatSupportIntent(intentName.toUpperCase());
            appLog.debug("Apps : {}", apps.toString());

            for (AppService appService : apps) {
                if (appService.getApplicationName().equalsIgnoreCase(appName)) {
                    return appService;
                }
            }
        } finally {
            if (client != null) {
                pool.returnToPool(client);
            }
        }

        appLog.warn("no app found for intent({}) with name ({)", intentName, appName);
        return null;
    }

    private BaseQueryableProcedure.Client getClient(AppService appService) throws TException {
        return pool.getClient(
                appService.getApplicationName(), appService.getServiceName(), BaseQueryableProcedure.Client.class);
    }

    /**
     * @param predicates
     * @return
     */
    private List<List<BinaryPredicate>> convertPredicates(List<List<TBinaryPredicate>> predicates) {
        List<List<BinaryPredicate>> converted = new ArrayList<List<BinaryPredicate>>();

        for (Iterator<List<TBinaryPredicate>> itr = predicates.iterator(); itr.hasNext(); ) {
            List<TBinaryPredicate> innerList = itr.next();
            List<BinaryPredicate> newInner = new ArrayList<BinaryPredicate>();
            for (Iterator<TBinaryPredicate> predicateItr = innerList.iterator(); predicateItr.hasNext(); ) {
                newInner.add(convertPredicate(predicateItr.next()));
            }

            converted.add(newInner);
        }

        return converted;
    }

    /**
     * @param oldPredicate
     * @return
     */
    private BinaryPredicate convertPredicate(TBinaryPredicate oldPredicate) {
        BinaryPredicate newPredicate = new BinaryPredicate();

        newPredicate.setColumnName(oldPredicate.getCol().getName());

        switch (oldPredicate.getOp()) {
            case EQ:
                newPredicate.setBinaryOperator(BinaryOperator.EQ);
                break;
            case NE:
                newPredicate.setBinaryOperator(BinaryOperator.NE);
                break;
            case GE:
                newPredicate.setBinaryOperator(BinaryOperator.GE);
                break;
            case GT:
                newPredicate.setBinaryOperator(BinaryOperator.GT);
                break;
            case LE:
                newPredicate.setBinaryOperator(BinaryOperator.LE);
                break;
            case LT:
                newPredicate.setBinaryOperator(BinaryOperator.LT);
                break;
            default:
                break;
        }

        ColumnValue value = new ColumnValue();
        TColumnValue oldValue = oldPredicate.getValue();

        if (oldValue.isSetString_val()) {
            value.setStringValue(oldPredicate.getValue().getString_val());
        } else if (oldValue.isSetInt_val()) {
            value.setIntegerValue(oldPredicate.getValue().getInt_val());
        } else if (oldValue.isSetBool_val()) {
            value.setBoolValue(oldPredicate.getValue().isBool_val());
        } else if (oldValue.isSetBigint_val()) {
            value.setLongValue(oldPredicate.getValue().getBigint_val());
        } else if (oldValue.isSetSmallint_val()) {
            // TODO: do we need a corresponding 'short' method???
            value.setIntegerValue(oldPredicate.getValue().getSmallint_val());
        } else if (oldValue.isSetDouble_val()) {
            value.setDoubleValue(oldPredicate.getValue().getDouble_val());
        } else {
            // Missing one????
        }

        newPredicate.setValue(value);

        return newPredicate;
    }
}
