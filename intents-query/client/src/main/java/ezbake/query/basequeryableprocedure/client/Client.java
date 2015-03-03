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

import com.cloudera.impala.extdatasource.thrift.TBinaryPredicate;
import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TCloseResult;
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.extdatasource.v1.ExternalDataSource;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.intents.common.ImpalaAppToken;
import ezbake.intents.common.RedisUtils;
import ezbake.query.basequeryableprocedure.ColumnData;
import ezbake.query.basequeryableprocedure.ColumnDataValues;
import ezbake.query.basequeryableprocedure.ColumnDataValues._Fields;
import ezbake.query.basequeryableprocedure.GetPageResult;
import ezbake.query.basequeryableprocedure.PrepareStats;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Client implements ExternalDataSource {
    private static Logger appLog = LoggerFactory.getLogger(Client.class);

    private EzbakeAppHandler appHandler;

    // Cached copy of the configuration object
    private EzConfiguration configuration;

    private RedisUtils redisUtils;

    /**
     *
     */
    public Client() {
        appLog.info("Creating the Client app");

        try {
            // TODO: how to configure this on production?
            configuration = new EzConfiguration(new DirectoryConfigurationLoader(Paths.get("/opt/ezbake/conf")));
            redisUtils = new RedisUtils(configuration);
        } catch (Exception e) {
            appLog.error("error create client", e);
        }
    }

    //
    // Package protected for the unit tests
    //
    void setRedisUtils(RedisUtils redisUtils) {
        this.redisUtils = redisUtils;
    }

    /**
     *
     */
    @Override
    public TPrepareResult prepare(TPrepareParams params) {
        appLog.info("prepare called with params {}", params.toString());

        List<List<TBinaryPredicate>> predicates = params.getPredicates();
        String tableName = params.getTable_name();
        String initString = params.getInit_string();

        TPrepareResult result = new TPrepareResult();
        result.setStatus(getOkStatus());

        String secUuid = retrieveSecUuidFromPredicate(predicates);
        if (null == secUuid) {
            result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, "Error: No SecUuid found"));
        } else {
            EzSecurityToken securityToken;
            try {
                securityToken = redisUtils.retrieveSecurityToken(secUuid);
            } catch (TException e) {
                appLog.error("retrieveSecurityToken failed", e);
                result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, "Error: Failed retrieve SecurityToken: " + e.getMessage()));
                return result;
            }

            if (null == securityToken) {
                // we must have the Security Token
                String msg = "ERROR: No EZSecurity Token available for the SecUuid(" + secUuid + ")";
                result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, msg));
            } else {
                // Impala creates a Client object for the prepare call and another for the
                // open/getNext/close calls(2 total)
                try {
                    PrepareStats ps = getAppHandler().prepare(tableName, initString, predicates, securityToken);
                    appLog.info("app prepare status: {}", ps.toString());
                    result.setNum_rows_estimate(ps.getNumberOfRowsEstimate());
                    result.setAccepted_conjuncts(ps.getAcceptedConjuncts());

                } catch (EzbakeAppHandlerException e) {
                    appLog.error("error prepare", e);
                    String msg = "ERROR: ImpalaAppHandlerException - " + e.getMessage();
                    result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, msg));
                }
            }
        }

        appLog.debug("finished with prepare call");

        return result;
    }

    /**
     *
     */
    @Override
    public TOpenResult open(TOpenParams params) {
        appLog.info("open called with params {}", params.toString());

        // TODO how to configure the batch_size?
//        params.setBatch_size(100);

        TOpenResult result = new TOpenResult();
        result.setStatus(getOkStatus());

        List<List<TBinaryPredicate>> predicates = params.getPredicates();
        String secUuid = retrieveSecUuidFromPredicate(predicates);

        if (null == secUuid) {
            result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, "Error: No SecUuid found"));
        } else {
            try {
                ImpalaAppToken appToken = redisUtils.openImpalaAppToken(params.getTable_name(),
                        params.getBatch_size(),
                        params.getRow_schema(),
                        predicates,
                        secUuid);

                result.setScan_handle(appToken.getScanHandle());
            } catch (TException e) {
                appLog.error("openImpalaAppToken failed", e);
                result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, "Error: Failed get ImpalaAppToken: " + e.getMessage()));
            }
        }

        appLog.debug("Returning from open call");

        return result;
    }

    @Override
    public TGetNextResult getNext(TGetNextParams params) {
        appLog.info("getNext called with {}", params.toString());

        String scanHandle = params.getScan_handle();

        TGetNextResult result = new TGetNextResult();
        result.setStatus(getOkStatus());

        GetPageResult pageResult;

        try {
            pageResult = getAppHandler().getNext(scanHandle);
            result.setEos(pageResult.isEos());
        } catch (TException e) {
            appLog.error("getNext() failed", e);
            result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, e.getMessage()));

            return result;
        }

        ImpalaAppToken appToken;
        TRowBatch tRowBatch;

        try {
            appToken = redisUtils.getImpalaAppToken(scanHandle);
        } catch (TException e) {
            appLog.error("getImpalaAppToken() failed", e);
            result.setStatus(getFailedStatus(TStatusCode.INTERNAL_ERROR, e.getMessage()));
            return result;
        }

        tRowBatch = generateTRowBatch(pageResult, appToken.getTableSchema());
        result.setRows(tRowBatch);

        // increment the offset to prepare for next getNext()
        redisUtils.incrementImpalaAppTokenOffSet(scanHandle, tRowBatch.getNum_rows());

        return result;
    }

    private TRowBatch generateTRowBatch(GetPageResult pageResult, TTableSchema tableSchema) {
        TRowBatch rowBatch = new TRowBatch();

        List<ColumnData> columnDataList = pageResult.getRows().getCols();
        Iterator<ColumnData> columnDataListIter = columnDataList.iterator();
        Iterator<TColumnDesc> schemaIter = tableSchema.getColsIterator();

        // Find the number of rows so that we can create an TColumnData object of the same dimension.
        // The isNull list will represent the number of rows that are being returned.
        int size = columnDataList.get(0).getIs_nullSize();
        rowBatch.setNum_rows(size);

        Boolean[] booleanArray = new Boolean[size];
        Arrays.fill(booleanArray, Boolean.TRUE);
        TColumnData secUuidColumnData = new TColumnData();
        secUuidColumnData.setIs_null(Arrays.asList(booleanArray));

        while (schemaIter.hasNext()) {
            TColumnDesc columnDesc = schemaIter.next();

            if ("secuuid".equalsIgnoreCase(columnDesc.getName())) {
                // EzBake apps should not worry about the Security UUID(secUUID) so if secUUID is
                // in the the TTableSchema then this client will handle it by creating a TColumnData
                // object with NULL values representing the SecUUID and insert it into the appropriate
                // location of the TRowBatch object based on the TTableSchema
                rowBatch.addToCols(secUuidColumnData);
            } else {
                ColumnData columnData = columnDataListIter.next();
                TColumnData validData = new TColumnData();
                validData.setIs_null(columnData.getIs_null());

                ColumnDataValues values = columnData.getValues();
                if (values != null) {
                    if (values.isSet(_Fields.BINARY_VALS)) {
                        validData.setBinary_vals(values.getBinary_vals());
                    } else if (values.isSet(_Fields.BOOL_VALS)) {
                        validData.setBool_vals(values.getBool_vals());
                    } else if (values.isSet(_Fields.BYTE_VALS)) {
                        validData.setByte_vals(values.getByte_vals());
                    } else if (values.isSet(_Fields.DOUBLE_VALS)) {
                        validData.setDouble_vals(values.getDouble_vals());
                    } else if (values.isSet(_Fields.INT_VALS)) {
                        validData.setInt_vals(values.getInt_vals());
                    } else if (values.isSet(_Fields.LONG_VALS)) {
                        validData.setLong_vals(values.getLong_vals());
                    } else if (values.isSet(_Fields.SHORT_VALS)) {
                        validData.setShort_vals(values.getShort_vals());
                    } else if (values.isSet(_Fields.STRING_VALS)) {
                        validData.setString_vals(values.getString_vals());
                    }
                }

                rowBatch.addToCols(validData);
            }
        }

        return rowBatch;
    }

    /**
     * Part of the External Data Source API
     *
     * @param params
     * @return TCloseResult
     */
    @Override
    public TCloseResult close(TCloseParams params) {
        TCloseResult result = new TCloseResult();
        result.setStatus(getOkStatus());

        appLog.info("close called with params: {}", params.toString());

        // mark the entry in Redis as closed using the scan handle
        if (!redisUtils.closeImpalaAppToken(params.getScan_handle())) {
            String msg = "Failed to close the ImpalaAppToken";
            result.setStatus(getFailedStatus(TStatusCode.RUNTIME_ERROR, msg));
        }

        if (appHandler != null) {
            appHandler.close();
        }
        return result;
    }

    /**
     * Helper method to generate an OK TStatus object
     *
     * @return TStatus
     */
    private TStatus getOkStatus() {
        TStatus status = new TStatus();
        status.setStatus_code(TStatusCode.OK);

        return status;
    }

    /**
     * Helper method to generate a Failed TStatus object
     *
     * @param errorMsg
     * @return TStatus
     */
    private TStatus getFailedStatus(TStatusCode statusCode, String errorMsg) {
        List<String> msgs = new ArrayList<String>();
        msgs.add(errorMsg);

        return getFailedStatus(statusCode, msgs);
    }

    /**
     * Helper method to generate a Failed TStatus object
     *
     * @param errorMsgs
     * @return TStatus
     */
    private TStatus getFailedStatus(TStatusCode statusCode, List<String> errorMsgs) {
        TStatus status = new TStatus();
        status.setStatus_code(statusCode);
        status.setError_msgs(errorMsgs);

        return status;
    }

    /**
     * Iterate through the predicates looking for the one which defines what the Security UUID is.
     * It should always be present in the list(i.e. always sent down by Impala)
     *
     * @param predicatesList
     * @return String value of the Security UUID (secUUID)
     */
    private String retrieveSecUuidFromPredicate(List<List<TBinaryPredicate>> predicatesList) {
        Iterator<List<TBinaryPredicate>> outerIter = predicatesList.iterator();
        while (outerIter.hasNext()) {
            List<TBinaryPredicate> inner = outerIter.next();
            Iterator<TBinaryPredicate> predicateItr = inner.iterator();
            while (predicateItr.hasNext()) {
                TBinaryPredicate predicate = predicateItr.next();
                String name = predicate.getCol().getName();
                if ("secuuid".equalsIgnoreCase(name)) {
                    String uuid = predicate.getValue().getString_val();
                    appLog.debug("Secuuid Found and its Value: {}", uuid);
                    return uuid;
                }
            }
        }

        appLog.warn("Didn't find the SecUUID to retrieve the UserToken");

        return null;
    }

    private EzbakeAppHandler getAppHandler() {
        if (appHandler == null) {
            appHandler = new EzbakeAppHandler(configuration);
        }

        return appHandler;
    }

    void setAppHandler(EzbakeAppHandler appHandler) {
        this.appHandler = appHandler;
    }
}
