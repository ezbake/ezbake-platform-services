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

package ezbake.intent.query.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ezbake.query.intents.QueryResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.EzConfiguration;
import ezbake.intent.query.processor.IntentImpalaClient;
import ezbake.intent.query.processor.QueryGeneratorImpl;
import ezbake.intent.query.processor.SecurityStorageRedisImpl;
import ezbake.intent.query.thrift.IntentsQueryService;
import ezbake.query.intents.Activity;
import ezbake.query.intents.BinaryOperator;
import ezbake.query.intents.ColumnDef;
import ezbake.query.intents.ColumnType;
import ezbake.query.intents.Equipment;
import ezbake.query.intents.Event;
import ezbake.query.intents.Facility;
import ezbake.query.intents.Image;
import ezbake.query.intents.IntentType;
import ezbake.query.intents.IntentTypeTable;
import ezbake.query.intents.Issue;
import ezbake.query.intents.Location;
import ezbake.query.intents.Metadata;
import ezbake.query.intents.Operator;
import ezbake.query.intents.Person;
import ezbake.query.intents.Query;
import ezbake.query.intents.Relationship;
import ezbake.query.intents.Report;
import ezbake.query.intents.State;
import ezbake.query.intents.Unit;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.util.AuditEvent;
import ezbake.util.AuditLogger;

public class QueryService extends EzBakeBaseThriftService implements IntentsQueryService.Iface {
    private Logger appLog = LoggerFactory.getLogger(QueryService.class);
    private AuditLogger auditLogger = AuditLogger.getAuditLogger(QueryService.class);

    private QueryGeneratorImpl queryGenerator = null;
    private IntentImpalaClient impalaClient = null;
    private EzbakeSecurityClient ezbakeSecurityClient;

    @Override
    public QueryResult query(Query query, EzSecurityToken securityToken) throws TException {
        validateSecurityToken(securityToken);

        appLog.info("query API called, executing");
        AuditEvent event = AuditEvent.event("query", securityToken).arg("query", query.toString());
        if(query.isSetPage()) {
            event.arg("page", query.getPage());
        }

        try {
            //generate SQL string
            String sqlString = queryGenerator.generateSQLString(query, securityToken);
            appLog.info("Generated SQL String: " + sqlString);

            QueryResult result = new QueryResult();
            //call impala client with the query to hand over query to impala for processing
            result.setResultSet( impalaClient.queryImpala(sqlString));

            if(query.isSetPage()){
                result.setOffset(query.getPage().getOffset());
                result.setPageSize((query.getPage().getPageSize()));
            }
            return result;
        } catch (TException e) {
            event.failed();
            event.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public Metadata getMetadata(EzSecurityToken securityToken) throws EzSecurityTokenException {
        validateSecurityToken(securityToken);
        AuditEvent event = AuditEvent.event("getMetadata", securityToken);

        try {
            // operators
            List<Operator> operators = new ArrayList<>();
            for (BinaryOperator operator : BinaryOperator.values()) {
                operators.add(getOperator(operator));
            }

            // relations
            List<String> relations = new ArrayList<>();
            relations.add("AND");
            relations.add("OR");

            // intent types
            List<IntentTypeTable> intentTypes = new ArrayList<>();
            for (IntentType type : IntentType.values()) {
                intentTypes.add(getIntentType(type));
            }

            return new Metadata(operators, relations, intentTypes);
        } catch (Exception e) {
            event.failed();
            event.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public TProcessor getThriftProcessor() {
        try {
            init();
            return new IntentsQueryService.Processor<>(this);
        } catch (Exception e) {
            //swallow
            appLog.info("Error Occurred Initializing QueryService");
            return null;
        }
    }

    @Override
    public void shutdown() {
        if (this.impalaClient != null) {
            this.impalaClient.shutdown();
        }
    }

    private void init() throws Exception {
        appLog.info("Initializing Query Service");

        final EzConfiguration configuration = new EzConfiguration();

        this.queryGenerator = new QueryGeneratorImpl(configuration, new SecurityStorageRedisImpl(configuration));
        this.impalaClient = new IntentImpalaClient();
        this.ezbakeSecurityClient = new EzbakeSecurityClient(configuration.getProperties());

        appLog.info("Successfully Initialized Query Service");
    }

    private void validateSecurityToken(EzSecurityToken token) throws EzSecurityTokenException {
        this.ezbakeSecurityClient.validateReceivedToken(token);
    }

    private IntentTypeTable getIntentType(IntentType type) {
        String text = getIntentTypeText(type);
        Collection<FieldMetaData> metadata = null;
        List<ColumnDef> columns = new ArrayList<>();

        //ACTIVITY, EQUIPMENT, EVENT, FACILITY, IMAGE, ISSUE, LOCATION, PERSON, REPORT, RELATIONSHIP, STATE, UNIT
        switch (type) {
            case ACTIVITY:
                metadata = Activity.metaDataMap.values();
                break;
            case EQUIPMENT:
                metadata = Equipment.metaDataMap.values();
                break;
            case EVENT:
                metadata = Event.metaDataMap.values();
                break;
            case FACILITY:
                metadata = Facility.metaDataMap.values();
                break;
            case IMAGE:
                metadata = Image.metaDataMap.values();
                break;
            case ISSUE:
                metadata = Issue.metaDataMap.values();
                break;
            case LOCATION:
                metadata = Location.metaDataMap.values();
                break;
            case PERSON:
                metadata = Person.metaDataMap.values();
                break;
            case REPORT:
                metadata = Report.metaDataMap.values();
                break;
            case RELATIONSHIP:
                metadata = Relationship.metaDataMap.values();
                break;
            case STATE:
                metadata = State.metaDataMap.values();
                break;
            case UNIT:
                metadata = Unit.metaDataMap.values();
                break;
        }

        if (metadata != null) {
            for (FieldMetaData entry : metadata) {
                String fieldName = entry.fieldName;
                ColumnType columnType = getColumnType(entry.valueMetaData);
                if (columnType != null) {
                    columns.add(new ColumnDef(fieldName, getColumnText(fieldName), columnType));
                }
            }
        }

        return new IntentTypeTable(type, text, columns);
    }

    private String getIntentTypeText(IntentType type) {
        return StringUtils.capitalize(type.name().toLowerCase());
    }

    private String getColumnText(String column) {
        column = StringUtils.capitalize(column);
        return StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(column), ' ');
    }

    private ColumnType getColumnType(FieldValueMetaData data) {
        switch (data.type) {
            case TType.BOOL:
                return ColumnType.BOOLEAN;
            case TType.DOUBLE:
                return ColumnType.DOUBLE;
            case TType.I16:
            case TType.I32:
                return ColumnType.INT;
            case TType.I64:
                return ColumnType.LONG;
            case TType.STRING:
                return ColumnType.STRING;
            case TType.STRUCT:
                if (data.isTypedef() && data.getTypedefName().equalsIgnoreCase("datetime")) {
                    return ColumnType.DATETIME;
                }
        }

        return null;
    }

    private Operator getOperator(BinaryOperator op) {
        String text = "";
        String notation = "";

        switch (op) {
            case EQ:
                text = "Equal to";
                notation = "=";
                break;
            case GE:
                text = "Greater than or equal to";
                notation = ">=";
                break;
            case GT:
                text = "Greater than";
                notation = ">";
                break;
            case LE:
                text = "Less than or equal to";
                notation = "<=";
                break;
            case LT:
                text = "Less than";
                notation = "<";
                break;
            case NE:
                text = "Not equal";
                notation = "!=";
                break;
        }

        return new Operator(op, text, notation);
    }

}
