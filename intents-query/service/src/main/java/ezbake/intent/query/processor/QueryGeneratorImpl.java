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

package ezbake.intent.query.processor;

import java.util.ArrayList;
import java.util.List;

import ezbake.query.intents.FieldSort;
import ezbake.query.intents.MissingOrder;
import ezbake.query.intents.SortOrder;
import org.apache.thrift.TException;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.intent.query.utils.Conversions;
import ezbake.query.intents.BinaryPredicate;
import ezbake.query.intents.IntentType;
import ezbake.query.intents.Predicate;
import ezbake.query.intents.Query;

public class QueryGeneratorImpl implements QueryGenerator {

    private static final String SECUUID = "secuuid";

    private final InsService insService;
    private final SecurityStorage securityStorage;

    public QueryGeneratorImpl(final EzConfiguration configuration, SecurityStorage securityStorage) {
        if (configuration != null) {
            insService = new InsService(configuration);
        } else {
            insService = null;
        }

        this.securityStorage = securityStorage;
    }

    @Override
    public String generateSQLString(Query query, EzSecurityToken token) throws TException {
        String sqlString = generateMainSqlString(query, token);

        if (query.isSetPage()) {
            return generatePagingSQLString(query, sqlString);
        }
        return sqlString;
    }

    private String generateMainSqlString(Query query, EzSecurityToken token) throws TException {
        IntentType primaryTable = query.getPrimaryQuery().getIntent();

        List<String> appNames = new ArrayList<>();
        if (insService != null) {
            appNames = insService.getAppNames(primaryTable.name());

        } else {
            appNames.add("testapp");
        }

        StringBuilder sb = new StringBuilder();
        int appSize = appNames.size();

        for (int i = 0; i < appSize; i++) {
            String tableName = appNames.get(i) + "_" + primaryTable.name();
            sb.append(generateSqlStringForTable(tableName, query, token));

            if (i < appSize - 1) {
                sb.append(" UNION ALL ");
            }
        }

        return sb.toString();
    }

    private String generatePagingSQLString(Query query, String sqlString) {
        StringBuilder sb = new StringBuilder();

        if (sqlString.contains("UNION ALL")) {
            // use a sub query
            sb.append("SELECT ");
            appendSelectColumns(sb, query);
            sb.append(" FROM (");
            sb.append(sqlString);
            sb.append(") t");
        } else {
            sb.append(sqlString);
        }

        sb.append(" ORDER BY ");

        if (query.isSetSortCriteria()) {
            List<FieldSort> sortCriterias = query.getSortCriteria();
            int sortCount = sortCriterias.size();
            for (int i = 0; i < sortCount; i++) {
                FieldSort sort = sortCriterias.get(i);
                sb.append(sort.getField());
                if (sort.isSetOrder() && sort.getOrder() == SortOrder.DESCENDING) {
                    sb.append(" DESC");
                } else {
                    sb.append(" ASC");
                }

                if (sort.isSetMissing() && sort.getMissing() == MissingOrder.FIRST) {
                    sb.append(" NULLS FIRST");
                } else {
                    sb.append(" NULLS LAST");
                }

                if (i < sortCount - 1) {
                    sb.append(", ");
                }
            }

        } else {
            // default to order by 1 column
            sb.append("1 ASC");
        }

        if (query.isSetPage()) {
            sb.append(" LIMIT ");
            sb.append(query.getPage().getPageSize());
            sb.append(" OFFSET ");
            sb.append(query.getPage().getOffset());
        }
        return sb.toString();
    }

    private void appendSelectColumns(StringBuilder sb, Query query) {
        int columnCount = query.getRequestedColumnsSize();
        for (int i = 0; i < columnCount; i++) {
            sb.append(query.getRequestedColumns().get(i));
            if (i < columnCount - 1) {
                sb.append(",");
            }
        }
    }

    private StringBuilder generateSqlStringForTable(String tableName, Query query, EzSecurityToken token) throws TException {
        StringBuilder sb = new StringBuilder("SELECT ");
        appendSelectColumns(sb, query);
        sb.append(" FROM ");
        sb.append(tableName);

        if (query.getPrimaryQuery().isSetPredicates()) {
            sb.append(" WHERE ");

            int predicateCount = query.getPrimaryQuery().getPredicates().size();

            // ( col1 OR col2) AND (col3 OR col4)
            for (int i = 0; i < predicateCount; i++) {
                List<Predicate> orPredicates = query.getPrimaryQuery().getPredicates().get(i);

                constructOrClause(sb, orPredicates);

                if (i < predicateCount - 1) {
                    sb.append(" AND ");
                }
            }

            // append secuuid
            if (predicateCount > 0) {
                sb.append(" AND ");
            }
            sb.append(SECUUID + " = '");
            sb.append(securityStorage.storeToken(token));
            sb.append("'");
        } else {
            sb.append(" WHERE ");
            sb.append(SECUUID + " = '");
            sb.append(securityStorage.storeToken(token));
            sb.append("'");
        }

        return sb;
    }

    private void constructOrClause(StringBuilder sb, List<Predicate> predicates) {
        int predicateCount = predicates.size();

        if (predicateCount > 1) {
            sb.append("(");
        }

        for (int i = 0; i < predicateCount; i++) {
            Predicate predicate = predicates.get(i);
            // Binary
            if (predicate.isSet(1)) {
                BinaryPredicate binaryPredicate = predicate.getBinaryPredicate();
                sb.append(binaryPredicate.getColumnName() + " ");
                sb.append(Conversions.convertOperatorToString(binaryPredicate.getBinaryOperator()) + " ");
                sb.append(Conversions.convertColValueToObject(binaryPredicate.getValue()));

                if (i < predicateCount - 1) {
                    sb.append(" OR ");
                }
            }
        }

        if (predicateCount > 1) {
            sb.append(")");
        }
    }
}
