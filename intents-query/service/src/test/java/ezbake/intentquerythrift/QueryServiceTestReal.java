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

package ezbake.intentquerythrift;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import ezbake.query.intents.FieldSort;
import ezbake.query.intents.QueryResult;
import ezbake.query.intents.SortOrder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.intent.query.thrift.IntentsQueryService;
import ezbake.query.intents.BinaryOperator;
import ezbake.query.intents.BinaryPredicate;
import ezbake.query.intents.ColumnValue;
import ezbake.query.intents.IntentType;
import ezbake.query.intents.Person;
import ezbake.query.intents.Predicate;
import ezbake.query.intents.Query;
import ezbake.query.intents.QueryAtom;
import ezbake.thrift.ThriftClientPool;


/**
 * Created by fyan on 11/4/14.
 */
public class QueryServiceTestReal {
    protected static ThriftClientPool pool;

    protected static String SERVICE_NAME = "intentQuery";
    protected static EzSecurityToken securityToken = ezbake.security.test.MockEzSecurityToken.getMockAppToken("client", "Principle");

    @BeforeClass
    public static void init() throws Exception {
        EzConfiguration configuration = new EzConfiguration();
        Properties properties = configuration.getProperties();
        properties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "192.168.50.105:2181");

        pool = new ThriftClientPool(properties);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    public void testOrQuery() throws Exception {
        IntentsQueryService.Client client = pool.getClient(SERVICE_NAME, IntentsQueryService.Client.class);

        try {
            List<String> columns = new ArrayList<>();
            columns.add(Person._Fields.FIRST_NAME.getFieldName());
            columns.add(Person._Fields.LAST_NAME.getFieldName());

            // person
            List<List<Predicate>> predicateList = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            BinaryPredicate predicate1 = new BinaryPredicate(Person._Fields.FIRST_NAME.getFieldName(), BinaryOperator.EQ, ColumnValue.stringValue("firstname_0001"));
            predicates.add(new Predicate(Predicate._Fields.BINARY_PREDICATE, predicate1));
            BinaryPredicate predicate2 = new BinaryPredicate(Person._Fields.FIRST_NAME.getFieldName(), BinaryOperator.EQ, ColumnValue.stringValue("firstname_0002"));
            predicates.add(new Predicate(Predicate._Fields.BINARY_PREDICATE, predicate2));
            predicateList.add(predicates);
            QueryAtom personQuery = new QueryAtom(IntentType.PERSON, predicateList);
            Query query = new Query(columns, personQuery);
            QueryResult result = client.query(query, securityToken);
            assertEquals(2, result.getResultSet().size());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void testAndQuery() throws Exception {
        IntentsQueryService.Client client = pool.getClient(SERVICE_NAME, IntentsQueryService.Client.class);

        try {
            List<String> columns = new ArrayList<>();
            columns.add(Person._Fields.FIRST_NAME.getFieldName());
            columns.add(Person._Fields.LAST_NAME.getFieldName());

            // person
            List<List<Predicate>> predicateList = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            BinaryPredicate predicate1 = new BinaryPredicate(Person._Fields.FIRST_NAME.getFieldName(), BinaryOperator.EQ, ColumnValue.stringValue("firstname_0001"));
            predicates.add(new Predicate(Predicate._Fields.BINARY_PREDICATE, predicate1));
            predicateList.add(predicates);

            List<Predicate> predicates2 = new ArrayList<>();
            BinaryPredicate predicate2 = new BinaryPredicate(Person._Fields.FIRST_NAME.getFieldName(), BinaryOperator.EQ, ColumnValue.stringValue("firstname_0002"));
            predicates2.add(new Predicate(Predicate._Fields.BINARY_PREDICATE, predicate2));
            predicateList.add(predicates2);

            QueryAtom personQuery = new QueryAtom(IntentType.PERSON, predicateList);
            Query query = new Query(columns, personQuery);
            QueryResult result = client.query(query, securityToken);
            assertEquals(0, result.getResultSet().size());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void testPageQuery() throws Exception {
        IntentsQueryService.Client client = pool.getClient(SERVICE_NAME, IntentsQueryService.Client.class);

        try {
            List<String> columns = new ArrayList<>();
            columns.add(Person._Fields.FIRST_NAME.getFieldName());
            columns.add(Person._Fields.LAST_NAME.getFieldName());

            // person
            List<List<Predicate>> predicateList = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            BinaryPredicate predicate = new BinaryPredicate(Person._Fields.COUNTRY.getFieldName(), BinaryOperator.EQ, ColumnValue.stringValue("jamaica"));
            predicates.add(new Predicate(Predicate._Fields.BINARY_PREDICATE, predicate));
            predicateList.add(predicates);
            QueryAtom personQuery = new QueryAtom(IntentType.PERSON, predicateList);
            Query query = new Query(columns, personQuery);

            List<FieldSort> sortFileds = new ArrayList<>();
            sortFileds.add(new FieldSort(Person._Fields.LAST_NAME.getFieldName(), SortOrder.DESCENDING));
            query.setSortCriteria(sortFileds);
            short pageSize = 100;

            for(int i = 0; i < 3; i++) {
                int offset = i * pageSize;
                ezbake.query.intents.Page page = new ezbake.query.intents.Page(offset, pageSize);
                query.setPage(page);

                QueryResult result = client.query(query, securityToken);

                assertEquals(pageSize, result.getResultSet().size());
                assertEquals(pageSize, result.getPageSize());
                assertEquals(offset, result.getOffset());
            }
        } finally {
            pool.returnToPool(client);
        }
    }
}
