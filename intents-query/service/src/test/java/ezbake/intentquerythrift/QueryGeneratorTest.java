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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.intent.query.processor.QueryGeneratorImpl;
import ezbake.intent.query.processor.SecurityStorageEmptyImpl;
import ezbake.query.intents.BinaryOperator;
import ezbake.query.intents.BinaryPredicate;
import ezbake.query.intents.ColumnValue;
import ezbake.query.intents.IntentType;
import ezbake.query.intents.Predicate;
import ezbake.query.intents.Query;
import ezbake.query.intents.QueryAtom;
import ezbake.security.test.MockEzSecurityToken;

/**
 * Unit test for simple App.
 */
public class QueryGeneratorTest {
    QueryGeneratorImpl qgImpl = null;

    @Before
    public void setUp() throws Exception {
        qgImpl = new QueryGeneratorImpl(null, new SecurityStorageEmptyImpl());
    }

    @Test
    public void returnColumnsTest() throws Exception {
        Query q = new Query();
        List<String> reqCols = new ArrayList<>();
        reqCols.add("ABC");
        reqCols.add("DEF");
        reqCols.add("GHI");
        reqCols.add("JKL");
        q.setRequestedColumns(reqCols);

        QueryAtom maintable = new QueryAtom();
        maintable.setIntent(IntentType.PERSON);

        q.setPrimaryQuery(maintable);

        EzSecurityToken token = MockEzSecurityToken.getMockAppToken("test", "test");
        String res = qgImpl.generateSQLString(q, token);

        assertTrue(res.startsWith("SELECT ABC,DEF,GHI,JKL FROM testapp_PERSON WHERE secuuid = '"));
    }

    @Test
    public void binaryPredicatesAndTest() throws Exception {
        Query q = new Query();
        List<String> reqCols = new ArrayList<String>();
        reqCols.add("ABC");
        reqCols.add("DEF");
        reqCols.add("GHI");
        reqCols.add("JKL");
        q.setRequestedColumns(reqCols);

        QueryAtom maintable = new QueryAtom();
        maintable.setIntent(IntentType.PERSON);

        Predicate predicate = new Predicate();
        BinaryPredicate bpredicate = new BinaryPredicate();
        bpredicate.setColumnName("PRED1");
        bpredicate.setBinaryOperator(BinaryOperator.EQ);

        ColumnValue cv = new ColumnValue();
        cv.setStringValue("somestring");
        bpredicate.setValue(cv);
        predicate.setBinaryPredicate(bpredicate);

        Predicate predicate1 = new Predicate();
        BinaryPredicate bpredicate1 = new BinaryPredicate();
        bpredicate1.setColumnName("PRED2");
        bpredicate1.setBinaryOperator(BinaryOperator.GT);

        ColumnValue cv1 = new ColumnValue();
        cv1.setDoubleValue(1000000);
        bpredicate1.setValue(cv1);

        predicate1.setBinaryPredicate(bpredicate1);

        Predicate predicate2 = new Predicate();
        BinaryPredicate bpredicate2 = new BinaryPredicate();
        bpredicate2.setColumnName("PRED3");
        bpredicate2.setBinaryOperator(BinaryOperator.NE);

        ColumnValue cv2 = new ColumnValue();
        cv2.setBoolValue(false);
        bpredicate2.setValue(cv2);

        predicate2.setBinaryPredicate(bpredicate2);

        List<List<Predicate>> predicateList = new ArrayList<>();

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(predicate);
        predicateList.add(predicates);

        List<Predicate> predicates1 = new ArrayList<>();
        predicates1.add(predicate1);
        predicateList.add(predicates1);

        List<Predicate> predicates2 = new ArrayList<>();
        predicates2.add(predicate2);
        predicateList.add(predicates2);

        maintable.setPredicates(predicateList);

        q.setPrimaryQuery(maintable);

        EzSecurityToken token = MockEzSecurityToken.getMockAppToken("test", "test");
        String res = qgImpl.generateSQLString(q, token);

        assertTrue(
                res.startsWith(
                        "SELECT ABC,DEF,GHI,JKL FROM testapp_PERSON WHERE PRED1 = 'somestring' AND PRED2 > 1000000.0 "
                                + "AND PRED3 != false AND secuuid = '"));
    }

    @Test
    public void binaryPredicatesOrTest() throws Exception {
        Query q = new Query();
        List<String> reqCols = new ArrayList<String>();
        reqCols.add("ABC");
        reqCols.add("DEF");
        reqCols.add("GHI");
        reqCols.add("JKL");
        q.setRequestedColumns(reqCols);

        QueryAtom maintable = new QueryAtom();
        maintable.setIntent(IntentType.PERSON);

        Predicate predicate = new Predicate();
        BinaryPredicate bpredicate = new BinaryPredicate();
        bpredicate.setColumnName("PRED1");
        bpredicate.setBinaryOperator(BinaryOperator.EQ);

        ColumnValue cv = new ColumnValue();
        cv.setStringValue("somestring");
        bpredicate.setValue(cv);
        predicate.setBinaryPredicate(bpredicate);

        Predicate predicate1 = new Predicate();
        BinaryPredicate bpredicate1 = new BinaryPredicate();
        bpredicate1.setColumnName("PRED2");
        bpredicate1.setBinaryOperator(BinaryOperator.GT);

        ColumnValue cv1 = new ColumnValue();
        cv1.setDoubleValue(1000000);
        bpredicate1.setValue(cv1);

        predicate1.setBinaryPredicate(bpredicate1);

        Predicate predicate2 = new Predicate();
        BinaryPredicate bpredicate2 = new BinaryPredicate();
        bpredicate2.setColumnName("PRED3");
        bpredicate2.setBinaryOperator(BinaryOperator.NE);

        ColumnValue cv2 = new ColumnValue();
        cv2.setBoolValue(false);
        bpredicate2.setValue(cv2);

        predicate2.setBinaryPredicate(bpredicate2);

        List<List<Predicate>> predicateList = new ArrayList<>();

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(predicate);
        predicates.add(predicate1);
        predicates.add(predicate2);
        predicateList.add(predicates);

        maintable.setPredicates(predicateList);

        q.setPrimaryQuery(maintable);

        EzSecurityToken token = MockEzSecurityToken.getMockAppToken("test", "test");
        String res = qgImpl.generateSQLString(q, token);

        assertTrue(
                res.startsWith(
                        "SELECT ABC,DEF,GHI,JKL FROM testapp_PERSON WHERE (PRED1 = 'somestring' OR PRED2 > 1000000.0 "
                                + "OR PRED3 != false) AND secuuid = '"));
    }

    @Test
    public void binaryPredicatesAndOrTest() throws Exception {
        Query q = new Query();
        List<String> reqCols = new ArrayList<String>();
        reqCols.add("ABC");
        reqCols.add("DEF");
        reqCols.add("GHI");
        reqCols.add("JKL");
        q.setRequestedColumns(reqCols);

        QueryAtom maintable = new QueryAtom();
        maintable.setIntent(IntentType.PERSON);

        Predicate predicate = new Predicate();
        BinaryPredicate bpredicate = new BinaryPredicate();
        bpredicate.setColumnName("PRED1");
        bpredicate.setBinaryOperator(BinaryOperator.EQ);
        ColumnValue cv = new ColumnValue();
        cv.setStringValue("somestring");
        bpredicate.setValue(cv);
        predicate.setBinaryPredicate(bpredicate);

        Predicate predicate1 = new Predicate();
        BinaryPredicate bpredicate1 = new BinaryPredicate();
        bpredicate1.setColumnName("PRED2");
        bpredicate1.setBinaryOperator(BinaryOperator.GT);
        ColumnValue cv1 = new ColumnValue();
        cv1.setDoubleValue(1000000);
        bpredicate1.setValue(cv1);
        predicate1.setBinaryPredicate(bpredicate1);

        Predicate predicate2 = new Predicate();
        BinaryPredicate bpredicate2 = new BinaryPredicate();
        bpredicate2.setColumnName("PRED3");
        bpredicate2.setBinaryOperator(BinaryOperator.NE);
        ColumnValue cv2 = new ColumnValue();
        cv2.setBoolValue(false);
        bpredicate2.setValue(cv2);
        predicate2.setBinaryPredicate(bpredicate2);

        Predicate predicate3 = new Predicate();
        BinaryPredicate bpredicate3 = new BinaryPredicate();
        bpredicate3.setColumnName("PRED4");
        bpredicate3.setBinaryOperator(BinaryOperator.NE);
        ColumnValue cv3 = new ColumnValue();
        cv3.setBoolValue(false);
        bpredicate3.setValue(cv2);
        predicate3.setBinaryPredicate(bpredicate3);

        List<List<Predicate>> predicateList = new ArrayList<>();

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(predicate);
        predicates.add(predicate1);
        predicateList.add(predicates);

        List<Predicate> predicates1 = new ArrayList<>();
        predicates1.add(predicate2);
        predicates1.add(predicate3);
        predicateList.add(predicates1);

        maintable.setPredicates(predicateList);

        q.setPrimaryQuery(maintable);

        EzSecurityToken token = MockEzSecurityToken.getMockAppToken("test", "test");
        String res = qgImpl.generateSQLString(q, token);

        assertTrue(
                res.startsWith(
                        "SELECT ABC,DEF,GHI,JKL FROM testapp_PERSON WHERE (PRED1 = 'somestring' OR PRED2 > 1000000.0) AND (PRED3 != false OR PRED4 != false) AND secuuid = '"));
    }
}
