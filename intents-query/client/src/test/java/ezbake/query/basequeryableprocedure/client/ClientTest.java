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
import com.cloudera.impala.extdatasource.thrift.TComparisonOp;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TStatusCode;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.intents.common.ImpalaAppToken;
import ezbake.intents.common.RedisUtils;
import ezbake.query.basequeryableprocedure.ColumnData;
import ezbake.query.basequeryableprocedure.GetPageResult;
import ezbake.query.basequeryableprocedure.PrepareStats;
import ezbake.query.basequeryableprocedure.RowBatch;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ClientTest extends EasyMockSupport {
    // the unit under test
    private Client client = new Client();

    // Our Mock Objects
    private RedisUtils redisUtilsMock;
    private EzbakeAppHandler appHandlerMock;

    // Common Parameter values
    private String secUuid = "12345abcd";
    private String tableName = "tablename";
    private String initString = "initString";
    private String scanHandle = "scanHandle";


    @Before
    public void init() {
        client = new Client();

        redisUtilsMock = createMock(RedisUtils.class);
        appHandlerMock = createMock(EzbakeAppHandler.class);

        client.setRedisUtils(redisUtilsMock);
    }

    @After
    public void cleanup() {
        client = null;
        redisUtilsMock = null;
        appHandlerMock = null;
    }

    @Test
    public void testPrepareWithMissingSecUuid() {
        TPrepareParams params = new TPrepareParams();
        List<List<TBinaryPredicate>> predicates = new ArrayList<List<TBinaryPredicate>>();
        List<TBinaryPredicate> innerPredicateList = new ArrayList<TBinaryPredicate>();
        predicates.add(innerPredicateList);

        params.setPredicates(predicates);

//    	expect(redisUtilsMock.retrieveSecurityToken(null)).andReturn(null);
//    	replay(redisUtilsMock);

        TPrepareResult prepareResult = client.prepare(params);

        assertEquals(prepareResult.getStatus().getStatus_code(), TStatusCode.INTERNAL_ERROR);
        assertNotNull(prepareResult.getStatus().getError_msgs().iterator().next());
//    	verify(redisUtilsMock);
    }

    private List<List<TBinaryPredicate>> generatePredicateList() {
        List<List<TBinaryPredicate>> predicates = new ArrayList<List<TBinaryPredicate>>();
        List<TBinaryPredicate> innerPredicateList = new ArrayList<TBinaryPredicate>();
        TBinaryPredicate predicate = new TBinaryPredicate();
        TColumnDesc col = new TColumnDesc();
        col.setName("secUuid");
        predicate.setCol(col);
        predicate.setOp(TComparisonOp.EQ);
        TColumnValue colvalue = new TColumnValue();
        colvalue.setString_val(secUuid);
        predicate.setValue(colvalue);
        innerPredicateList.add(predicate);
        predicates.add(innerPredicateList);

        return predicates;
    }

    @Test
    public void testPrepareWithValidSecUuid() throws Exception {

        EzSecurityToken securityToken = new EzSecurityToken();

        TPrepareParams params = new TPrepareParams();
        List<List<TBinaryPredicate>> predicates = generatePredicateList();
        params.setTable_name(tableName);
        params.setInit_string(initString);
        params.setPredicates(predicates);

        PrepareStats ps = new PrepareStats();
        List<Integer> accepted = new ArrayList<Integer>();
        accepted.add(0);
        ps.setNumberOfRowsEstimate(10);
        ps.setAcceptedConjuncts(accepted);

        expect(redisUtilsMock.retrieveSecurityToken(secUuid)).andReturn(securityToken);
        replay(redisUtilsMock);
        expect(appHandlerMock.prepare(tableName, initString, predicates, securityToken)).andReturn(ps);
        replay(appHandlerMock);
        client.setAppHandler(appHandlerMock);

        TPrepareResult prepareResult = client.prepare(params);

        assertEquals(10, prepareResult.getNum_rows_estimate());
        assertEquals(accepted, prepareResult.getAccepted_conjuncts());

        verify(redisUtilsMock);
        verify(appHandlerMock);
    }

    @Test
    public void testGetNextWithoutSecUuidInSchema() throws Exception {
        TGetNextParams params = new TGetNextParams();
        params.setScan_handle(scanHandle);

        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);

        TColumnDesc desc1 = new TColumnDesc();
        desc1.setName("fakecolumn");
        desc1.setType(columnType);

        TTableSchema tableSchema = new TTableSchema();
        tableSchema.addToCols(desc1);

        ImpalaAppToken appToken = new ImpalaAppToken();
        appToken.setSecUuidInResultSet(true);
        appToken.setTableSchema(tableSchema);

        GetPageResult pageresult = new GetPageResult();

        RowBatch rows = new RowBatch();
        ColumnData elem = new ColumnData();
        List<Boolean> isNull = new ArrayList<Boolean>();
        isNull.add(true);
        elem.setIs_null(isNull);
        rows.addToCols(elem);
        pageresult.setRows(rows);

        expect(redisUtilsMock.getImpalaAppToken(scanHandle)).andReturn(appToken);
        redisUtilsMock.incrementImpalaAppTokenOffSet(scanHandle, 1);
        replay(redisUtilsMock);

        expect(appHandlerMock.getNext(scanHandle)).andReturn(pageresult);
        replay(appHandlerMock);
        client.setAppHandler(appHandlerMock);

        TGetNextResult getNextResult = client.getNext(params);

        // should only have the one entry; client shouldn't have added another
        assertEquals(1, getNextResult.getRows().getColsSize());

        verify(redisUtilsMock);
        verify(appHandlerMock);
    }

    @Test
    public void testGetNextWithSecUuidInSchema() throws Exception {
        TGetNextParams params = new TGetNextParams();
        params.setScan_handle(scanHandle);

        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);

        TColumnDesc desc1 = new TColumnDesc();
        desc1.setName("secUuid");
        desc1.setType(columnType);

        TColumnDesc desc2 = new TColumnDesc();
        desc2.setName("fakecolumn");
        desc2.setType(columnType);

        TTableSchema tableSchema = new TTableSchema();
        tableSchema.addToCols(desc1);
        tableSchema.addToCols(desc2);

        ImpalaAppToken appToken = new ImpalaAppToken();
        appToken.setSecUuidInResultSet(true);
        appToken.setTableSchema(tableSchema);

        GetPageResult pageresult = new GetPageResult();

        RowBatch rows = new RowBatch();

        // Only create 1 ColumnData to be returned by the app handler.
        // The client should look at the table schema to see if the security UUID
        // is requested and add a entry into our returned result set for the security UUID.
        // Apps shouldn't have to handle returning security UUID results.
        ColumnData elem = new ColumnData();
        List<Boolean> isNull = new ArrayList<Boolean>();
        isNull.add(true);
        elem.setIs_null(isNull);
        rows.addToCols(elem);

        pageresult.setRows(rows);

        expect(redisUtilsMock.getImpalaAppToken(scanHandle)).andReturn(appToken);
        redisUtilsMock.incrementImpalaAppTokenOffSet(scanHandle, 1);
        replay(redisUtilsMock);

        expect(appHandlerMock.getNext(scanHandle)).andReturn(pageresult);
        replay(appHandlerMock);
        client.setAppHandler(appHandlerMock);

        TGetNextResult getNextResult = client.getNext(params);

        // 1 for the 'fakecolumn' + 1 for the Client added secUuid entry
        assertEquals(2, getNextResult.getRows().getColsSize());

        verify(redisUtilsMock);
        verify(appHandlerMock);
    }

    @Test
    public void testSuccessfulClose() {
        TCloseParams params = new TCloseParams();
        params.setScan_handle(scanHandle);

        expect(redisUtilsMock.closeImpalaAppToken(scanHandle)).andReturn(true);
        replay(redisUtilsMock);

        TCloseResult closeResult = client.close(params);

        assertEquals(closeResult.getStatus().getStatus_code(), TStatusCode.OK);
        verify(redisUtilsMock);
    }

    @Test
    public void testBadClose() {
        TCloseParams params = new TCloseParams();
        params.setScan_handle(scanHandle);

        expect(redisUtilsMock.closeImpalaAppToken(scanHandle)).andReturn(false);
        replay(redisUtilsMock);

        TCloseResult closeResult = client.close(params);

        assertEquals(closeResult.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        assertNotNull(closeResult.getStatus().getError_msgs().iterator().next());
        verify(redisUtilsMock);
    }
}
