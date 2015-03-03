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
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TComparisonOp;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TUniqueId;
import ezbake.query.intents.Person;

import java.util.ArrayList;
import java.util.List;


public class Main {
    private static List<List<TBinaryPredicate>> generatePredicates() {
        List<List<TBinaryPredicate>> list = new ArrayList<List<TBinaryPredicate>>();
        List<TBinaryPredicate> inner = new ArrayList<TBinaryPredicate>();
        List<TBinaryPredicate> secId = new ArrayList<TBinaryPredicate>();

        TBinaryPredicate predicate = new TBinaryPredicate();

        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);
        TColumnDesc columnDesc = new TColumnDesc();
        columnDesc.setName(Person._Fields.FIRST_NAME.getFieldName());
        columnDesc.setType(columnType);

        TColumnValue columnValue = new TColumnValue();
        columnValue.setString_val("person1");

        predicate.setCol(columnDesc);
        predicate.setOp(TComparisonOp.EQ);
        predicate.setValue(columnValue);

        inner.add(predicate);

        TBinaryPredicate predicate2 = new TBinaryPredicate();

        TColumnType columnType2 = new TColumnType();
        columnType2.setType(TPrimitiveType.STRING);
        TColumnDesc columnDesc2 = new TColumnDesc();
        columnDesc2.setName(Person._Fields.COUNTRY.getFieldName());
        columnDesc2.setType(columnType2);

        TColumnValue columnValue2 = new TColumnValue();
        columnValue2.setString_val("australia");

        predicate2.setCol(columnDesc2);
        predicate2.setOp(TComparisonOp.NE);
        predicate2.setValue(columnValue2);

        inner.add(predicate2);

        // Add the secUuid predicate
        TBinaryPredicate secPredicate = new TBinaryPredicate();
        TColumnType columnType3 = new TColumnType();
        columnType3.setType(TPrimitiveType.STRING);
        TColumnDesc columnDesc3 = new TColumnDesc();
        columnDesc3.setName("secUuid");
        columnDesc3.setType(columnType3);

        TColumnValue columnValue3 = new TColumnValue();
        columnValue3.setString_val("12345uuid");

        secPredicate.setCol(columnDesc3);
        secPredicate.setOp(TComparisonOp.EQ);
        secPredicate.setValue(columnValue3);

        secId.add(secPredicate);

        list.add(secId);
        list.add(inner);

        return list;
    }

    private static TTableSchema generateTableSchema() {
        TTableSchema tableSchema = new TTableSchema();
        List<TColumnDesc> cols = new ArrayList<TColumnDesc>();

        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);

        TColumnDesc firstNameColumnDesc = new TColumnDesc();
        firstNameColumnDesc.setName(Person._Fields.FIRST_NAME.getFieldName());
        firstNameColumnDesc.setType(columnType);

        TColumnDesc lastNameColumnDesc = new TColumnDesc();
        lastNameColumnDesc.setName(Person._Fields.LAST_NAME.getFieldName());
        lastNameColumnDesc.setType(columnType);

        cols.add(firstNameColumnDesc);
        cols.add(lastNameColumnDesc);

        tableSchema.setCols(cols);

        return tableSchema;
    }

    public static void main(String[] args) {
        Client c = new Client();

        // Data to set up tokens and parameter arguments
        String tablename = "testapp_person";
        String authenticated_user_name = "validUserHere";
        int batchsize = 1000;
        String init_string = "i'm an init-string from impala";

        TUniqueId query_id = new TUniqueId();
        query_id.setHi(123456789L);
        query_id.setLo(987654321L);

        // Prepare
        System.out.println("Calling prepare...");
        TPrepareParams prepareParam = new TPrepareParams();
        prepareParam.setTable_name(tablename);
        prepareParam.setInit_string(init_string);
        prepareParam.setPredicates(Main.generatePredicates()); // same ones get generated

        TPrepareResult prepareResult = c.prepare(prepareParam);
        System.out.println("number of rows estimate is " + prepareResult.getNum_rows_estimate());
        System.out.println("accepted conjuncts: " + prepareResult.getAccepted_conjuncts());

        // Open
        System.out.println("Calling open...");
        TOpenParams openParams = new TOpenParams();
        openParams.setTable_name(tablename);
        openParams.setAuthenticated_user_name(authenticated_user_name);
        openParams.setBatch_size(batchsize);
        openParams.setInit_string(init_string);
        openParams.setQuery_id(query_id);
        openParams.setPredicates(Main.generatePredicates());
        openParams.setRow_schema(Main.generateTableSchema());

        TOpenResult openResult = c.open(openParams);
        System.out.println("scanhandle is " + openResult.getScan_handle());

        // getNext
        System.out.println("Calling getNext...");
        TGetNextParams getNextParams = new TGetNextParams();
        getNextParams.setScan_handle(openResult.getScan_handle());

        TGetNextResult getNextResult = c.getNext(getNextParams);

        System.out.println("TStatus: " + getNextResult.getStatus());
        List<TColumnData> data = getNextResult.getRows().getCols();

        for (TColumnData d : data) {
            System.out.println(d.getIs_null());

            if (d.isSetBinary_vals()) {
                System.out.println(d.getBinary_vals());
            } else if (d.isSetBool_vals()) {
                System.out.println(d.getBool_vals());
            } else if (d.isSetByte_vals()) {
                System.out.println(d.getByte_vals());
            } else if (d.isSetDouble_vals()) {
                System.out.println(d.getDouble_vals());
            } else if (d.isSetInt_vals()) {
                System.out.println(d.getInt_vals());
            } else if (d.isSetLong_vals()) {
                System.out.println(d.getLong_vals());
            } else if (d.isSetShort_vals()) {
                System.out.println(d.getShort_vals());
            } else if (d.isSetString_vals()) {
                System.out.println(d.getString_vals());
            }
        }

        // close the token
        System.out.println("Calling close...");
        TCloseParams params = new TCloseParams();
        params.setScan_handle(openResult.getScan_handle());

        c.close(params);

        System.out.println("Done!");
    }
}
