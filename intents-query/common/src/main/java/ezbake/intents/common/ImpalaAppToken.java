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

package ezbake.intents.common;

import com.cloudera.impala.extdatasource.thrift.TBinaryPredicate;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import ezbake.base.thrift.EzSecurityToken;

import java.util.List;

public class ImpalaAppToken {

    private EzSecurityToken userToken;

    private String tableName;

    private boolean closed;

    private String scanHandle;

    private int offset;

    private int batchsize;

    private List<List<TBinaryPredicate>> predicates;

    private TTableSchema tableSchema;

    private boolean secUuidInResultSet;

    public EzSecurityToken getUserToken() {
        return userToken;
    }

    public void setUserToken(EzSecurityToken userToken) {
        this.userToken = userToken;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public String getScanHandle() {
        return scanHandle;
    }

    public void setScanHandle(String scanHandle) {
        this.scanHandle = scanHandle;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getBatchsize() {
        return batchsize;
    }

    public void setBatchsize(int batchsize) {
        this.batchsize = batchsize;
    }

    public List<List<TBinaryPredicate>> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<List<TBinaryPredicate>> predicates) {
        this.predicates = predicates;
    }

    public String getIntent() {
        // Tablename is defined to be APPNAME_INTENT
        // e.g. testapp_person ==> PERSON intent
        int underscoreIndex = tableName.lastIndexOf("_") + 1;

        return tableName.substring(underscoreIndex);
    }

    public TTableSchema getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(TTableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public boolean isSecUuidInResultSet() {
        return secUuidInResultSet;
    }

    public void setSecUuidInResultSet(boolean b) {
        this.secUuidInResultSet = b;
    }
}
