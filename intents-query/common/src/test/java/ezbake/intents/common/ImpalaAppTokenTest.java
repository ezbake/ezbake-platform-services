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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImpalaAppTokenTest {

    // the unit under test
    private ImpalaAppToken uut;

    @Test
    public void test() {
        String uuid = "blahblahblah";
        String userAuths = "TS,S,SI";
        String tableName = "my table name";
        boolean closed = false;
        String scanHandle = "my scan handle";
        int offset = 1234;
        int batchsize = 2345;

        uut = new ImpalaAppToken();
        uut.setBatchsize(batchsize);
        uut.setClosed(closed);
        uut.setOffset(offset);
        uut.setScanHandle(scanHandle);
        uut.setTableName(tableName);

        assertEquals("batchsize mismatch", batchsize, uut.getBatchsize());
        assertEquals("closed mismatch", closed, uut.isClosed());
        assertEquals("offset mismatch", offset, uut.getOffset());
        assertEquals("scanhandle mismatch", scanHandle, uut.getScanHandle());
        assertEquals("tablename mismatch", tableName, uut.getTableName());
    }
}
