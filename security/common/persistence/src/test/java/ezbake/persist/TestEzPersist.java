/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.persist;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 8:11 AM
 */
public class TestEzPersist {
    @Test
    public void testEscapePart() {
        String row = "test:Test";
        String escapedRow = "test_:_Test";
        Assert.assertEquals(escapedRow, EzPersist.escapeKeyPart(row));
        Assert.assertEquals(row, EzPersist.unescapeKeyPart(escapedRow));
    }
    @Test
    public void testKey() {
        String row = "TEST";
        String colf = "test_test";
        String colq = "test";
        String expected = row+ EzPersist.KEY_SEP+colf+ EzPersist.KEY_SEP+colq;

        String key = EzPersist.key(row, colf, colq);
        Assert.assertEquals(expected, key);
    }
}
