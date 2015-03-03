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

package ezbake.protect.ezca;

import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.persistence.model.CAPersistenceModel;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 11:13 AM
 */
public class TestFactory {
    @Test
    public void testGetClass() {
        Map<String, String> carows = new HashMap<String, String>();
        carows.put("test:certificate:None", "cetificate");
        carows.put("test:private_key:None", "pk");
        carows.put("test:serial:None", "100");

        Map<String, String> certrows = new HashMap<String, String>();
        certrows.put("test:admins:None", "admins");
        certrows.put("test:level:None", "med");
        certrows.put("test:owner:None", "own");
        certrows.put("test:name:None", "app name");

        Assert.assertEquals(CAPersistenceModel.class, Factory.entryType(carows));
        Assert.assertEquals(AppPersistenceModel.class, Factory.entryType(certrows));
    }
}
