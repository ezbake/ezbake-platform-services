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

package ezbake.security.persistence.model;

import ezbake.persist.EzPersist;
import ezbake.persist.FilePersist;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 10:44 AM
 */
public class TestCAPersistenceModel {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    EzPersist ezPersist;
    @Before
    public void setUpFileSystem() throws IOException {
        String root = folder.getRoot().toString();

        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "ca", "certificate", "", "data")), "my certificate");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "ca", "private_key", "", "data")), "my private key");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "ca", "serial", "", "data")), "100");
        ezPersist = new FilePersist(root);
    }

    @After
    public void cleanFileSystem() throws IOException {
        FileUtils.cleanDirectory(folder.getRoot());
    }

    @Test
    public void testCaGet() {
        List<Map<String, String>> cas = ezPersist.all();
        for (Map<String, String> ca : cas) {
            CAPersistenceModel capm = CAPersistenceModel.fromEzPersist(ca);
            Assert.assertEquals("ca", capm.getId());
            Assert.assertEquals("my certificate", capm.getCertificate());
            Assert.assertEquals("my private key", capm.getPrivateKey());
            Assert.assertEquals(100L, capm.getSerial());
            break;
        }
    }
}
