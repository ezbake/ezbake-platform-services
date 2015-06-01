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

package ezbake.security.service.admins;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Set;

/**
 * User: jhastings
 * Date: 3/25/14
 * Time: 8:13 AM
 */
public class AdministratorServiceStaticFileTest {
    private static final Logger logger = LoggerFactory.getLogger(AdministratorServiceStaticFileTest.class);
    private static String adminFilePath = "/admins.yaml";
    private static String adminUser = "Test User";
    private AdministratorService adminService;

    @Before
    public void setUp() throws FileNotFoundException {
        URL u = AdministratorServiceStaticFileTest.class.getResource(adminFilePath);
        if (u == null) {
            throw new FileNotFoundException("Resource: " + adminFilePath + " not found");
        }
        adminService = new AdministratorService(u.getFile());
    }

    @Test
    public void testLoadStream() {
        InputStream adminStream= AdministratorServiceStaticFileTest.class.getResourceAsStream(adminFilePath);
        Set<String> s = AdministratorService.loadAdministratorYaml(adminStream);
        Assert.assertFalse(s.isEmpty());
    }
    @Test
    public void testLoadNull() {
        Set<String> s = AdministratorService.loadAdministratorYaml(null);
        Assert.assertNull(s);
    }

    @Test
    public void testIsAdmin() {
        Assert.assertTrue(adminService.isAdmin(adminUser));
    }

    @Test
    public void testAdminsYaml() throws UnsupportedEncodingException {
        String yaml = "- Test User1";
        AdministratorService adminService = AdministratorService.fromYAML(yaml);
        Assert.assertTrue(adminService.isAdmin("Test User1"));
        Assert.assertEquals(1, adminService.numAdmins());
    }

    @Test
    public void testAdminsChange() {
        String yaml = "- Test User1";
        AdministratorService adminService = AdministratorService.fromYAML(yaml);
        Assert.assertTrue(adminService.isAdmin("Test User1"));
        Assert.assertEquals(1, adminService.numAdmins());

        yaml = "- Test User1\n- Test User2";
        adminService.loadUpdate(new ByteArrayInputStream(yaml.getBytes()));
        Assert.assertTrue(adminService.isAdmin("Test User1"));
        Assert.assertTrue(adminService.isAdmin("Test User2"));
        Assert.assertEquals(2, adminService.numAdmins());
    }

}
