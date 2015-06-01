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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Set;

/**
 * User: jhastings
 * Date: 7/25/14
 * Time: 9:05 AM
 */
public class AdminstratorServiceDynamicFileTest {
    private static final Logger logger = LoggerFactory.getLogger(AdminstratorServiceDynamicFileTest.class);

    private File watchFile;
    private AdministratorService adminService;

    @Before
    public void setUp() throws IOException, InterruptedException {
        watchFile = new File("tmpFile");
        watchFile.deleteOnExit();

        Writer w = new FileWriter(watchFile);
        w.write("- Test User1");
        w.close();

        adminService = new AdministratorService(watchFile.getAbsolutePath());
        Thread.sleep(1000);
    }

    @Test
    public void testAdminAutoReload() throws IOException, InterruptedException {
        Assert.assertTrue(adminService.isAdmin("Test User1"));
        Assert.assertEquals(1, adminService.numAdmins());

        writeNewUserToFile(watchFile, "- Test User1\n- Test User2");

        // Wait for the update to run
        Thread.sleep(12000);

        Assert.assertTrue(adminService.isAdmin("Test User1"));
        Assert.assertTrue(adminService.isAdmin("Test User2"));
        Assert.assertEquals(2, adminService.numAdmins());
    }

    @Test
    public void changeWatcherNotification() throws IOException, InterruptedException {
        final Set<String> expectedNewUsers = Sets.newHashSet("Test User1", "Test User2");

        Thread watcher = new Thread(new Runnable() {
            @Override
            public void run() {
                Set<String> admins = adminService.getAdmins();
                Assert.assertEquals(expectedNewUsers, admins);
                logger.info("Loaded change in administrators {}", admins);
            }
        });
        watcher.start();

        // Write to the file - expect update to run in ~ 1 second
        writeNewUserToFile(watchFile, "- " + Joiner.on("\n- ").join(expectedNewUsers));

        // Wait for the watcher thread to finish
        watcher.join();
    }

    private static void writeNewUserToFile(File f, String userString) throws IOException {
        logger.debug("Writing update to file");
        Writer w = new FileWriter(f);
        w.write(userString);
        w.close();
    }
}
