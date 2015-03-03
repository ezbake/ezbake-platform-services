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
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.local.zookeeper.LocalZookeeper;
import ezbake.security.thrift.ezsecurityConstants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.easymock.EasyMock;
import org.junit.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 * User: jhastings
 * Date: 7/24/14
 * Time: 10:37 PM
 */
public class PublishingAdminServiceTest {

    public static int ZOOKEEPER_PORT = randomPort(4555, 6923);

    public static int randomPort(int start, int end) {
        Random portChooser = new Random();
        return portChooser.nextInt((end - start) + 1) + start;
    }

    static LocalZookeeper lz;

    @BeforeClass
    public static void start() throws Exception {
        lz = new LocalZookeeper(ZOOKEEPER_PORT);
    }

    File watchFile;
    CuratorFramework zk;
    Properties p;

    @Before
    public void setUpTest() throws Exception {
        zk = CuratorFrameworkFactory.builder()
                .connectString(lz.getConnectionString())
                .retryPolicy(new RetryNTimes(5, 1000))
                .build();
        zk.start();
        try {
            zk.delete().deletingChildrenIfNeeded().forPath("/ezsecurity");
        } catch (Exception e) {
            // just ignore this...
        }

        watchFile = new File("tmpFile");
        watchFile.deleteOnExit();

        Writer w = new FileWriter(watchFile);
        w.write("- Test User1");
        w.close();

        p = new Properties();
        p.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, lz.getConnectionString());
    }

    @After
    public void stopTest() {
        if (zk != null) {
            zk.close();
        }
    }

    @AfterClass
    public static void stop() throws IOException {
        if (lz != null) {
            lz.shutdown();
        }
    }


    @Test @Ignore("This doesn't actually test anything")
    public void testInstance() throws Exception {
        final Set<String> expectedNewUsers = Sets.newHashSet("Test User1", "Test User2");

        ServiceDiscoveryClient sdc = EasyMock.createMock(ServiceDiscoveryClient.class);
        EasyMock.expect(sdc.getEndpoints(ezsecurityConstants.SERVICE_NAME))
                .andReturn(Collections.<String>emptyList())
                .anyTimes();
        EasyMock.replay(sdc);

        AdminSyncer as = new AdminSyncer(p, sdc);

        PublishingAdminService service = new PublishingAdminService(watchFile.getAbsolutePath(), zk, as);
        Thread.sleep(100);

        writeNewUserToFile(watchFile, "- "+ Joiner.on(", -").join(expectedNewUsers));

        Thread.sleep(12000);

        service.close();
    }

    private static void writeNewUserToFile(File f, String userString) throws IOException {
        Writer w = new FileWriter(f);
        w.write(userString);
        w.close();
    }
}
