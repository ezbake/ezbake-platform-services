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

package deployer.publishers;

import deployer.TestUtils;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.EzSecurityRegistrationClient;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.protect.mock.server.EzSecurityRegistrationMockService;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftServerPool;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;
import static ezbake.deployer.utilities.Utilities.s;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * User: jhastings
 * Date: 6/2/14
 * Time: 4:43 PM
 */
public class EzSecurityRegistrationClientTest {
    static ThriftServerPool pool;
    private static Properties configuration;

    @BeforeClass
    public static void setUpServer() throws Exception {
        configuration = new EzConfiguration(new ClasspathConfigurationLoader("/test.properties")).getProperties();
        pool = new ThriftServerPool(configuration, 15000);
        pool.startCommonService(new EzSecurityRegistrationMockService(),
                EzSecurityRegistrationConstants.SERVICE_NAME, "sid");
    }

    @AfterClass
    public static void shutdownServer() {
        if (pool != null) {
            pool.shutdown();
        }
    }

    EzSecurityRegistrationClient client;

    @Before
    public void setUp() {
        EzbakeSecurityClient securityClient = EasyMock.createMock(EzbakeSecurityClient.class);
        client = new EzSecurityRegistrationClient(new EzDeployerConfiguration(configuration),
                securityClient);
    }

    @Test
    public void testCertificates() throws DeploymentException {
        List<ArtifactDataEntry> certs = client.get(TestUtils.getTestEzSecurityToken(), TestUtils.APP_NAME, TestUtils.SECURITY_ID);

        assertEquals(7, certs.size());

        Hashtable<String, ArtifactDataEntry> temp = new Hashtable<String, ArtifactDataEntry>();
        Hashtable<String, Integer> sizes = new Hashtable<String, Integer>();
        for (ArtifactDataEntry c : certs) {
            temp.put(c.getEntry().getName(), c);
            sizes.put(c.getEntry().getName(), c.getData().length);
        }
        assertEntry(temp, sizes, s("%s/%s/application.p12", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/application.pub", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/application.priv", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/application.crt", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/ezbakeca.crt", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/ezbakeca.jks", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));
        assertEntry(temp, sizes, s("%s/%s/ezbakesecurityservice.pub", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID));

    }

    public void assertEntry(Hashtable<String, ArtifactDataEntry> temp, Hashtable<String, Integer> expected, String key) {
        ArtifactDataEntry entry = temp.get(key);
        long size = expected.get(key);
        assertNotNull(s("Entry %s does not exist in tar. Existing entries: %s", key, temp.keySet()), entry);
        assertEquals(size, entry.getEntry().getSize());
    }
}
