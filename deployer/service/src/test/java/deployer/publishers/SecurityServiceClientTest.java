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
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.SecurityServiceClient;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.services.deploy.thrift.DeploymentException;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.URL;
import java.util.Hashtable;
import java.util.List;

import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;
import static ezbake.deployer.utilities.Utilities.s;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SecurityServiceClientTest {

    public static final String EXAMPLE_SS_URL = "http://foobar.com";

    public static class MockSecurityServiceClient extends SecurityServiceClient {
        public MockSecurityServiceClient(EzDeployerConfiguration config) {
            super(config);
        }

        @Override
        protected HttpsURLConnection openUrlConnection(URL endpoint) throws IOException {
            assertEquals(new URL(s("%s/registrations/%s/download", EXAMPLE_SS_URL, TestUtils.SECURITY_ID)), endpoint);
            HttpsURLConnection mockedConn = EasyMock.createMock(HttpsURLConnection.class);
            mockedConn.connect();
            expectLastCall().atLeastOnce();
            expect(mockedConn.getInputStream()).andReturn(
                    SecurityServiceClientTest.class.getClassLoader().getResourceAsStream("ssl/testCerts.tar"));
            replay(mockedConn);

            return mockedConn;
        }
    }

    @Test
    public void testClient() throws DeploymentException {


        EzDeployerConfiguration mockedConfig = EasyMock.createMock(EzDeployerConfiguration.class);

        expect(mockedConfig.getSecurityServiceKeyStoreFormat()).andReturn("PKCS12").anyTimes();
        expect(mockedConfig.getSecurityServiceKeyStorePass()).andReturn("password").anyTimes();
        expect(mockedConfig.getSecurityServiceKeyStorePath()).andReturn("./src/test/resources/ssl/keystore.p12").anyTimes();
        expect(mockedConfig.getSecurityServiceTrustStoreFormat()).andReturn("JKS").anyTimes();
        expect(mockedConfig.getSecurityServiceTrustStorePass()).andReturn("password").anyTimes();
        expect(mockedConfig.getSecurityServiceTrustStorePath()).andReturn("./src/test/resources/ssl/truststore.jks").anyTimes();

        expect(mockedConfig.getSecurityServiceBasePath()).andReturn(EXAMPLE_SS_URL).anyTimes();
        replay(mockedConfig);

        SSLCertsService service = new MockSecurityServiceClient(mockedConfig);

        List<ArtifactDataEntry> list = service.get(TestUtils.APP_NAME, TestUtils.SECURITY_ID);
        Hashtable<String, ArtifactDataEntry> temp = new Hashtable<String, ArtifactDataEntry>();
        for (ArtifactDataEntry c : list) {
            temp.put(c.getEntry().getName(), c);
        }
        assertEntry(temp, s("%s/%s/appSSLCert.p12", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID), 3540);
        assertEntry(temp, s("%s/%s/ca.jks", SSL_CONFIG_DIRECTORY, TestUtils.SECURITY_ID), 1114);
        assertEquals(2, list.size());
    }

    public void assertEntry(Hashtable<String, ArtifactDataEntry> temp, String key, int expected) {
        ArtifactDataEntry entry = temp.get(key);
        assertNotNull(s("Entry %s does not exist in tar. Existing entries: %s", key, temp.keySet()), entry);
        assertEquals(expected, entry.getData().length);
        assertEquals(expected, entry.getEntry().getSize());
    }
}
