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

package ezbake.security.service.registration.handler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCryptoException;
import ezbake.local.zookeeper.LocalZookeeper;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.test.MockEzSecurityToken;
import ezbakehelpers.accumulo.AccumuloHelper;

import org.apache.accumulo.core.client.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * User: jhastings
 * Date: 4/8/14
 * Time: 1:20 PM
 */
public class HandlerBaseTest {

    private static Logger log = LoggerFactory.getLogger(BasicHandlerTest.class);

    public static final String ca = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDFjCCAf6gAwIBAgIBATANBgkqhkiG9w0BAQUFADAqMRUwEwYDVQQLEwxUaGVT\n" +
            "b3VyY2VEZXYxETAPBgNVBAMTCEV6QmFrZUNBMCAXDTE0MTEyNTE4MTk0OVoYDzIw\n" +
            "NDQxMTI1MTgxOTQ5WjAqMRUwEwYDVQQLEwxUaGVTb3VyY2VEZXYxETAPBgNVBAMT\n" +
            "CEV6QmFrZUNBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA19H/DmXI\n" +
            "aYMAXcU2v5Wyj9nlX7U4z8U5NUHNAEdJSez6//rTvck/bopwCvvVlpLTE4QImPHf\n" +
            "Rccye/zuCUEA7ZC7hOyvQAcjLdYkdeo40vkhHJa74DH463+Iz/uw6TzPWdK8okRJ\n" +
            "MW1jc8QlqXnSjFCTuTz+7h7o+QNErGHSyWWqx8nYyStCPhPgPEfFZlN8GJ5t4Z3V\n" +
            "q4jUki6XGMPjDvL6wF60MbGL7d9Q1+65rvBO7JD6NarIrATAtMCuMIqeH38OwqVj\n" +
            "ss1FBzAI1QK6iaFicO4g6kb9sfrImJcpNvLzR2PlVe67x3/gsedi099sSQfv3Tk6\n" +
            "LGvCwENj0LWYgwIDAQABo0UwQzASBgNVHRMBAf8ECDAGAQH/AgEAMA4GA1UdDwEB\n" +
            "/wQEAwIBBjAdBgNVHQ4EFgQUaEb00k4OIFiPw679dCo0AqTWTAIwDQYJKoZIhvcN\n" +
            "AQEFBQADggEBAIwj69Z6LYLdqHOBqK5rxZW0oOzgv05i5WFbN31ArNO/7fVY3Hc9\n" +
            "tqAO2CupJjF5drJSiinLy+6/dWpcfaZ7phlYdtOG7+OlL8uFAnz8wSciQwC0093A\n" +
            "LqPW3cvmwo9ECT+kGf1GOigBjETuloikOrAlZRqEC6c9bpyfuLML56cWbDkRyqvC\n" +
            "TYLb3DBEZVb2mvlGxgBpcSgViDZGqvK3TRyaVSHiGcE1coB76gXse7uR2ibbNSIS\n" +
            "1FIpSi6tqeQqdogiNywkjjf5AxHgGwGcqhNFTRSURoQI+P8HASBFSzfu+hx4Od83\n" +
            "PVuworkkw7nZT9pDUc6S7tjSVLHgxJc3aKk=\n" +
            "-----END CERTIFICATE-----\n";

    protected static Properties ezConfig;
    private static LocalZookeeper localZookeeper;
    private static int zooPort = 2188;

    protected EzSecurityRegistrationHandler handler;

    @BeforeClass
    public static void setUpClass() throws Exception {
        ezConfig = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        ezConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                EzSecurityRegistrationHandlerTest.class.getResource("/conf/ssl/serverssl").getPath());
        ezConfig.setProperty(EzSecurityRegistrationHandler.PKEY_MODE, EzSecurityRegistrationHandler.PkeyDevMode);


        // Set up a zookeeper for the client pool
        localZookeeper = new LocalZookeeper(zooPort);
        ezConfig.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, localZookeeper.getConnectionString());
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        if (localZookeeper != null) {
            localZookeeper.shutdown();
        }
    }

    @Before
    public void setUpTest() throws AccumuloSecurityException, AccumuloException, IOException, TableNotFoundException, AppPersistCryptoException, PKeyCryptoException {
        Connector conn = new AccumuloHelper(ezConfig).getConnector();
        try {
            conn.tableOperations().delete(AccumuloRegistrationManager.REG_TABLE);
            conn.tableOperations().delete(AccumuloRegistrationManager.LOOKUP_TABLE);
        } catch (TableNotFoundException e) {
            // ignore
        }
        initHandler();

        BatchWriter writer = conn.createBatchWriter(AccumuloRegistrationManager.REG_TABLE,1000000L, 1000L, 10);
        AppPersistenceModel m = new AppPersistenceModel();
        m.setId(SecurityID.ReservedSecurityId.CA.getId());
        m.setX509Cert(ca);
        writer.addMutations(m.getObjectMutations());

        AppPersistenceModel n = new AppPersistenceModel();
        m.setId(SecurityID.ReservedSecurityId.EzSecurity.getId());
        m.setPublicKey("PublicKey");
        writer.addMutations(m.getObjectMutations());

        writer.close();
    }

    public void initHandler() throws AccumuloSecurityException, AccumuloException, PKeyCryptoException, IOException {
        handler = new EzSecurityRegistrationHandler(ezConfig);
    }

    public EzSecurityToken getTestEzSecurityToken() {
        return getTestEzSecurityToken(false);
    }

    public EzSecurityToken getTestEzSecurityToken(boolean admin) {
        return getTestEzSecurityToken(admin, "dn");
    }

    public EzSecurityToken getTestEzSecurityToken(boolean admin, String dn) {
        EzSecurityToken token = MockEzSecurityToken.getBlankToken("SecurityClientTest", null, System.currentTimeMillis()+1000);
        MockEzSecurityToken.populateExternalProjectGroups(token, Maps.<String, List<String>>newHashMap(), admin);
        MockEzSecurityToken.populateAuthorizations(token, "high", Sets.newHashSet("U"));
        MockEzSecurityToken.populateUserInfo(token, dn, "USA", null);
        return token;
    }
}
