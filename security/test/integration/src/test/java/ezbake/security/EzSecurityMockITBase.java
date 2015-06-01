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

package ezbake.security;

import com.google.common.io.Closeables;
import ezbake.base.thrift.EzSecurityPrincipal;
import ezbake.base.thrift.ValidityCaveats;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.service.processor.EzSecurityHandler;
import ezbake.crypto.PKeyCryptoException;
import ezbake.security.thrift.ezsecurityConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import ezbake.thrift.ThriftServerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * User: jhastings
 * Date: 12/13/13
 * Time: 2:46 PM
 */
public class EzSecurityMockITBase extends EzSecurityITBase {
    private static Logger log = LoggerFactory.getLogger(EzSecurityMockITBase.class);

    protected static Properties properties;

    // App Registration Data
    static final String DN = "Hodor";
    static final String App = "10000000";

    protected EzbakeSecurityClient ezbakeSecurityClient;

    @BeforeClass
    public static void setUpServerPool() throws Exception {
        Random portChooser = new Random();
        int port = portChooser.nextInt((34999 - 30000) + 1) + 30000;
        int zooPort = portChooser.nextInt((20499 - 20000) + 1) + 20000;

        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        properties.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, String.valueOf(true));
        properties.setProperty(EzBakeSecurityClientConfigurationHelper.MOCK_USER_KEY, DN);
        properties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:"+String.valueOf(zooPort));

        Properties serverConfig = new Properties();
        serverConfig.putAll(properties);
        serverConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, EzSecurityMockITBase.class.getResource("/pki/server").getFile());
        serverConfig.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "serverssl");
        serverConfig.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_SERVICE_MOCK_SERVER, String.valueOf(true));
        serverConfig.setProperty(FileUAService.USERS_FILENAME, EzSecurityMockITBase.class.getResource("/users.json").getFile());

        serverPool = new ThriftServerPool(serverConfig, port);
        serverPool.startCommonService(new EzSecurityHandler(), ezsecurityConstants.SERVICE_NAME, "1234534");
    }

    @Before
    public void setUpClient() {
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @After
    public void closeClient() throws IOException {
        Closeables.close(ezbakeSecurityClient, true);
    }

}
