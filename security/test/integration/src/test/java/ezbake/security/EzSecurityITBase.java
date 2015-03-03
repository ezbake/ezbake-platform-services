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

import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.groups.service.EzGroupsService;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.local.redis.LocalRedis;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.service.modules.AdminServiceModule;
import ezbake.security.service.processor.EzSecurityHandler;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.security.thrift.ezsecurityConstants;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import ezbake.thrift.ThriftServerPool;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;
import java.util.Random;

/**
 * User: jhastings
 * Date: 12/12/13
 * Time: 4:29 PM
 */
public class EzSecurityITBase {
    private static Logger log = LoggerFactory.getLogger(EzSecurityITBase.class);

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    // App Registration Data
    protected static final String DN = "Hodor";
    protected static final String App = "10000000";
    protected static final String AppName = "SecurityClientTest";

    protected static Properties properties;
    protected static ThriftServerPool serverPool;

    // Servers' keys
    protected static String publicKeyPath = EzSecurityITBase.class.getResource("/pki/server/ezbakesecurityservice.pub").getFile();
    protected static String privateKeyPath = EzSecurityITBase.class.getResource("/pki/server/application.priv").getFile();
    protected static PKeyCrypto serverCrypt;

    protected static LocalRedis redisServer;

    protected EzbakeSecurityClient ezbakeSecurityClient;

    @BeforeClass
    public static void setUpServerPool() throws Exception {
        Random portChooser = new Random();
        int port = portChooser.nextInt((34999 - 30000) + 1) + 30000;
        int zooPort = portChooser.nextInt((20499 - 20000) + 1) + 20000;

        redisServer = new LocalRedis();

        EzConfiguration ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader());
        properties = ezConfiguration.getProperties();
        properties.setProperty(EzBakePropertyConstants.REDIS_HOST, "localhost");
        properties.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(redisServer.getPort()));
        properties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:"+String.valueOf(zooPort));
        properties.setProperty(FileUAService.USERS_FILENAME, EzSecurityITBase.class.getResource("/users.json").getFile());
        properties.setProperty(EzBakePropertyConstants.EZBAKE_ADMINS_FILE, EzSecurityITBase.class.getResource("/admins").getFile());
        properties.setProperty(AdminServiceModule.PUBLISHING, Boolean.TRUE.toString());

        Properties localConfig = new Properties();
        localConfig.putAll(properties);
        localConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, EzSecurityITBase.class.getResource("/pki/server").getFile());
        localConfig.setProperty("storage.directory", folder.getRoot().toString());

        serverPool = new ThriftServerPool(localConfig, port);
        serverPool.startCommonService(new EzSecurityHandler(), ezsecurityConstants.SERVICE_NAME, "12345");
        serverPool.startCommonService(new EzGroupsService(), EzGroupsConstants.SERVICE_NAME, "12345");

        ServiceDiscoveryClient client = new ServiceDiscoveryClient(properties.getProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING));
        client.setSecurityIdForCommonService(AppName, "10000000");
        client.setSecurityIdForApplication(AppName, "10000000");

        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, EzSecurityITBase.class.getResource("/pki/client").getFile());
        properties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "10000000");
    }

    @AfterClass
    public static void stopServer() throws IOException {
        if (serverPool != null) {
            serverPool.shutdown();
        }
        if (redisServer != null) {
            redisServer.close();
        }
    }

    @BeforeClass
    public static void setUpCrpto() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        // Set up crypto for signing requests
        serverCrypt = new RSAKeyCrypto(FileUtils.readFileToString(new File(privateKeyPath)), FileUtils.readFileToString(new File(publicKeyPath)));
    }

    public ProxyPrincipal getSignedPrincipal(String principal) throws IOException, PKeyCryptoException, TException {
        ProxyUserToken userToken = new ProxyUserToken(
                        new X509Info(principal),
                        "EzSecurity",
                        "EFE",
                        System.currentTimeMillis()+1000);
        ProxyPrincipal token  = new ProxyPrincipal(EzSecurityTokenUtils.serializeProxyUserTokenToJSON(userToken), "");
        token.setSignature(EzSecurityTokenUtils.proxyUserTokenSignature(userToken, serverCrypt));
        return token;
    }

    public EzSecurityPrincipal getSignedEzSecurityPrincipal(String principal) throws IOException, PKeyCryptoException {
        EzSecurityPrincipal ret = new EzSecurityPrincipal(
                principal, new ValidityCaveats("EzSecurity", "",System.currentTimeMillis()+1000, ""));
        ret.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(ret, serverCrypt));
        return ret;
    }

    public EzSecurityPrincipal getUnsignedEzSecurityPrincipal(String principal) throws IOException, PKeyCryptoException {
        return new EzSecurityPrincipal(
                principal, new ValidityCaveats("EzSecurity", "",System.currentTimeMillis()+1000, ""));
    }
}
