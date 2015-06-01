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

package ezbake.security.service.processor;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.name.Names;
import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.persistence.AppPersistenceModule;
import ezbake.security.service.EzSecurityBaseTest;
import ezbake.security.service.ServiceTokenProvider;
import ezbake.security.service.modules.AdminServiceModule;
import ezbake.security.service.modules.TokenJSONModule;
import ezbake.security.service.registration.ClientLookup;
import ezbake.security.service.registration.EzbakeRegistrationService;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.service.sync.NoopRedisCache;
import ezbake.security.ua.UAModule;
import ezbake.crypto.utils.EzSSL;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.BeforeClass;
import ezbake.thrift.ThriftClientPool;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 12/12/13
 * Time: 9:23 AM
 */
public class EzSecurityTokenBaseTest extends EzSecurityBaseTest {

    protected static final String dn = "Jim Bob";
    protected static final String dn2 = "EzbakeClient";
    protected static final String dn3 = "EzbakeAdmin";
    protected static final String dn4 = "John Doe";
    
    protected static String securityId = "SecurityClientTest";
    protected static String securityId2 = "EzPy";
    protected static String securityId3 = "NewSecurityClientTest";
    protected static String securityId4 = "NewEzPy";
    
    protected static String targetSecurityId = "SecurityClientTest";
    protected static String targetSecurityId2 = "EzPy";
    protected static String targetSecurityId3 = "ExSearch";
    
    // Configuration for a second application client
    protected static String clientSecurityId = "client";
    protected static PKeyCrypto clientCrypto;

    protected static PKeyCrypto serverCrypto;
    protected EzSecurityHandler handler;

    protected static String getRequestSignature(TokenRequest request) throws PKeyCryptoException, IOException {
        return getRequestSignature(request, clientCrypto);
    }
    protected static String getRequestSignature(TokenRequest request, PKeyCrypto crypto) throws PKeyCryptoException, IOException {
        return EzSecurityTokenUtils.tokenRequestSignature(request, crypto);
    }
    

    /**
     * @throws IOException
     */




    public TokenRequest tokenRequestForUser(String securityID, String user) throws PKeyCryptoException, IOException, TException {
        TokenRequest req = new TokenRequest(securityID, System.currentTimeMillis(), TokenType.USER);
        req.setProxyPrincipal(getSignedProxyPrincipal(user, serverCrypto));
        return req;
    }

    public TokenRequest tokenRequestForApp(String securityID) throws PKeyCryptoException, IOException, TException {
        TokenRequest req = new TokenRequest(securityID, System.currentTimeMillis(), TokenType.APP);
        req.setProxyPrincipal(getSignedProxyPrincipal(securityID, serverCrypto));
        return req;
    }

    static Properties serverConfig;
    @BeforeClass
    public static void setUpProperties() throws IOException, EzConfigurationLoaderException {
        Properties ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

        serverConfig = setServerEzConfig(ezConfiguration);
        serverCrypto = EzSSL.getCrypto(serverConfig);

        Properties clientEzConfig = setClientEzConfig(ezConfiguration);
        clientCrypto = EzSSL.getCrypto(clientEzConfig);
    }

    @Before
    public void setUpTest() throws TException, PKeyCryptoException, EzConfigurationLoaderException {
        handler = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                try {
                    bind(ThriftClientPool.class).toInstance(getMockClientPool());
                    bind(Properties.class).toInstance(serverConfig);
                } catch (TException e) {
                    e.printStackTrace();
                }
                bind(EzSecurityRedisCache.class).to(NoopRedisCache.class);
                try {
                    bind(PKeyCrypto.class).annotatedWith(Names.named("server crypto")).toInstance(EzSSL.getCrypto(serverConfig));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                bind(EzSecurityTokenProvider.class).to(ServiceTokenProvider.class);
                bind(EzbakeRegistrationService.class).to(ClientLookup.class);
                install(new AdminServiceModule(serverConfig));
                install(new TokenJSONModule(serverConfig));
                install(new UAModule(serverConfig));
                install(new AppPersistenceModule(serverConfig));
            }
        }).getInstance(EzSecurityHandler.class);
    }



}
