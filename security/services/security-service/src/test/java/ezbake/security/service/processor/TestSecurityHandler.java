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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.name.Names;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenJson;
import ezbake.base.thrift.TokenRequest;
import ezbake.base.thrift.TokenType;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.utils.EzSSL;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.common.core.EzSecurityConstant;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.persistence.AppPersistenceModule;
import ezbake.security.service.EzSecurityBaseTest;
import ezbake.security.service.ServiceTokenProvider;
import ezbake.security.service.modules.AdminServiceModule;
import ezbake.security.service.modules.TokenJSONModule;
import ezbake.security.service.registration.ClientLookup;
import ezbake.security.service.registration.EzbakeRegistrationService;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.service.sync.NoopRedisCache;
import ezbake.thrift.ThriftClientPool;

public class TestSecurityHandler extends EzSecurityBaseTest {
    private static PKeyCrypto crypto;
    private static long expiry = 10 * 60 * 1000; //millis
    Properties serverConfiguration;
    PKeyCrypto serverCrypto;
    private EzSecurityHandler handler;

    @BeforeClass
    public static void setUpProperties() throws IOException {
        crypto = EzSSL.getCrypto(setClientEzConfig(new Properties()));
    }

    @Before
    public void setUpTest() throws EzConfigurationLoaderException, IOException {
        final EzConfiguration ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader());
        serverConfiguration = setServerEzConfig(ezConfiguration.getProperties());
        serverCrypto = EzSSL.getCrypto(serverConfiguration);

        handler = Guice.createInjector(
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Properties.class).toInstance(serverConfiguration);
                        install(new AppPersistenceModule(serverConfiguration));
                        install(new AdminServiceModule(serverConfiguration));
                        bind(UserAttributeService.class).to(FileUAService.class);
                        bind(EzSecurityRedisCache.class).to(NoopRedisCache.class);
                        bind(EzSecurityTokenProvider.class).to(ServiceTokenProvider.class);
                        bind(EzbakeRegistrationService.class).to(ClientLookup.class);
                        try {
                            bind(ThriftClientPool.class).toInstance(getMockClientPool());
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                        try {
                            bind(PKeyCrypto.class).annotatedWith(Names.named("server crypto"))
                                    .toInstance(EzSSL.getCrypto(serverConfiguration));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        install(new TokenJSONModule());
                    }
                }).getInstance(EzSecurityHandler.class);
    }

    @Test
    public void tokenFactoryAppInfoHasExpiration() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = new TokenRequest("SecurityClientTest", System.currentTimeMillis(), TokenType.APP);
        EzSecurityToken token =
                handler.requestToken(request, EzSecurityTokenBaseTest.getRequestSignature(request, crypto));

        // Basic not null assertions
        Assert.assertNotNull(token);

        // Make sure it sets the expiration millis
        Assert.assertTrue(token.getValidity().getNotAfter() != 0);
        Assert.assertTrue(token.getValidity().getNotAfter() > System.currentTimeMillis());
    }

    @Test
    public void testBasicAppInfoToken() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = new TokenRequest("SecurityClientTest", System.currentTimeMillis(), TokenType.APP);
        EzSecurityToken token =
                handler.requestToken(request, EzSecurityTokenBaseTest.getRequestSignature(request, crypto));

        // Basic not null assertions
        Assert.assertNotNull(token);
        Assert.assertEquals(TokenType.APP, token.getType());

        // Actual content assertions
        Assert.assertEquals("SecurityClientTest", token.getTokenPrincipal().getPrincipal());

        Assert.assertEquals("high", token.getAuthorizationLevel());

        Set<String> l1 = ImmutableSortedSet.of("ezbake", "42six", "CSC", "USA", "high");
        Assert.assertEquals(l1, token.getAuthorizations().getFormalAuthorizations());
    }

    @Test
    public void testUserInfoAsJson() throws PKeyCryptoException, TException, IOException {
        TokenRequest request = new TokenRequest("EFE", System.currentTimeMillis(), TokenType.USER);
        request.setPrincipal(getSignedDn("Jim Bob", serverCrypto));

        EzSecurityTokenJson out =
                handler.requestUserInfoAsJson(request, EzSecurityTokenUtils.tokenRequestSignature(request, crypto));
    }

    @Test
    public void testForwardableAppInfoToken() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = new TokenRequest("SecurityClientTest", System.currentTimeMillis(), TokenType.APP);
        request.setTargetSecurityId("EzPy");

        EzSecurityToken token =
                handler.requestToken(request, EzSecurityTokenBaseTest.getRequestSignature(request, crypto));

        // Basic not null assertions
        Assert.assertNotNull(token);

        // Actual content assertions
        Assert.assertEquals("SecurityClientTest", token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals("high", token.getAuthorizationLevel());
        Assert.assertEquals(Sets.newHashSet("ezbake", "42six"), token.getAuthorizations().getFormalAuthorizations());

        // Assert forwardable to EzPy
        Assert.assertEquals("SecurityClientTest", token.getValidity().getIssuedTo());
        Assert.assertEquals("EzPy", token.getValidity().getIssuedFor());
    }

    @Test
    public void testCorrectAuthorizations1() throws PKeyCryptoException, TException, IOException {
        TokenRequest request = new TokenRequest("SecurityClientTest", System.currentTimeMillis(), TokenType.APP);
        request.setTargetSecurityId("EzPy");

        EzSecurityToken token =
                handler.requestToken(request, EzSecurityTokenBaseTest.getRequestSignature(request, crypto));

        Assert.assertNotNull(token != null);
    }

    @Test
    public void testCacheInvalidation() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = new TokenRequest("SecurityClientTest", System.currentTimeMillis(), TokenType.APP);
        request.setTargetSecurityId("SecurityClientTest");

        EzSecurityToken token =
                handler.requestToken(request, EzSecurityTokenBaseTest.getRequestSignature(request, crypto));
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        map.put(EzSecurityConstant.EZ_INTERNAL_PROJECT, Lists.newArrayList(EzSecurityConstant.EZ_INTERNAL_ADMIN_GROUP));
        token.setExternalProjectGroups(map);

        handler.invalidateCache(token);
    }
}
