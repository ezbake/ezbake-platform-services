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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

import ezbake.base.thrift.*;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityConstant;
import ezbake.security.thrift.*;
import ezbake.crypto.PKeyCryptoException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;


/**
 * User: jhastings
 * Date: 12/13/13
 * Time: 2:43 PM
 */
public class EzSecurityClientIT extends EzSecurityITBase {
    private static Logger log = LoggerFactory.getLogger(EzSecurityClientIT.class);

    @Before
    public void setUpClient() {
        EzbakeSecurityClient.clearCache();
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @After
    public void closeClient() throws IOException {
        Closeables.close(ezbakeSecurityClient, true);
    }

    @Test
    public void clientPing() throws TException {
        ezbakeSecurityClient.ping();
    }

    @Test
    public void appInfo() throws TException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken(App);
        assertEquals(App, token.getTokenPrincipal().getPrincipal());
        Set<String> l1 = ImmutableSortedSet.of("high", "servant", "carry", "help", "protect", "nan", "Stark", "ezbake",
                "42six", "CSC", "USA");
        assertEquals(l1, ImmutableSortedSet.copyOf(token.getAuthorizations().getFormalAuthorizations()));
    }

    @Test
    public void appInfoExclude() throws TException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken(App, Sets.newHashSet("ezbake", "USA"));
        assertEquals(App, token.getTokenPrincipal().getPrincipal());
        Set<String> l1 = ImmutableSortedSet.of("high", "servant", "carry", "help", "protect", "nan", "Stark", "42six",
                "CSC");
        assertEquals(l1, ImmutableSortedSet.copyOf(token.getAuthorizations().getFormalAuthorizations()));
    }

    @Test(expected=EzSecurityTokenException.class)
    public void appInfoForOtherApp() throws TException, EzSecurityTokenException, AppNotRegisteredException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken("99494949494");
    }


    @Test
    public void appInfoDerived() throws TException {
        EzSecurityToken tokne = ezbakeSecurityClient.fetchAppToken("10000001");

        Properties pp = new EzProperties(properties, true);
        pp.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "10000001");
        EzSecurityToken tok = new EzbakeSecurityClient(pp).fetchDerivedTokenForApp(tokne, "10000003");

        assertEquals(tokne.getValidity().getIssuedTime(), tok.getValidity().getIssuedTime());
    }

    @Test
    public void appInfoDerivedExclude() throws TException {
        EzSecurityToken tokne = ezbakeSecurityClient.fetchAppToken("10000001");

        Properties pp = new EzProperties(properties, true);
        pp.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "10000001");
        EzSecurityToken tok = new EzbakeSecurityClient(pp).fetchDerivedTokenForApp(tokne, "10000003", Sets.newHashSet("ezbake"));

        assertEquals(tokne.getValidity().getIssuedTime(), tok.getValidity().getIssuedTime());
        assertTrue(tok.getAuthorizations().getFormalAuthorizations().isEmpty());
    }

    @Test
    public void testUserIsAdmin() throws TException, PKeyCryptoException, IOException {
        String adminDn = "Daenerys";
        ProxyPrincipal dn = getSignedPrincipal(adminDn);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        assertTrue(token.getExternalProjectGroups().containsKey(EzSecurityConstant.EZ_INTERNAL_PROJECT));
        assertTrue(token.getExternalProjectGroups().get(
                EzSecurityConstant.EZ_INTERNAL_PROJECT).contains(EzSecurityConstant.EZ_INTERNAL_ADMIN_GROUP));
        assertTrue(EzbakeSecurityClient.isEzAdmin(token));
    }

    @Test
    public void fetchTokenAppName() throws TException, PKeyCryptoException, IOException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken(AppName);
        Assert.assertEquals(App, token.getValidity().getIssuedFor());

        String adminDn = "Daenerys";
        ProxyPrincipal dn = getSignedPrincipal(adminDn);
        token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, AppName);
        Assert.assertEquals(App, token.getValidity().getIssuedFor());
    }

    @Test
    public void appInfoCached() throws TException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken(App);

        Set<String> l1 = ImmutableSortedSet.of("high", "servant", "carry", "help", "protect", "nan", "Stark", "ezbake",
                "42six", "CSC", "USA");

        assertEquals(App, token.getTokenPrincipal().getPrincipal());
        assertEquals(l1, ImmutableSortedSet.copyOf(token.getAuthorizations().getFormalAuthorizations()));

        EzSecurityToken token2 = ezbakeSecurityClient.fetchAppToken(App);

        assertEquals(App, token.getTokenPrincipal().getPrincipal());
        assertEquals(l1, token.getAuthorizations().getFormalAuthorizations());

        token.validate();
        token2.validate();
        assertEquals(token, token2);
    }

    @Test
    public void userCached() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken token1 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken token2 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        token1.validate();
        token2.validate();
        assertEquals(token1, token2);
    }

    @Test
    public void fetchUser() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, "");

        assertNotNull(token);
        assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
        assertEquals("Hodor", token.getTokenPrincipal().getName());

        Set<String> l1 = ImmutableSortedSet.of("Stark", "carry", "help", "protect", "nan", "servant");
        assertTrue(l1.containsAll(token.getAuthorizations().getFormalAuthorizations()));
        assertTrue(token.getAuthorizations().getFormalAuthorizations().containsAll(l1));
        assertEquals("nan", token.getOrganization());
        assertEquals("Stark", token.getCitizenship());
        assertEquals("servant", token.getAuthorizationLevel());

        // assert community attributes
        CommunityMembership ca = token.getExternalCommunities().get("starkies");
        assertEquals("familiar", ca.getType());
        assertEquals("stark", ca.getOrganization());
        assertArrayEquals(new String[]{"hodor", "honor"}, ca.getTopics().toArray());
        assertArrayEquals(new String[]{"north"}, ca.getRegions().toArray());
        assertArrayEquals(new String[]{"helpers"}, ca.getGroups().toArray());
    }
    @Test
    public void fetchUserExclude() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, "", Sets.newHashSet("protect"));

        assertNotNull(token);
        assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
        assertEquals("Hodor", token.getTokenPrincipal().getName());

        Set<String> l1 = ImmutableSortedSet.of("Stark", "carry", "help", "nan", "servant");
        assertTrue(l1.containsAll(token.getAuthorizations().getFormalAuthorizations()));
        assertTrue(token.getAuthorizations().getFormalAuthorizations().containsAll(l1));
    }

    @Test
    public void getsUserInfo() throws InterruptedException, TException, PKeyCryptoException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        assertNotNull(info);
        assertTrue(info.getTokenPrincipal().getPrincipal().contains("Hodor"));
        assertEquals("Hodor", info.getTokenPrincipal().getName());

        Set<String> l1 = ImmutableSortedSet.of("Stark", "carry", "help", "protect", "nan", "servant");

        System.out.println(info.getAuthorizations().getFormalAuthorizations());
        assertTrue(l1.containsAll(info.getAuthorizations().getFormalAuthorizations()));
        assertTrue(info.getAuthorizations().getFormalAuthorizations().containsAll(l1));
        assertEquals("nan", info.getOrganization());
        assertEquals("Stark", info.getCitizenship());
        assertEquals("servant", info.getAuthorizationLevel());

        // assert community attributes
        CommunityMembership ca = info.getExternalCommunities().get("starkies");
        assertEquals("familiar", ca.getType());
        assertEquals("stark", ca.getOrganization());
        assertArrayEquals(new String[]{"hodor", "honor"}, ca.getTopics().toArray());
        assertArrayEquals(new String[]{"north"}, ca.getRegions().toArray());
        assertArrayEquals(new String[]{"helpers"}, ca.getGroups().toArray());
    }

    @Test
    public void testValidateReceivedToken() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, App);
        byte[] bytes = new TSerializer().serialize(info);

        EzSecurityToken token = new EzSecurityToken();
        new TDeserializer().deserialize(token, bytes);

        ezbakeSecurityClient.validateReceivedToken(token);
    }

    @Test
    public void testValidateReceivedTokenFutureCompatibility() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, App);
        byte[] bytes = new TSerializer().serialize(info);

        EzSecurityToken token = new EzSecurityToken();
        new TDeserializer().deserialize(token, bytes);

        ezbakeSecurityClient.validateReceivedToken(token);
    }

    @Test
    public void testValidateReceivedTokenFuture() throws PKeyCryptoException, TException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, App);
        byte[] bytes = new TSerializer().serialize(info);

        EzSecurityToken token = new EzSecurityToken();
        new TDeserializer().deserialize(token, bytes);

        ezbakeSecurityClient.validateReceivedToken(token);
    }

    @Test
    public void appCommsToken() throws TException, PKeyCryptoException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, "10000002");

        assertNotNull(info);
        assertEquals("10000002", info.getValidity().getIssuedFor());
    }

    @Test
    public void testUserCaching() throws TException, PKeyCryptoException, IOException {
        EzbakeSecurityClient client = new EzbakeSecurityClient(properties);

        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken info = client.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken info2 = client.fetchTokenForProxiedUser(dn, null);

        assertEquals(info, info2);

    }

    @Test
    public void appTokenCache() throws TException {
        EzbakeSecurityClient client = new EzbakeSecurityClient(properties);
        EzbakeSecurityClient client2 = new EzbakeSecurityClient(properties);
        EzSecurityToken token1 = client.fetchAppToken("10000001");
        EzSecurityToken token2 =client.fetchAppToken("10000001");
        EzSecurityToken token3 =client.fetchAppToken("10000001");
        EzSecurityToken token4 =client.fetchAppToken("10000001");
        EzSecurityToken token5 =client.fetchAppToken("10000001");
        assertEquals(token1, token2);

        client2.fetchAppToken("10000001");
        client2.fetchAppToken("10000001");
        client2.fetchAppToken("10000001");
        client2.fetchAppToken("10000001");
    }


    @Test
    public void refreshInvalidToken() throws PKeyCryptoException, IOException, TException, InterruptedException {
        EzbakeSecurityClient client = new EzbakeSecurityClient(properties);

        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken token = client.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken info = new EzSecurityToken(token);

        Thread.sleep(3000);

        client.validateReceivedToken(token);

        assertNotEquals(info.getValidity().getNotAfter(), token.getValidity().getNotAfter());
        assertEquals(info.getType(), token.getType());
        assertEquals(info.getCitizenship(), token.getCitizenship());
        assertEquals(info.getOrganization(), token.getOrganization());
        assertEquals(info.getAuthorizationLevel(), token.getAuthorizationLevel());
        assertEquals(info.getAuthorizations(), token.getAuthorizations());
        assertEquals(info.getExternalCommunities(), token.getExternalCommunities());
        assertEquals(info.getExternalProjectGroups(), token.getExternalProjectGroups());
        assertEquals(info.getValidity().getIssuedTime(), token.getValidity().getIssuedTime());
        assertEquals(info.getValidity().getIssuedTo(), token.getValidity().getIssuedTo());
        assertEquals(info.getValidity().getIssuedFor(), token.getValidity().getIssuedFor());
        assertEquals(info.getValidity().getIssuer(), token.getValidity().getIssuer());
        assertEquals(info.getTokenPrincipal().getPrincipal(), token.getTokenPrincipal().getPrincipal());
        assertEquals(info.getTokenPrincipal().getIssuer(), token.getTokenPrincipal().getIssuer());
        assertEquals(info.getTokenPrincipal().getName(), token.getTokenPrincipal().getName());
        assertEquals(info.getTokenPrincipal().getValidity().getIssuedTo(), token.getTokenPrincipal().getValidity().getIssuedTo());
        assertEquals(info.getTokenPrincipal().getValidity().getIssuedFor(), token.getTokenPrincipal().getValidity().getIssuedFor());
    }

   
}
