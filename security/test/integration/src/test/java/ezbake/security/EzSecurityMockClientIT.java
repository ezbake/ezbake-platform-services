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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSortedSet;

import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.*;
import ezbake.crypto.PKeyCryptoException;
import ezbake.security.client.EzSecurityTokenWrapper;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * User: jhastings
 * Date: 12/13/13
 * Time: 3:13 PM
 */
public class EzSecurityMockClientIT extends EzSecurityMockITBase {
    private static Logger log = LoggerFactory.getLogger(EzSecurityMockClientIT.class);

    @BeforeClass
    public static void setSecurityId() {
        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, EzSecurityMockClientIT.class.getResource("/pki/client").getFile());
        properties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "10000000");
    }

    @Before
    public void setUpTest() {
    }

    @Test
    public void clientPing() throws TException {
        ezbakeSecurityClient.ping();
    }

    @Test
    public void appInfo() throws AppNotRegisteredException, TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken();
        Assert.assertEquals(App, token.getTokenPrincipal().getPrincipal());
        Set<String> auths = ImmutableSortedSet.of("high", "servant", "carry", "help", "protect", "nan", "Stark", "ezbake",
                "42six", "CSC", "USA");
        Assert.assertEquals(auths, token.getAuthorizations().getFormalAuthorizations());
    }

    @Test
    public void fetchUsertokenNoArgs() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser();
        Assert.assertNotNull(token);
        Assert.assertEquals(App, token.getValidity().getIssuedTo());
        Assert.assertEquals(App, token.getValidity().getIssuedFor());
        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
    }

    @Test
    public void fetchUsertokenTargetSecurityId() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser("10000000");
        Assert.assertNotNull(token);
        Assert.assertEquals(App, token.getValidity().getIssuedTo());
        Assert.assertEquals("10000000", token.getValidity().getIssuedFor());
        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
    }

    @Test
    public void userInfoDn() throws TException, EzSecurityTokenException, IOException, PKeyCryptoException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        Assert.assertEquals(App, token.getValidity().getIssuedTo());
        Assert.assertEquals(App, token.getValidity().getIssuedFor());
        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
    }

    @Test
    public void fetchUserInfo() throws TException, EzSecurityTokenException, IOException, PKeyCryptoException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);


        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
        Assert.assertEquals("Hodor", token.getTokenPrincipal().getName());

        Set<String> l1 = ImmutableSortedSet.of("Stark", "carry", "help", "protect", "nan", "servant");

        Assert.assertEquals(l1, token.getAuthorizations().getFormalAuthorizations());
        Assert.assertEquals("nan", token.getOrganization());
        Assert.assertEquals("Stark", token.getCitizenship());
        Assert.assertEquals("servant", token.getAuthorizationLevel());

        // assert community attributes
        CommunityMembership ca = token.getExternalCommunities().get("starkies");
        Assert.assertEquals("familiar", ca.getType());
        Assert.assertEquals("stark", ca.getOrganization());
        Assert.assertArrayEquals(new String[]{"hodor", "honor"}, ca.getTopics().toArray());
        Assert.assertArrayEquals(new String[]{"north"}, ca.getRegions().toArray());
        Assert.assertArrayEquals(new String[]{"helpers"}, ca.getGroups().toArray());
    }


    @Test
    public void testUserInfoNoArgs() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser();
        Assert.assertNotNull(token);
        Assert.assertEquals(App, token.getValidity().getIssuedTo());
        Assert.assertEquals(App, token.getValidity().getIssuedFor());
        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
    }

    @Test
    public void testUserInfoTargetId() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser("10000000");
        Assert.assertNotNull(token);
        Assert.assertEquals(App, token.getValidity().getIssuedTo());
        Assert.assertEquals("10000000", token.getValidity().getIssuedFor());
        Assert.assertTrue(token.getTokenPrincipal().getPrincipal().contains("Hodor"));
    }

    @Test
    public void getsUserInfo() throws InterruptedException, EzSecurityTokenException, TException, PKeyCryptoException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        Assert.assertNotNull(info);
        Assert.assertTrue(info.getTokenPrincipal().getPrincipal().contains("Hodor"));
        Assert.assertEquals("Hodor", info.getTokenPrincipal().getName());

        Set<String> l1 = ImmutableSortedSet.of("Stark", "carry", "help", "protect", "nan", "servant");

        Assert.assertEquals(l1,info.getAuthorizations().getFormalAuthorizations());
        Assert.assertEquals("nan", info.getOrganization());
        Assert.assertEquals("Stark", info.getCitizenship());
        Assert.assertEquals("servant", info.getAuthorizationLevel());

        // assert community attributes
        CommunityMembership ca = info.getExternalCommunities().get("starkies");
        Assert.assertEquals("familiar", ca.getType());
        Assert.assertEquals("stark", ca.getOrganization());
        Assert.assertArrayEquals(new String[]{"hodor", "honor"}, ca.getTopics().toArray());
        Assert.assertArrayEquals(new String[]{"north"}, ca.getRegions().toArray());
        Assert.assertArrayEquals(new String[]{"helpers"}, ca.getGroups().toArray());
    }

    @Test
    public void appCommsToken() throws EzSecurityTokenException, TException, PKeyCryptoException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);
        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, "10000002");

        Assert.assertEquals("10000002", info.getValidity().getIssuedFor());
    }

    @Test
    public void mockRequestUser() throws EzSecurityTokenException, TException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser();
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), DN);
    }

    @Test
    public void mockRequestUserToken() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser("10000001");
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), DN);
        Assert.assertEquals("10000001", token.getValidity().getIssuedFor());
    }

    @Test
    public void mockRequestUserToken2() throws TException, EzSecurityTokenException {
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser();
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), DN);
        Assert.assertEquals("10000000", token.getValidity().getIssuedFor());
    }


    @Test
    public void appInfoCached() throws TException, EzSecurityTokenException, AppNotRegisteredException {
        EzSecurityToken token = ezbakeSecurityClient.fetchAppToken("10000000");

        Set<String> l1 = ImmutableSortedSet.of("high", "servant", "carry", "help", "protect", "nan", "Stark", "ezbake",
                "42six", "CSC", "USA");

        Assert.assertEquals(App, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals(l1, token.getAuthorizations().getFormalAuthorizations());

        EzSecurityToken token2 = ezbakeSecurityClient.fetchAppToken("10000000");
        Assert.assertEquals(App, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals(l1, token.getAuthorizations().getFormalAuthorizations());

        token.validate();
        token2.validate();
        Assert.assertEquals(token, token2);
    }

    @Test
    public void legacyUserCached() throws PKeyCryptoException, TException, EzSecurityTokenException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken token1 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken token2 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        token1.validate();
        token2.validate();
        Assert.assertEquals(token1, token2);
    }

    @Test
    public void legacyUserInfoCached() throws PKeyCryptoException, TException, EzSecurityTokenException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken info1 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken info2 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        info1.validate();
        info2.validate();
        Assert.assertEquals(info1, info2);
    }

    @Test
    public void fetchUserCached() throws PKeyCryptoException, TException, EzSecurityTokenException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
        EzSecurityToken token2 = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        token.validate();
        token2.validate();
        Assert.assertEquals(token, token2);
    }
    
    @Test
    public void fetchTokenTestUser() throws EzSecurityTokenException, TException, IOException, PKeyCryptoException {
    	ProxyPrincipal dn = getSignedPrincipal(DN);
    	
    	EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
    	EzSecurityToken token2 = ezbakeSecurityClient.fetchDerivedTokenForApp(token, "10000000");
    	EzSecurityToken token3 = ezbakeSecurityClient.fetchDerivedTokenForApp(token2, "10000001");
    	
    	Assert.assertTrue(token2 != null);
    	Assert.assertTrue(token3 != null);
    	
    	log.debug("The Chain: {}", token3.getTokenPrincipal().getRequestChain());
    	
    	Assert.assertTrue(token3.getTokenPrincipal().getRequestChain().size() == 2);
    	Assert.assertTrue(token3.getTokenPrincipal().getRequestChain().get(0).equals("10000000"));
    	Assert.assertTrue(token3.getTokenPrincipal().getRequestChain().get(1).equals("10000001"));
    }
    
    @Test
    public void fetchTokenTestApp() throws TException {
    	EzSecurityToken token = ezbakeSecurityClient.fetchAppToken(App);
    	
    	log.debug("Token Type: {}", token.getType());
    	
    	EzSecurityToken token2 = ezbakeSecurityClient.fetchDerivedTokenForApp(token, "10000001");
 
    	Assert.assertTrue(token != null);
    	Assert.assertTrue(token2 != null);
    	
    	log.debug("The Chain: {}", token2.getTokenPrincipal().getRequestChain());
    	
    	Assert.assertTrue(token2.getTokenPrincipal().getRequestChain().get(0).equals("10000000"));
    	Assert.assertTrue(token2.getTokenPrincipal().getRequestChain().get(1).equals("10000001"));
    }
    
    @Test
    public void fetchTokenCacheTest() throws EzSecurityTokenException, TException, IOException, PKeyCryptoException {
    	ProxyPrincipal dn = getSignedPrincipal(DN);
    	
    	EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
    	EzSecurityToken token2 = ezbakeSecurityClient.fetchDerivedTokenForApp(token, "10000000");
    	EzSecurityToken token3 = ezbakeSecurityClient.fetchDerivedTokenForApp(token, "10000000");
    	
    	Assert.assertTrue(token2.equals(token3));
    }
    
    @Test
    public void testPartialMode() throws TException {
        log.debug("Test Partial Mode");
        Properties copy = new Properties();
        copy.putAll(properties);
        
        copy.setProperty("ezbake.security.client.mode", "Partial");
        EzbakeSecurityClient client = new EzbakeSecurityClient(copy);
        
        log.debug("Do Call");
        EzSecurityToken token = client.fetchAppToken("10000001");
        
        assertTrue(token != null);
        assertTrue(token.getType().equals(TokenType.APP));
        assertTrue(token.getTokenPrincipal().getRequestChain().contains("10000001"));
        
        log.debug("Finish Test Partial Mode");
    }
    
    @Test
    public void testMockFetchTokenForProxiedUser() throws EzSecurityTokenException, TException {
        EzSecurityTokenWrapper w = ezbakeSecurityClient.fetchTokenForProxiedUser("10000000");

        log.info("Username {} {}", w.getUsername(), w.getUserId());
        
        assertTrue(w != null);
        assertTrue(w.getTargetSecurityId().equals("10000000"));
        assertTrue(w.getUsername().equals("Hodor"));
        assertTrue(w.getUserId().equals("Hodor"));

    }
    
    @Test
    public void testMockFetchTokenForApp() throws TException {
        EzSecurityTokenWrapper w = ezbakeSecurityClient.fetchAppToken("10000000");
        
        assertTrue(w != null);
        assertTrue(w.getApplicationSecurityId().equals("10000000"));
    }
    
    @Test
    public void testMockFetchDerivedToken() throws EzSecurityTokenException, TException {
        EzSecurityTokenWrapper w1 = ezbakeSecurityClient.fetchTokenForProxiedUser("10000000");
        EzSecurityTokenWrapper w2 = ezbakeSecurityClient.fetchDerivedTokenForApp(w1, "10000001");

        log.debug("Token: {}", w1);
        log.debug("Token: {}", w2);
        assertTrue(w2 != null);
        assertTrue(w2.getTargetSecurityId().equals("10000001"));
    }

}
