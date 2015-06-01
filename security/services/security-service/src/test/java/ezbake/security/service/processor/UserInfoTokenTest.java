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

import com.google.common.collect.ImmutableSortedSet;
import ezbake.base.thrift.*;
import ezbake.security.thrift.*;
import ezbake.crypto.PKeyCryptoException;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * User: jhastings
 * Date: 12/12/13
 * Time: 9:23 AM
 */
public class UserInfoTokenTest extends EzSecurityTokenBaseTest {
    private static Logger log = LoggerFactory.getLogger(UserInfoTokenTest.class);



    @Test
    public void userAuthsDontBreakWhenAppsRequest() throws PKeyCryptoException, TException, IOException {
        EzSecurityToken token;
        TokenRequest request;

        // SecurityClientTest expects these auths
        Set<String> securityClientTestExpectedAuths = ImmutableSortedSet.of("42six", "CSC", "USA", "ezbake", "high");

        // client expects these auths
        Set<String> clientExpectedAuths = ImmutableSortedSet.of("ezbake");

        // First request with SecurityClientTest
        request = tokenRequestForUser(securityId, dn);
        token = handler.requestToken(request, getRequestSignature(request));

        // Validate SecurityClientTest auths
        Assert.assertArrayEquals(securityClientTestExpectedAuths.toArray(), token.getAuthorizations().getFormalAuthorizations().toArray());

        // Now request with client
        request = tokenRequestForUser(clientSecurityId, dn);
        token = handler.requestToken(request, getRequestSignature(request, clientCrypto));

        // Validate client auths
        Assert.assertArrayEquals(clientExpectedAuths.toArray(), token.getAuthorizations().getFormalAuthorizations().toArray());

        // Now request with SecurityClientTest Again
        request = tokenRequestForUser(securityId, dn);
        token = handler.requestToken(request, getRequestSignature(request));

        // Validate SecurityClientTest auths
        Assert.assertEquals(securityClientTestExpectedAuths, token.getAuthorizations().getFormalAuthorizations());
    }

    @Test
    public void requestTokenUser() throws PKeyCryptoException, TException, IOException  {
        TokenRequest request = tokenRequestForUser(securityId, dn);//new TokenRequest(securityId, System.currentTimeMillis(), uToken, TokenType.USER);

        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Set<String> expectedAuths = ImmutableSortedSet.of("ezbake", "42six", "high", "CSC", "USA");

        Assert.assertNotNull(token);
        Assert.assertEquals(dn, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals("Jim Bob", token.getTokenPrincipal().getName());
        Assert.assertEquals("BobJim.1234567890", token.getTokenPrincipal().getExternalID());

        Assert.assertEquals(expectedAuths, token.getAuthorizations().getFormalAuthorizations());
        Assert.assertEquals("high", token.getAuthorizationLevel());
        Assert.assertEquals("USA", token.getCitizenship());
        Assert.assertEquals("CSC", token.getOrganization());

        // community attributes info
        CommunityMembership ca = token.getExternalCommunities().get("name");
        Assert.assertEquals("company",ca.getType());
        Assert.assertEquals("CSC", ca.getOrganization());
        Assert.assertArrayEquals(new String[]{"TopicA"}, ca.getTopics().toArray());
        Assert.assertArrayEquals(new String[]{"Region1"}, ca.getRegions().toArray());
        Assert.assertNotNull(ca.getGroups());
        Assert.assertEquals(0,ca.getGroups().size());
    }

    @Test
    public void tokenHasExpires() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        // Basic not null assertions
        Assert.assertNotNull(token);

        // Make sure it sets the expiration millis
        Assert.assertTrue(token.getValidity().getNotAfter() != 0);
        Assert.assertTrue(token.getValidity().getNotAfter() > System.currentTimeMillis());
    }

    @Test
    public void tokenHasSourceSecurityId() throws TException, PKeyCryptoException, EzSecurityTokenException, IOException {
        TokenRequest request = tokenRequestForUser(securityId, dn);

        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
        Assert.assertEquals(securityId, token.getValidity().getIssuedTo());
    }

    @Test
    public void tokenHasTargetSecurityId() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
        request.setTargetSecurityId(targetSecurityId);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Assert.assertEquals(targetSecurityId, token.getValidity().getIssuedFor());
    }

    @Test
    public void tokenHasUserInfo() throws PKeyCryptoException, TException, IOException {
        TokenRequest request = null;
        EzSecurityToken token = null;

        //dn..1
        //
        request = tokenRequestForUser(securityId, dn);
        token = handler.requestToken(request, getRequestSignature(request));

        // Basic user info
        Assert.assertEquals(dn, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals("Jim Bob", token.getTokenPrincipal().getName());

        // community attributes info
        CommunityMembership ca = token.getExternalCommunities().get("name");
        assertEquals("company", ca.getType());
        assertEquals("CSC", ca.getOrganization());
        assertArrayEquals(new String[]{"TopicA"}, ca.getTopics().toArray());
        assertArrayEquals(new String[]{"Region1"}, ca.getRegions().toArray());
        List<String> groups = ca.getGroups();
        assertNotNull(groups);
        assertEquals(0, groups.size());

        //dn..2
        //
        request = tokenRequestForUser(securityId, dn2);

        request.setType(TokenType.USER);
        token = handler.requestToken(request, getRequestSignature(request));

        // Basic user info
        Assert.assertEquals(dn2, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals("Ezbake Client", token.getTokenPrincipal().getName());

        // community attributes info
        ca = token.getExternalCommunities().get("name");
        assertEquals("product", ca.getType());
        assertEquals("ezbake", ca.getOrganization());
        assertArrayEquals(new String[]{"TopicA"}, ca.getTopics().toArray());
        assertArrayEquals(new String[]{"Region1"}, ca.getRegions().toArray());
        groups = ca.getGroups();
        assertNotNull(groups);
        assertEquals(0, groups.size());

        //dn..3
        //
        request = tokenRequestForUser(securityId, dn3);
        token = handler.requestToken(request, getRequestSignature(request));

        // Basic user info
        Assert.assertEquals(dn3, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals("Ezbake Admin", token.getTokenPrincipal().getName());

        // community attributes info
        ca = token.getExternalCommunities().get("name");
        assertEquals("group", ca.getType());
        assertEquals("team", ca.getOrganization());
        assertArrayEquals(new String[]{"TopicA"}, ca.getTopics().toArray());
        assertArrayEquals(new String[]{"Region1"}, ca.getRegions().toArray());
        groups = ca.getGroups();
        assertNotNull(groups);
        assertEquals(0, groups.size());
    }

    @Test
    public void cachedTokenRequest() throws TException, PKeyCryptoException, EzSecurityTokenException, IOException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Assert.assertNotNull(token.getTokenPrincipal());

        Set<String> auths = token.getAuthorizations().getFormalAuthorizations();
        for (int i = 0; i < 20; ++i) {
            request = tokenRequestForUser(securityId, dn);
            token = handler.requestToken(request, getRequestSignature(request));

            Assert.assertNotNull(token);
            Assert.assertNotNull(token.getTokenPrincipal());

            Assert.assertArrayEquals(auths.toArray(), token.getAuthorizations().getFormalAuthorizations().toArray());
        }
    }

    @Test
    public void emptyAuthTest() throws PKeyCryptoException, IOException, TException {
        TokenRequest request = tokenRequestForUser("empty auth", "empty auth");
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Assert.assertTrue(!token.getAuthorizations().getFormalAuthorizations().contains(""));
    }

    @Test
    public void noAuthsTest() throws PKeyCryptoException, IOException, TException {
        TokenRequest request = tokenRequestForUser("no auths", "Jim Bob");
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        log.debug("TOken: {}", token);
    }
}

