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

/**
 * User: jhastings
 * Date: 12/12/13
 * Time: 8:50 AM
 */
public class TestAppInfoToken extends EzSecurityTokenBaseTest {
    private static Logger log = LoggerFactory.getLogger(TestAppInfoToken.class);
	private static String querySecurityId = "EzPy";
    private static long expiry = 10 * 60 * 1000; //millis

    @Test
    public void tokenHasExpires() throws TException, PKeyCryptoException, IOException {
    	TokenRequest request = tokenRequestForApp(securityId);
      
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        // Basic not null assertions
        Assert.assertNotNull(token);

        // Make sure it sets the expiration millis
        Assert.assertTrue(token.getValidity().getNotAfter() != 0);
        Assert.assertTrue(token.getValidity().getNotAfter() > System.currentTimeMillis());
    }

    @Test
    public void tokenHasSourceSecurityId() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Assert.assertEquals(securityId, token.getValidity().getIssuedTo());
    }

    @Test
    public void tokenHasTargetSecurityId() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        request.setTargetSecurityId(targetSecurityId);

        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Assert.assertEquals(securityId, token.getValidity().getIssuedFor());
        Assert.assertEquals(targetSecurityId, token.getValidity().getIssuedFor());
    }

    @Test
    public void tokenHasAppInfo() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        // Basic not null assertions
        Assert.assertNotNull(token.getTokenPrincipal());
        Assert.assertEquals(securityId, token.getTokenPrincipal().getPrincipal());
    }

    @Test
    public void tokenHasAppAuths() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Set<String> expectedAuthorization = ImmutableSortedSet.of("ezbake", "42six", "high", "CSC", "USA");

        Assert.assertEquals(securityId, token.getTokenPrincipal().getPrincipal());
        Assert.assertEquals(expectedAuthorization, token.getAuthorizations().getFormalAuthorizations());
        Assert.assertEquals("high", token.getAuthorizationLevel());
    }

    @Test(expected=EzSecurityTokenException.class)
    public void tokenFailsIfSignatureInvalid() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        String sig = getRequestSignature(request);

        // Modify request after sig gen
        request.setTargetSecurityId(targetSecurityId);

        EzSecurityToken token = handler.requestToken(request, sig);
    }

    @Test(expected=AppNotRegisteredException.class)
    public void tokenFailsIfApplicationNotRegistered() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp("NotARegisteredApp");
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    }

    @Test
    public void tokenHasCommunityAuths() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        Set<String> expectedAuthorization = ImmutableSortedSet.of("comm1", "comm2");
        Assert.assertEquals(expectedAuthorization, token.getAuthorizations().getExternalCommunityAuthorizations());

        request = tokenRequestForApp(securityId);
        request.setTargetSecurityId(securityId2);
        token = handler.requestToken(request, getRequestSignature(request));
        expectedAuthorization = ImmutableSortedSet.of("comm1");
        Assert.assertEquals(expectedAuthorization, token.getAuthorizations().getExternalCommunityAuthorizations());
    }
}
