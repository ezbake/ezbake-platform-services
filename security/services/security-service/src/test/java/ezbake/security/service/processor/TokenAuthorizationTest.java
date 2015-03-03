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

import static org.junit.Assert.*;

import java.io.IOException;

import com.google.common.collect.Sets;
import ezbake.base.thrift.*;

import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.security.thrift.AppNotRegisteredException;
import ezbake.crypto.PKeyCryptoException;

public class TokenAuthorizationTest extends EzSecurityTokenBaseTest {
	private Logger log = LoggerFactory.getLogger(TokenAuthorizationTest.class);
	
	@Test
    public void testAuthorizations1() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
        
        log.debug("Authorizations 1:  {}", token.getAuthorizations());
    	
    	assertTrue(token.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(token.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    	
    }
    
    @Test
    public void testAuthorizations2() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
    	request.setTargetSecurityId(targetSecurityId2);
    	
    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    	
    	log.debug("Authorizations 2: {}", token.getAuthorizations());
    	
    	assertTrue(token.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(token.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    	assertTrue(!token.getAuthorizations().getFormalAuthorizations().contains("CSC"));
    	assertTrue(!token.getAuthorizations().getFormalAuthorizations().contains("UA"));
    }
    
    @Test
    public void testAuthorizations3() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
    	request.setTargetSecurityId(targetSecurityId2);
    	
    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    	
    	log.debug("Formal Authorizations {}", token.getAuthorizations().getFormalAuthorizations());

        request = new TokenRequest(targetSecurityId2, System.currentTimeMillis(), TokenType.USER);
        request.setTokenPrincipal(token);
    	request.setTargetSecurityId(targetSecurityId3);

        assertEquals(Sets.newHashSet("42six", "ezbake"), token.getAuthorizations().getFormalAuthorizations());

        EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));

    	log.debug("Formal Authorizations {}", token2.getAuthorizations().getFormalAuthorizations());
    	
    	assertTrue(token2.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(!token2.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    }
    
    @Test
    public void testAuthorizations4() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
    	TokenRequest request = tokenRequestForApp(securityId);
    	request.setTargetSecurityId(securityId2);

    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    	
    	log.debug("Authorizations 1:  {}", token.getAuthorizations());
    	
    	assertTrue(token.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(!token.getAuthorizations().getFormalAuthorizations().contains("CSC"));
    	assertTrue(!token.getAuthorizations().getFormalAuthorizations().contains("USA"));
    	
    }

    @Ignore("This is using the same crypto to request app token as a different application")
    @Test
    public void testAuthorizations5() throws EzSecurityTokenException, AppNotRegisteredException, TException, PKeyCryptoException, IOException {
    	TokenRequest request = tokenRequestForApp(securityId);
    	request.setTargetSecurityId(securityId2);

    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));

        request = new TokenRequest(securityId2, System.currentTimeMillis(), TokenType.APP);
    	request.setTokenPrincipal(token);

    	request.setTargetSecurityId(targetSecurityId3);

    	EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
    	
    	log.debug("Authorizations 2: {}", token2.getAuthorizations());
    	
    	assertTrue(token2.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(!token2.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    	
    }
    
    @Test
    public void testAuthorizations6() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForApp(securityId);
        request.setTargetSecurityId(securityId2);

        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    	
        log.debug("The Chain: {}", token.getTokenPrincipal().getRequestChain());

        request.setPrincipal(token.getTokenPrincipal());
        request.setSecurityId(securityId2);
        request.setTargetSecurityId(targetSecurityId3);
    	
        EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
    	
        assertTrue(token2.getAuthorizations().getFormalAuthorizations().contains("42six"));
        assertTrue(!token2.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    }

    @Test
    public void testAuthorizations7() throws TException, PKeyCryptoException, IOException {
        TokenRequest request = tokenRequestForUser(securityId, dn);
        request.setTargetSecurityId(securityId2);

        EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
    	
    	log.debug("The Chain: {}", token.getTokenPrincipal().getRequestChain());

        request.setPrincipal(token.getTokenPrincipal());
    	request.setSecurityId(securityId2);
    	request.setTargetSecurityId(targetSecurityId3);
    	
    	EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
    	
    	assertTrue(token2.getAuthorizations().getFormalAuthorizations().contains("42six"));
    	assertTrue(!token2.getAuthorizations().getFormalAuthorizations().contains("ezbake"));
    }
    
}
