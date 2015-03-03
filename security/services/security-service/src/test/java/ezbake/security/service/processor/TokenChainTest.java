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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import ezbake.base.thrift.*;
import ezbake.crypto.PKeyCryptoException;
import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.security.thrift.AppNotRegisteredException;

public class TokenChainTest extends EzSecurityTokenBaseTest {
	private Logger log = LoggerFactory.getLogger(TokenChainTest.class);
	
	 @Test
	 public void chainedTokenRequest1() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
	    	/* Testing For User */
	    	TokenRequest request = tokenRequestForUser(securityId, dn);
	    	request.setTargetSecurityId(targetSecurityId2);
	    	
	    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
	    	
	    	log.debug("Authorization List {}", token.getAuthorizations());
	    	
	    	assertTrue(token.getTokenPrincipal() != null);
	    	
	    	List<String> chain = token.getTokenPrincipal().getRequestChain();
	    	
	    	assertTrue(chain != null);
	    	assertTrue(chain.get(0).equals(securityId));
	    	assertTrue(chain.get(1).equals(targetSecurityId2));
	    }
	    
	    @Test
	    public void chainedTokenRequest2() throws EzSecurityTokenException, AppNotRegisteredException, TException, PKeyCryptoException, IOException {
	    	TokenRequest request = tokenRequestForUser(securityId, dn);
	    	request.setTargetSecurityId(targetSecurityId2);
	    	
	    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
	    	
	    	request = tokenRequestForUser(targetSecurityId2, dn);
            request.setTokenPrincipal(token);
	    	request.setTargetSecurityId(targetSecurityId3);
	    	
	    	EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
	    	
	    	List<String> chainedList = token2.getTokenPrincipal().getRequestChain();
	    	
	    	assertTrue(chainedList.get(0).equals(securityId));
	    	assertTrue(chainedList.get(1).equals(targetSecurityId2));
	    	assertTrue(chainedList.get(2).equals(targetSecurityId3));
	    }
	    
	    @Test
	    public void chainedTokenRequest3() throws PKeyCryptoException, IOException, EzSecurityTokenException, AppNotRegisteredException, TException {
	    	TokenRequest request = tokenRequestForUser(securityId, dn);
	    	request.setTargetSecurityId(targetSecurityId2);
	    	
	    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
	    	
	    	request = tokenRequestForUser(targetSecurityId2, dn);
            request.setTokenPrincipal(token);
	    	request.setTargetSecurityId(securityId);
	    	
	    	EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
	    	
	    	List<String> chainedList = token2.getTokenPrincipal().getRequestChain();

	    	assertTrue(chainedList.get(0).equals(securityId));
	    	assertTrue(chainedList.get(1).equals(targetSecurityId2));
	    	assertTrue(chainedList.get(2).equals(securityId));
	    }
	    
	    @Test
	    public void chainedTokenRequest4() throws EzSecurityTokenException, AppNotRegisteredException, TException, PKeyCryptoException, IOException {
	    	TokenRequest request = tokenRequestForUser(securityId, dn);
	    	request.setTargetSecurityId(targetSecurityId2);
	    	
	    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
	    	
	    	request = tokenRequestForUser(targetSecurityId2, dn);
            request.setTokenPrincipal(token);
	    	request.setTargetSecurityId(targetSecurityId3);
	    	
	    	EzSecurityToken token2 = handler.requestToken(request, getRequestSignature(request));
	    	
	    	request = tokenRequestForUser(targetSecurityId3, dn);
            request.setTokenPrincipal(token2);

	    	EzSecurityToken token3 = handler.requestToken(request, getRequestSignature(request));
	    	List<String> chainedList = token3.getTokenPrincipal().getRequestChain();
	    	
	    	assertTrue(chainedList.get(0).equals(securityId));
	    	assertTrue(chainedList.get(1).equals(targetSecurityId2));
	    	assertTrue(chainedList.get(2).equals(targetSecurityId3));
	    }
	    
	    @Test
	    public void chainedTokenRequest5() throws EzSecurityTokenException, AppNotRegisteredException, TException, PKeyCryptoException, IOException {
            TokenRequest request = tokenRequestForApp(securityId);
	    	request.setTargetSecurityId(targetSecurityId2);
	    	
	    	EzSecurityToken token = handler.requestToken(request, getRequestSignature(request));
	    	
	    	assertTrue(token.getTokenPrincipal() != null);
	    	List<String> chain = token.getTokenPrincipal().getRequestChain();
	    	assertTrue(chain != null);
	    	assertTrue(chain.get(0).equals(securityId));
	    	assertTrue(chain.get(1).equals(targetSecurityId2));
	    }
}
