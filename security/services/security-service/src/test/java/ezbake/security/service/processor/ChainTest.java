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

import java.util.ArrayList;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityPrincipal;

public class ChainTest extends EzSecurityTokenBaseTest {
	private static Logger log = LoggerFactory.getLogger(ChainTest.class);

	@Test
	public void testUpdateWithEmptyChain() {
		EzSecurityPrincipal principal = new EzSecurityPrincipal();
		principal.setRequestChain(new ArrayList<String>());
		handler.updateChain("securityId", "targetSecurityId", principal);
		
		assertTrue(principal.getRequestChain().get(0).equals("securityId"));
		assertTrue(principal.getRequestChain().get(1).equals("targetSecurityId"));
	}
	
	@Test
	public void testUpdateWithNullSecurityId() {
		EzSecurityPrincipal principal = new EzSecurityPrincipal();
		principal.setRequestChain(new ArrayList<String>());
		
		handler.updateChain(null, "target", principal);
		
		assertTrue(principal.getRequestChain().get(0).equals("target"));
	}
	
	@Test
	public void testUpdateWithNullTargetSecurityId() {
		EzSecurityPrincipal principal = new EzSecurityPrincipal();
		principal.setRequestChain(new ArrayList<String>());
		
		handler.updateChain("securityId", null, principal);
		assertTrue(principal.getRequestChain().get(0).equals("securityId"));
		
	}
	
	@Test
	public void testUpdateWithNonEmptyChain() {
		EzSecurityPrincipal principal = new EzSecurityPrincipal();
		principal.setRequestChain(new ArrayList<String>());
		
		handler.updateChain("securityId", "targetSecurityId", principal);
		handler.updateChain("targetSecurityId", "newTargetSecurityId", principal);
		
		assertTrue(principal.getRequestChain().size() == 3);
		assertTrue(principal.getRequestChain().get(0).equals("securityId"));
		assertTrue(principal.getRequestChain().get(1).equals("targetSecurityId"));
		assertTrue(principal.getRequestChain().get(2).equals("newTargetSecurityId"));
	}
}
