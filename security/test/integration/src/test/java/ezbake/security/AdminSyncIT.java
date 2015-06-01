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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.ProxyPrincipal;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityConstant;
import ezbake.security.service.processor.EzSecurityHandler;
import ezbake.security.thrift.ezsecurityConstants;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * User: jhastings
 * Date: 8/15/14
 * Time: 4:28 PM
 */
public class AdminSyncIT extends EzSecurityITBase {

    @Before
    public void setUpClient() {
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @Ignore("This doesn't always pass because of timing problems with loading updates from the admin file")
    @Test
    public void testInstances() throws Exception {
        serverPool.startCommonService(new EzSecurityHandler(), ezsecurityConstants.SERVICE_NAME, "12345");

        String file = properties.getProperty(EzBakePropertyConstants.EZBAKE_ADMINS_FILE);
        try (PrintWriter pw = new PrintWriter(new File(file))) {
            pw.println("- Hodor");
        }
        Thread.sleep(15000);

        String adminDn = "Hodor";
        ProxyPrincipal dn = getSignedPrincipal(adminDn);
        EzSecurityToken token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        assertTrue(token.getExternalProjectGroups().containsKey(EzSecurityConstant.EZ_INTERNAL_PROJECT));
        assertTrue(token.getExternalProjectGroups().get(
                EzSecurityConstant.EZ_INTERNAL_PROJECT).contains(EzSecurityConstant.EZ_INTERNAL_ADMIN_GROUP));
        assertTrue(EzbakeSecurityClient.isEzAdmin(token));


        try (PrintWriter pw = new PrintWriter(new File(file))) {
            pw.println("- Not Admins\n- Jeff");
        }
        Thread.sleep(20000);

        dn = getSignedPrincipal(adminDn);
        token = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);

        assertFalse(token.getExternalProjectGroups().containsKey(EzSecurityConstant.EZ_INTERNAL_PROJECT));
    }
}
