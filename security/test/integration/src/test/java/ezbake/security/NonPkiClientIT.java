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
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.client.provider.TokenProvider;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

/**
 * User: jhastings
 * Date: 10/13/14
 * Time: 9:26 PM
 */
public class NonPkiClientIT extends EzSecurityMockITBase {

    @BeforeClass
    public static void setSecurityId() {
        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, "");
        properties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "10000000");
        properties.setProperty(TokenProvider.CLIENT_MODE, TokenProvider.ClientMode.Dev.getValue());
    }

    @Before @Override
    public void setUpClient() {
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @Test
    public void appInfo() throws TException {
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
}
