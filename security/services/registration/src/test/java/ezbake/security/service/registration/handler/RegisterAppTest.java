/*   Copyright (C) 2013-2015 Computer Sciences Corporation
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

package ezbake.security.service.registration.handler;

import ezbake.base.thrift.EzSecurityToken;

import ezbake.security.thrift.*;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 4/8/14
 * Time: 1:20 PM
 */
public class RegisterAppTest extends HandlerBaseTest {

    @Test
    public void testRegisterNullId() throws RegistrationException, SecurityIDExistsException, TException {
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");

        String id = handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, null,null, "app dn");
        Assert.assertNotNull(id);
        Assert.assertTrue(!id.isEmpty());
    }

    @Test
    public void testRegisterEmptyId() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException {
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final EzSecurityToken token = getTestEzSecurityToken(false);
        String id = handler.registerApp(token, appName, classification, auths, "",null, "app dn");

        Assert.assertNotNull(id);
        Assert.assertTrue(!id.isEmpty());
    }

    @Test
    public void testRegisterPassedId() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException {
        final String appId = "93939393";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final EzSecurityToken token = getTestEzSecurityToken(false);


        String id = handler.registerApp(token, appName, classification, auths, appId,null, "app dn");
        Assert.assertEquals(appId, id);

        ApplicationRegistration reg = handler.getRegistration(token, appId);
        Assert.assertEquals(appId, reg.getId());
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), reg.getOwner());
        Assert.assertEquals(appName, reg.getAppName());
        Assert.assertArrayEquals(auths.toArray(), reg.getAuthorizations().toArray());
        Assert.assertEquals(RegistrationStatus.PENDING, reg.getStatus());
    }

    @Test(expected=SecurityIDExistsException.class)
    public void testRegisterPassedIdAlreadyExists() throws RegistrationException, SecurityIDExistsException, TException {
        final String appId = "3030303";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");

        handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, appId,null, "app dn");

        // Should throw here
        handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, appId, null, "app dn");
    }

    @Test
    public void testOtherAppHasSecurityId() throws RegistrationException, SecurityIDExistsException, TException {
        final String appId = "4040494";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");

        String id = handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, appId, null, "app dn");
        String other = handler.registerApp(getTestEzSecurityToken(false), "othername", classification, auths, appId, null, "app dn");

        Assert.assertNotEquals(id, other);
    }

    @Test
    public void registerWithAdmin() throws RegistrationException, SecurityIDExistsException, TException, PermissionDeniedException, SecurityIDNotFoundException {
        final String appId = "1000303";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final Set<String> admins = new HashSet<String>(Arrays.asList("Jeff"));
        final EzSecurityToken token = getTestEzSecurityToken();

        String id = handler.registerApp(token, appName, classification, auths, appId, admins, "app dn");
        ApplicationRegistration reg = handler.getRegistration(token, appId);

        Assert.assertEquals(appId, reg.getId());
        Assert.assertEquals(appName, reg.getAppName());
        Assert.assertArrayEquals(auths.toArray(), reg.getAuthorizations().toArray());
        Assert.assertEquals(classification, reg.getClassification());
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), reg.getOwner());
        Assert.assertArrayEquals(admins.toArray(), reg.getAdmins().toArray());
    }

    @Test
    public void testOwnerIsAdmin() throws RegistrationException, SecurityIDExistsException, PermissionDeniedException, SecurityIDNotFoundException {
        final String appId = "1000303";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final EzSecurityToken token = getTestEzSecurityToken();

        String id = handler.registerApp(token, appName, classification, auths, appId, null, "app dn");
        ApplicationRegistration reg = handler.getRegistration(token, appId);
        Assert.assertTrue(reg.getAdmins().contains(token.getTokenPrincipal().getPrincipal()));
    }

    @Test(expected=RegistrationException.class)
    public void registerReservedIdString() throws TException {
        final String appId = "_Ez_TestId";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final EzSecurityToken token = getTestEzSecurityToken(false);

        String id = handler.registerApp(token, appName, classification, auths, appId, null, "app dn");
    }

    @Test(expected=RegistrationException.class)
    public void registerReservedIdNumber() throws TException {
        final String appId = "0";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final EzSecurityToken token = getTestEzSecurityToken(false);

        String id = handler.registerApp(token, appName, classification, auths, appId, null, "app dn");
    }
}
