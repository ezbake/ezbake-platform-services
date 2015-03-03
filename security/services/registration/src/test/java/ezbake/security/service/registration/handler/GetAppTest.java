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
import java.util.List;

/**
 * User: jhastings
 * Date: 4/8/14
 * Time: 3:00 PM
 */
public class GetAppTest extends HandlerBaseTest {

    @Test
    public void testRegisterAndGet() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException {
        EzSecurityToken token = getTestEzSecurityToken(false);
        final String appName = "Dune";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");
        String id = handler.registerApp(token, appName, appClass, appAuths, null, null, "app dn");
        ApplicationRegistration reg = handler.getRegistration(token, id);

        Assert.assertEquals(id, reg.getId());
        Assert.assertEquals(appName, reg.getAppName());
        Assert.assertEquals(token.getTokenPrincipal().getPrincipal(), reg.getOwner());
        Assert.assertEquals(appClass, reg.getClassification());
        Assert.assertArrayEquals(appAuths.toArray(), reg.getAuthorizations().toArray());
    }

    @Test
    public void testGetRegistrationsAdminSeesAll() throws RegistrationException, SecurityIDExistsException, TException {
        EzSecurityToken adminToken = getTestEzSecurityToken(true);
        EzSecurityToken user1Token = getTestEzSecurityToken(false, "User1");
        EzSecurityToken user2Token = getTestEzSecurityToken(false, "User2");

        final String appName = "TestApp1";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");


        handler.registerApp(user1Token, appName, appClass, appAuths, null,null, "app dn");
        handler.registerApp(user2Token, appName, appClass, appAuths, null,null, "app dn");

        List<ApplicationRegistration> registrations = handler.getRegistrations(adminToken);
        Assert.assertEquals(2, registrations.size());
    }

    @Test
    public void testOneUserDoesntSeeOtherUsers() throws RegistrationException, SecurityIDExistsException, TException {
        EzSecurityToken user1Token = getTestEzSecurityToken(false, "User1");
        EzSecurityToken user2Token = getTestEzSecurityToken(false, "User2");

        final String appName = "TestApp1";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");


        handler.registerApp(user1Token, appName, appClass, appAuths, null,null, "app dn");
        handler.registerApp(user2Token, appName, appClass, appAuths, null,null, "app dn");

        List<ApplicationRegistration> registrations = handler.getRegistrations(user1Token);
        Assert.assertEquals(1, registrations.size());
    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void testOneUserDoesntSeeOtherUsersWhenQueryById() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException {
        EzSecurityToken user1Token = getTestEzSecurityToken(false, "User1");
        EzSecurityToken user2Token = getTestEzSecurityToken(false, "User2");

        final String appName = "TestApp1";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");


        handler.registerApp(user1Token, appName, appClass, appAuths, null,null, "app dn");
        String hiddenID = handler.registerApp(user2Token, appName, appClass, appAuths, null,null, "app dn");

        ApplicationRegistration registrations = handler.getRegistration(user1Token, hiddenID);
        Assert.assertNull(registrations);
    }

    @Test
    public void testGetAllReturnsPendingWithFilter() throws RegistrationException, SecurityIDExistsException, TException {
        EzSecurityToken user1Token = getTestEzSecurityToken(false, "User1");
        final String appName = "TestApp1";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");


        handler.registerApp(user1Token, appName, appClass, appAuths, null,null, "app dn");
        List<ApplicationRegistration> registrations = handler.getAllRegistrations(user1Token, RegistrationStatus.PENDING);
        Assert.assertEquals(1, registrations.size());
    }

    @Test
    public void testGetAllReturnsPendingWithoutFilter() throws RegistrationException, SecurityIDExistsException, TException {
        EzSecurityToken user1Token = getTestEzSecurityToken(false, "User1");
        final String appName = "TestApp1";
        final String appClass = "U";
        final List<String> appAuths = Arrays.asList("U");


        handler.registerApp(user1Token, appName, appClass, appAuths, null,null, "app dn");
        List<ApplicationRegistration> registrations = handler.getAllRegistrations(user1Token);
        Assert.assertEquals(1, registrations.size());
    }

}
