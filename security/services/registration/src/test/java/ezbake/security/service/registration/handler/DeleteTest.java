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

import com.google.common.collect.Sets;
import ezbake.security.thrift.*;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 4/21/14
 * Time: 11:55 AM
 */
public class DeleteTest extends HandlerBaseTest {
    static final Logger logger = LoggerFactory.getLogger(DeleteTest.class);

    @Test(expected=SecurityIDNotFoundException.class)
    public void registerAndDelete() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException {
        final String id = "99393939";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");

        try {
            handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, id, null, "App Dn");
            handler.deleteApp(getTestEzSecurityToken(true), id);
        } catch (Exception e) {
            Assert.fail("Unexpected exception");
        }

        handler.getRegistration(getTestEzSecurityToken(), id);
    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void registerAndDeleteAsOwner() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException {
        final String id = "99393939";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");

        try {
            handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, id, null, "App Dn");
            handler.deleteApp(getTestEzSecurityToken(false), id);
        } catch (Exception e) {
            Assert.fail("Unexpected exception");
        }

        handler.getRegistration(getTestEzSecurityToken(), id);
    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void registerAndDeleteAppWithAdmins() throws RegistrationException, SecurityIDExistsException, TException, SecurityIDNotFoundException, PermissionDeniedException, IOException, TableNotFoundException {
        final String id = "99393939";
        final String appName = "Atreides";
        final String classification = "U";
        final List<String> auths = Arrays.asList("FOUO");
        final Set<String> admins = Sets.newHashSet("Jared", "Frank");

        try {
            handler.registerApp(getTestEzSecurityToken(false), appName, classification, auths, id, admins, "App Dn");
            ApplicationRegistration registered = handler.getRegistration(getTestEzSecurityToken(), id);
            registered.getAdmins().add("Bill");
            handler.update(getTestEzSecurityToken(false), registered);
            handler.deleteApp(getTestEzSecurityToken(false), id);
        } catch (Exception e) {
            Assert.fail("Unexpected exception");
        }

        handler.getRegistration(getTestEzSecurityToken(), id);
    }



}
