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

package ezbake.security.persistence.impl;

import com.google.common.collect.Lists;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * User: jhastings
 * Date: 10/9/13
 * Time: 12:12 PM
 */
public class TestRegistrationManager {
    private static final Logger logger = LoggerFactory.getLogger(TestRegistrationManager.class);
    protected RegistrationManager reg;

    private static Properties ezConfiguration;
    protected static String[] scanAuths = new String[] {"all"};
    protected static String[] pendAuths = new String[] {"all", "pending"};

    public static final String bob1 = "3000000";
    public static final String bob2 = "3000001";
    public static final String jim1 = "3000002";

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException, IOException, RegistrationException, EzConfigurationLoaderException {
        ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

        reg = new AccumuloRegistrationManager(ezConfiguration, AccumuloRegistrationManager.REG_TABLE);

        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        admins.add("Other Admin");
        reg.register(bob1, "Bob", "TestApp1", "med", Arrays.asList("X", "Y", "Z"), admins, "App Dn 1");
        reg.register(bob2, "Bob", "TestApp2", "high", Arrays.asList("X", "Y", "Z"), null, "App Dn 2");
        reg.register(jim1, "Jim", "Jim1", "low", Arrays.asList("X", "Y", "Z"), null, "App Dn 3");
    }

    @After
    public void tearDown() throws RegistrationException, SecurityIDNotFoundException, IOException, TableNotFoundException, MutationsRejectedException {
        Connector connector = new AccumuloHelper(ezConfiguration).getConnector();

        Scanner regScanner = connector.createScanner(AccumuloRegistrationManager.REG_TABLE, new Authorizations(pendAuths));
        List<Range> regRows = new ArrayList<Range>();
        for (Map.Entry<Key, Value> entry : regScanner) {
            regRows.add(new Range(entry.getKey().getRow()));
        }
        if (!regRows.isEmpty()) {
            BatchDeleter regDeleter = connector.createBatchDeleter(AccumuloRegistrationManager.REG_TABLE, new Authorizations(pendAuths), 10, 1000000L, 1000L, 10);
            regDeleter.setRanges(regRows);
            regDeleter.delete();
            regDeleter.close();
        }

        Scanner lookScanner = connector.createScanner(AccumuloRegistrationManager.LOOKUP_TABLE, new Authorizations(pendAuths));
        List<Range> lookRows = new ArrayList<Range>();
        for (Map.Entry<Key, Value> entry : lookScanner) {
            lookRows.add(new Range(entry.getKey().getRow()));
        }
        if (!lookRows.isEmpty()) {
            BatchDeleter lookDeleter = connector.createBatchDeleter(AccumuloRegistrationManager.LOOKUP_TABLE, new Authorizations(pendAuths), 10, 1000000L, 1000L, 10);
            lookDeleter.setRanges(lookRows);
            lookDeleter.delete();
            lookDeleter.close();
        }
    }

    /***** Test Registration visibility to owners and admins ******/
    @Test
    public void testRegisterAndGetAsOwner() throws RegistrationException, SecurityIDNotFoundException {
        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        reg.register("10494885", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), admins, "App Dn 1");
        reg.getRegistration(pendAuths, "10494885", "Test Owner", null);
    }
    @Test
    public void testRegisterAndGetAsAdmin() throws RegistrationException, SecurityIDNotFoundException {
        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        reg.register("10494885", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), admins, "App Dn 2");
        reg.getRegistration(pendAuths, "10494885", "Test Admin", null);
    }
    @Test
    public void testRegisterAndGetNull() throws RegistrationException, SecurityIDNotFoundException {
        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        reg.register("10494885", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), admins, "App Dn 3");
        reg.getRegistration(pendAuths, "10494885", null, null);
    }
    @Test(expected=SecurityIDNotFoundException.class)
    public void testRegisterAndGetAsOther() throws RegistrationException, SecurityIDNotFoundException {
        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        reg.register("10494885", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), admins, "App Dn 4");
        reg.getRegistration(pendAuths, "10494885", "Test Other", null);
    }

    @Test
    public void testRegisterAndDelete() throws RegistrationException, SecurityIDNotFoundException, IOException, TableNotFoundException {
        Set<String> admins = new HashSet<>();
        reg.register("10494885", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"),
                Collections.<String>emptySet(), "App Dn 2");
        AppPersistenceModel model = reg.getRegistration(pendAuths, "10494885", "Test Owner", null);

        // Delete registration
        reg.delete(pendAuths, "10494885");

        Connector connector = new AccumuloHelper(ezConfiguration).getConnector(false);
        Scanner regScanner = connector.createScanner(AccumuloRegistrationManager.REG_TABLE, new Authorizations(pendAuths));
        Scanner lookScanner = connector.createScanner(AccumuloRegistrationManager.LOOKUP_TABLE, new Authorizations(pendAuths));

        // Make sure the security Id is not found anywhere anymore
        List<String> rowIds = Lists.newArrayList();
        for (Map.Entry<Key, Value> s : regScanner) {
            rowIds.add(s.getKey().getRow().toString());
            rowIds.add(s.getKey().getColumnFamily().toString());
            rowIds.add(s.getKey().getColumnQualifier().toString());
            rowIds.add(s.getValue().toString());
        }
        for (Map.Entry<Key, Value> s : lookScanner) {
            rowIds.add(s.getKey().getRow().toString());
            rowIds.add(s.getKey().getColumnFamily().toString());
            rowIds.add(s.getKey().getColumnQualifier().toString());
            rowIds.add(s.getValue().toString());
        }
        Assert.assertTrue(!rowIds.contains("10494885"));
    }

    /****** Test various registration statuses ********/
    @Test
    public void testApprove() throws RegistrationException, SecurityIDNotFoundException {
        reg.register("98484847", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), null, "The App Dn 1");
        AppPersistenceModel registration = reg.getRegistration(scanAuths, "98484847", null, null);
        Assert.assertNotNull(registration);
        Assert.assertEquals(RegistrationStatus.PENDING, registration.getStatus());

        reg.approve(scanAuths, "98484847");
        registration = reg.getRegistration(scanAuths, "98484847", null, null);
        Assert.assertNotNull(registration);
        Assert.assertEquals(RegistrationStatus.ACTIVE, registration.getStatus());
    }
    @Test
    public void testDeny() throws RegistrationException, SecurityIDNotFoundException {
        reg.register("98484847", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), null, "The App Dn 2");
        AppPersistenceModel registration = reg.getRegistration(scanAuths, "98484847", null, null);
        Assert.assertNotNull(registration);
        Assert.assertEquals(RegistrationStatus.PENDING, registration.getStatus());

        reg.deny(scanAuths, "98484847");
        registration = reg.getRegistration(scanAuths, "98484847", null, null);
        Assert.assertNotNull(registration);
        Assert.assertEquals(RegistrationStatus.DENIED, registration.getStatus());
    }
    @Test
    public void testSetStatus() throws RegistrationException, SecurityIDNotFoundException {
        reg.register("984848", "Test Owner", "App Name", "low", Arrays.asList("V", "Q", "Y"), null, "The App Dn 4");

        reg.setStatus("984848", RegistrationStatus.ACTIVE);
        AppPersistenceModel registration = reg.getRegistration(scanAuths, "984848", null, null);
        Assert.assertEquals(RegistrationStatus.ACTIVE, registration.getStatus());

        reg.setStatus("984848", RegistrationStatus.PENDING);
        registration = reg.getRegistration(scanAuths, "984848", null, null);
        Assert.assertEquals(RegistrationStatus.PENDING, registration.getStatus());

        reg.setStatus("984848", RegistrationStatus.DENIED);
        registration = reg.getRegistration(scanAuths, "984848", null, null);
        Assert.assertEquals(RegistrationStatus.DENIED, registration.getStatus());

        reg.setStatus("984848", RegistrationStatus.DENIED);
        registration = reg.getRegistration(scanAuths, "984848", null, null);
        Assert.assertEquals(RegistrationStatus.DENIED, registration.getStatus());

        List<AppPersistenceModel> registrations = reg.all(scanAuths, null, RegistrationStatus.DENIED);
        Assert.assertEquals(1, registrations.size());
        Assert.assertTrue(registrations.contains(registration));
    }

    /****** Test Registration Getters ********/
    @Test
    public void getApp() throws RegistrationException, SecurityIDNotFoundException {
        String id = "40000";
        Set<String> admins = new HashSet<String>();
        admins.add("Test Admin");
        reg.register(id, "TESTER", "APPNAME", "XYZ", Arrays.asList("I", "J", "K"), admins, "lol");

        AppPersistenceModel registration = reg.getRegistration(pendAuths, id, null, null);
        Assert.assertNotNull(registration);
        Assert.assertEquals("APPNAME", registration.getAppName());
        Assert.assertEquals("TESTER", registration.getOwner());
        Assert.assertEquals("XYZ", registration.getAuthorizationLevel());
        Assert.assertEquals(RegistrationStatus.PENDING, registration.getStatus());
        Assert.assertArrayEquals(new String[]{"I", "J", "K"}, registration.getFormalAuthorizations().toArray());
        Assert.assertEquals(admins, registration.getAdmins());
    }
    @Test
    public void testGetAllAsEzAdmin() throws RegistrationException {
        List<AppPersistenceModel> registrations = reg.all(pendAuths, null, null);
        Assert.assertEquals(3, registrations.size());
    }
    @Test
    public void testGetAllAsOwner() throws RegistrationException {
        List<AppPersistenceModel> registrations = reg.all(pendAuths, "Bob", null);
        Assert.assertEquals(2, registrations.size());
    }
    @Test
    public void testGetAllAsAdmin() throws RegistrationException {
        List<AppPersistenceModel> registrations = reg.all(pendAuths, "Test Admin", null);
        Assert.assertEquals(1, registrations.size());
    }
    @Test
    public void testGetAllStatusAsEzAdmin() throws RegistrationException, SecurityIDNotFoundException {
        reg.setStatus(bob1, RegistrationStatus.ACTIVE);
        List<AppPersistenceModel> registrations = reg.all(pendAuths, null, RegistrationStatus.PENDING);
        Assert.assertEquals(2, registrations.size());
    }
    @Test
    public void testGetAllStatusAsOwner() throws RegistrationException, SecurityIDNotFoundException {
        reg.setStatus(bob1, RegistrationStatus.ACTIVE);
        List<AppPersistenceModel> registrations = reg.all(pendAuths, "Bob", RegistrationStatus.PENDING);
        Assert.assertEquals(1, registrations.size());
    }
    @Test
    public void testGetAllStatusAsAdmin() throws RegistrationException, SecurityIDNotFoundException {
        reg.setStatus(bob1, RegistrationStatus.ACTIVE);
        List<AppPersistenceModel> registrations = reg.all(pendAuths, "Test Admin", RegistrationStatus.PENDING);
        Assert.assertEquals(0, registrations.size());
    }

    @Test
    public void testGetStatus() throws SecurityIDNotFoundException {
        RegistrationStatus mod = reg.getStatus(pendAuths, bob1);
        Assert.assertEquals(RegistrationStatus.PENDING, mod);
    }

    @Test
    public void testContainsId() throws SecurityIDNotFoundException, RegistrationException {
        boolean mod = reg.containsId(pendAuths, bob1);
        Assert.assertTrue(mod);
    }

    @Test
    public void testRegisterWithCommunityAuths() throws RegistrationException, SecurityIDNotFoundException {
        AppPersistenceModel regModel = new AppPersistenceModel();
        regModel.setId("TEST!@#$");
        regModel.setOwner("Ben");
        regModel.setAppName("Community");
        regModel.setAuthorizationLevel("MED");
        regModel.setFormalAuthorizations(Lists.newArrayList("DOCTOR"));
        regModel.setCommunityAuthorizations(Lists.newArrayList("PEDIATRIC"));

        reg.register(regModel);

        AppPersistenceModel persistedModel = reg.getRegistration(new String[]{"U"}, regModel.getId(), null, null);

        Assert.assertEquals(regModel.getCommunityAuthorizations(), persistedModel.getCommunityAuthorizations());
    }

    @Test
    public void testRegisterWithNullCommunityAuths() throws RegistrationException, SecurityIDNotFoundException {
        AppPersistenceModel regModel = new AppPersistenceModel();
        regModel.setId("TEST!@#$");
        regModel.setOwner("Ben");
        regModel.setAppName("Community");
        regModel.setAuthorizationLevel("MED");
        regModel.setFormalAuthorizations(Lists.newArrayList("DOCTOR"));
        regModel.setCommunityAuthorizations(null);

        reg.register(regModel);

        AppPersistenceModel persistedModel = reg.getRegistration(new String[]{"U"}, regModel.getId(), null, null);

        Assert.assertEquals(regModel.getCommunityAuthorizations(), persistedModel.getCommunityAuthorizations());
    }

    /**** Test Adding and Removal of Admins ****/
    @Test
    public void testAddAdminToExisting() throws TException, SecurityIDNotFoundException, RegistrationException {
        reg.addAdmin(pendAuths, bob1, "NewAdmin");
        reg.getRegistration(pendAuths, bob1, "NewAdmin", null);
    }
    @Test(expected=SecurityIDNotFoundException.class)
    public void testRemoveAdmin() throws RegistrationException, SecurityIDNotFoundException {
        reg.removeAdmin(pendAuths, bob1, "Test Admin");
        AppPersistenceModel registration = reg.getRegistration(pendAuths, bob1, "Bob", null);
        Assert.assertTrue(!registration.getAdmins().contains("Test Admin"));
        reg.getRegistration(pendAuths, bob1, "Test Admin", null);
    }
    @Test
    public void testRemovalDoesntAffectOwner() throws RegistrationException, SecurityIDNotFoundException {
        reg.removeAdmin(pendAuths, bob1, "Test Admin");
        AppPersistenceModel model = reg.getRegistration(pendAuths, bob1, "Bob", null);
        Assert.assertNotNull(model);
    }
    @Test
    public void testRemovalDoesntAffectOther() throws RegistrationException, SecurityIDNotFoundException {
        reg.removeAdmin(pendAuths, bob1, "Test Admin");
        AppPersistenceModel model = reg.getRegistration(pendAuths, bob1, "Other Admin", null);
        Assert.assertNotNull(model);
    }
    @Test
    public void testAddAdminDoesntAffectOwner() throws RegistrationException, SecurityIDNotFoundException {
        reg.addAdmin(pendAuths, bob1, "New Admin");
        AppPersistenceModel model = reg.getRegistration(pendAuths, bob1, "Bob", null);
        Assert.assertNotNull(model);
    }
    @Test
    public void testAddDoesntAffectOther() throws RegistrationException, SecurityIDNotFoundException {
        reg.removeAdmin(pendAuths, bob1, "Test2 Admin");
        AppPersistenceModel model = reg.getRegistration(pendAuths, bob1, "Other Admin", null);
        Assert.assertNotNull(model);
    }

    @Test
    public void update() throws RegistrationException, SecurityIDNotFoundException {
        reg.approve(scanAuths, bob1);
        AppPersistenceModel app1 = reg.getRegistration(scanAuths, bob1, null, RegistrationStatus.ACTIVE);
        Assert.assertNotNull(app1);
        Assert.assertEquals(RegistrationStatus.ACTIVE, app1.getStatus());

        app1.setAppName("Bob's new application");
        reg.update(app1, RegistrationStatus.PENDING);

        app1 = reg.getRegistration(scanAuths, bob1, null, RegistrationStatus.PENDING);
        Assert.assertNotNull(app1);
        Assert.assertEquals("Bob's new application", app1.getAppName());
        Assert.assertEquals(RegistrationStatus.PENDING, app1.getStatus());
    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void updateNonExisting() throws RegistrationException, SecurityIDNotFoundException {
        AppPersistenceModel non = new AppPersistenceModel();
        non.setId("NonExist");
        non.setAppName("Blank");
        non.setOwner("Noone");

        reg.update(non, null);
    }

    /*** Reserved Cert tests ***/
    @Test(expected=RegistrationException.class)
    public void testCASecurityIDReserved() throws RegistrationException {
        reg.register(SecurityID.ReservedSecurityId.CA.getId(), "System", "CA", "low",
                Arrays.asList("V", "Q", "Y"), null, "App Dn 1");
    }
    @Test(expected=RegistrationException.class)
    public void testEzSecurityIDReserved() throws RegistrationException {
        reg.register(SecurityID.ReservedSecurityId.EzSecurity.getId(), "System", "CA", "low",
                Arrays.asList("V", "Q", "Y"), null, "App Dn 2");
    }
    @Test(expected=RegistrationException.class)
    public void testEzEFEIDReserved() throws RegistrationException {
        reg.register(SecurityID.ReservedSecurityId.EFE.getId(), "System", "CA", "low",
                Arrays.asList("V", "Q", "Y"), null, "App Dn 3");
    }
    @Test(expected=RegistrationException.class)
    public void testEzRegistrationIDReserved() throws RegistrationException {
        reg.register(SecurityID.ReservedSecurityId.Registration.getId(), "System", "CA", "low",
                Arrays.asList("V", "Q", "Y"), null, "App Dn 4");
    }


    @Test
    public void testGetRegOwnerAndStatus() throws RegistrationException, SecurityIDNotFoundException {
        reg.getRegistration(pendAuths, bob1, "Bob", RegistrationStatus.PENDING);
        reg.getRegistration(pendAuths, bob1, "Bob", null);
        reg.getRegistration(pendAuths, bob1, null, RegistrationStatus.PENDING);
    }
    @Test(expected=SecurityIDNotFoundException.class)
    public void testGetRegOwnerWrongStatus() throws RegistrationException, SecurityIDNotFoundException {
        reg.getRegistration(pendAuths, bob1, "Bob", RegistrationStatus.ACTIVE);
    }
    @Test(expected=SecurityIDNotFoundException.class)
    public void testGetRegWrongOwnerAndStatus() throws RegistrationException, SecurityIDNotFoundException {
        reg.getRegistration(pendAuths, bob1, "bob", RegistrationStatus.PENDING);
    }
}
