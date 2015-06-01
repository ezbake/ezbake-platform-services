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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCryptoException;
import ezbake.ezca.ezcaConstants;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.security.thrift.*;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.*;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;
import org.junit.*;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EzSecurityRegistrationHandlerTest extends HandlerBaseTest {

    private static Logger log = LoggerFactory.getLogger(EzSecurityRegistrationHandlerTest.class);
    private static ThriftClientPool clientPool;
    private static ThriftServerPool pool;

    public static final String SECURITY_ID = "SECURITY_ID";

    @BeforeClass
    public static void init() throws Exception {
        Properties ezConfig = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        ezConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                EzSecurityRegistrationHandlerTest.class.getResource("/conf/ssl/serverssl").getPath());
        ezConfig.setProperty(EzSecurityRegistrationHandler.PKEY_MODE, EzSecurityRegistrationHandler.PkeyDevMode);


        pool = new ThriftServerPool(ezConfig, 25844);

        pool.startCommonService(new EzSecurityRegistrationHandler(), EzSecurityRegistrationConstants.SERVICE_NAME, SECURITY_ID);
        pool.startCommonService(new EzCAMockHandler(), ezcaConstants.SERVICE_NAME, "101");

        clientPool = new ThriftClientPool(ezConfig);
    }

    @AfterClass
    public static void shutdown() {
        if (pool != null) {
            pool.shutdown();
        }
        if (clientPool != null) {
            clientPool.close();
        }
    }

    @Test
    public void testRegisterApp() throws TException {
        log.info("Testing Register Application");

        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");

        log.info("Attempting Retrieval of eztoken");
        EzSecurityToken ezToken = this.getTestEzSecurityToken();
        log.info("Got Token");
        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            log.info("Attempting Invocation");
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            log.info("Invoking registerApp");
            client.registerApp(ezToken, "nodeClient", "U", authorizations, null, "1235", admins, "App Dn 1");
            
            ApplicationRegistration appReg = client.getRegistration(ezToken, "1235");
            
            assertTrue(appReg != null);
            assertTrue(appReg.getAppDn().equals("App Dn 1"));

        } finally {
            clientPool.returnToPool(client);
        }

        

    }

    @Test
    public void testDuplicateSecIdRegistration() throws TException {
        log.info("Testing  Duplicate Security Id Registration Scenario");

        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");


        EzSecurityToken ezToken = this.getTestEzSecurityToken();
        EzSecurityRegistration.Client client = null;

        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            log.info("Invoking registerApp");
            client.registerApp(ezToken, "nodeClient", "U", authorizations,null, "100001", admins, "App Dn 1");
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            String id = client.registerApp(ezToken, "Different APP >:P", "TS", authorizations, null,"100001", admins, "App Dn 2");
            Assert.assertNotEquals(id, "100001");
        } finally {

            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testPromote() throws TException {
        log.info("Testing Promote");

        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");

        EzSecurityToken ezToken = this.getTestEzSecurityToken(true);
        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "nodeClient", "U", authorizations, null, "1234", admins, "App Dn 1");
            client.promote(ezToken, "1234");

            log.info("Promote Invoked...");

            RegistrationStatus status = client.getStatus(ezToken, "1234");

            assertTrue(status == RegistrationStatus.ACTIVE);
        } finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testPromoteTwice() throws Exception {
        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");

        EzSecurityToken ezToken = this.getTestEzSecurityToken(true);
        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();

        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");

        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "nodeClient", "U", authorizations, null,"1234", admins, "App Dn 1");
            client.promote(ezToken, "1234");
            AppCerts origCerts = client.getAppCerts(ezToken, "1234");

            ApplicationRegistration updatedRegistration = client.getRegistration(ezToken, "1234");
            updatedRegistration.setAuthorizations(Lists.newArrayList("U", "C"));
            updatedRegistration.setStatus(RegistrationStatus.PENDING);
            client.update(ezToken, updatedRegistration);
            client.promote(ezToken, "1234");
            AppCerts newCerts = client.getAppCerts(ezToken, "1234");
            assertTrue("Private Cert shouldn't change after updating and promoting again",
                    TBaseHelper.compareTo(newCerts.application_priv, origCerts.application_priv) == 0);
            assertTrue("Public Cert shouldn't change after updating and promoting again",
                    TBaseHelper.compareTo(newCerts.application_pub, origCerts.application_pub) == 0);

        } finally {
            clientPool.returnToPool(client);
        }
    }


    @Test
    public void testGet() throws TException {
        log.info("Testing Get");

        String id = "1236";
        EzSecurityToken ezToken = getTestEzSecurityToken(false, "CN=Gary Drocella,O=42six,S=Maryland,C=USA");

        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");

        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();

        try {
            log.info("Attempting to get client");
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);

            log.info("Invoking register app");

            client.registerApp(ezToken, "nodeClient", "U", authorizations, null,id, admins, "App Dn 1");

            ApplicationRegistration reg = client.getRegistration(ezToken, id);

            assertTrue(reg.getAppName().equals("nodeClient"));
            assertTrue(!reg.getOwner().contains(";"));
            log.debug("Owner: {}", reg.getOwner());
            assertTrue(reg.getOwner().equals("CN=Gary Drocella,O=42six,S=Maryland,C=USA"));
            
            
        } 
        finally {
            clientPool.returnToPool(client);
        }


    }

    @Test
    public void testDemote() throws TException {
        log.info("Testing Demote");

        List<String> auths = new LinkedList<>();
        auths.add("U");
        String id = "4321";
        EzSecurityToken ezToken = this.getTestEzSecurityToken(true);
        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "nodeClient", "U", auths, null, id, admins, "App Dn 1");
            client.promote(ezToken, id);
            client.demote(ezToken, id);
            RegistrationStatus status = client.getStatus(ezToken, id);

            assertTrue(status == RegistrationStatus.PENDING);
        } finally {
            clientPool.returnToPool(client);
        }

    }


    @Test
    public void testUpdate() throws TException {
        String id = "1234";
        String expectedDn = "App Dn 2";
        List<String> auths = Lists.newArrayList("U");
        EzSecurityToken ezToken = getTestEzSecurityToken();
        ApplicationRegistration appReg;

        EzSecurityRegistration.Client client = null;
        try {
            client = clientPool.getClient(EzSecurityServicesConstants.REGISTRATION_SERVICE_NAME, EzSecurityRegistration.Client.class);
            id = client.registerApp(ezToken, "appName", "A", auths, null, id, null, "App Dn 1");
            appReg = client.getRegistration(ezToken, id);

            appReg.setAppDn(expectedDn);
            client.update(ezToken, appReg);

            ApplicationRegistration reg = client.getRegistration(ezToken, id);
            assertEquals("appName", reg.getAppName());
            assertEquals(expectedDn, reg.getAppDn());
        }
        finally {
            clientPool.returnToPool(client);
        }

    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void testUpdateOnNonExistingSecId() throws TException {
        log.info("Testing Update on a Non Existing Security ID in the accumulo db");

        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        ApplicationRegistration appReg = new ApplicationRegistration("1122", "CN=John Doe,O=Unknown,C=TS", "App That Saves the World!", "TS", new ArrayList<String>(), null, null, admins, "App Dn 1");
        EzSecurityToken ezToken = this.getTestEzSecurityToken();

        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.update(ezToken, appReg);
        }  finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testDenial() throws TException {
        log.info("Testing Registration Denial");

        String id = "1234";
        EzSecurityToken ezToken = this.getTestEzSecurityToken(true);
        EzSecurityRegistration.Client client = null;
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "Appname", "U", new ArrayList<String>(), null, id, null, "App Dn 1");
            client.denyApp(ezToken, id);
            RegistrationStatus status = client.getStatus(ezToken, id);

            assertTrue(status == RegistrationStatus.DENIED);

        } finally {
            clientPool.returnToPool(client);
        }
    }


    @Test
    public void testGetStatus() throws TException {

        EzSecurityToken ezToken = this.getTestEzSecurityToken();
        List<String> authorizations = new LinkedList<>();
        authorizations.add("U");

        EzSecurityRegistration.Client client = null;
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "nodeClient", "U", authorizations, null, "12345", admins, "App Dn 1");
            RegistrationStatus status = client.getStatus(ezToken, "12345");

            assertTrue(status == RegistrationStatus.PENDING);


        } finally {
            clientPool.returnToPool(client);
        }

    }

    @Test
    public void testGetRegistrations() throws TException {
        log.info("Test Get Registrations");

        EzSecurityToken ezToken = this.getTestEzSecurityToken();
        EzSecurityRegistration.Client client = null;
        
        
        List<String> authorizations = new ArrayList<>();
        authorizations.add("U");
        
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "Some App", "Some Classification", authorizations, null, "2222", admins, "App Dn 1");
            
            ezToken.getTokenPrincipal().setPrincipal("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
            ezToken.setExternalProjectGroups(new HashMap<String, List<String>>());
            
            List<ApplicationRegistration> l = client.getRegistrations(ezToken);
            
            assertTrue(l != null);
            
            log.debug("Num Regs: " + l.size());

        }  finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testGetAllRegistrations() throws TException {
        log.info("Testing Get All Registrations");

        EzSecurityToken ezToken = this.getTestEzSecurityToken();
        EzSecurityRegistration.Client client = null;
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.getAllRegistrations(ezToken, RegistrationStatus.ACTIVE);
        }  finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testGetApplicationSecurityInfo() throws TException {
        log.info("Test Get Application Security Info");

        EzSecurityToken ezToken = getTestEzSecurityToken(true);

        List<String> auths = new ArrayList<>();
        auths.add("U");

        EzSecurityRegistration.Client client = null;

        String id = "19009";
        Set<String> admins = new HashSet<>();
        
        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
        
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "Mammoth", "TS", auths, null, id, admins, "App Dn 1");
            client.promote(ezToken, id);
            AppCerts info = client.getAppCerts(ezToken, id);

            String privateKey = new String(info.getApplication_priv());
            String publicKey = new String(info.getApplication_pub());
            String certificate = new String(info.getApplication_crt());

            log.info("Private Key: {}", privateKey);
            log.info("Public Key: {}", publicKey);
            log.info("Certificate: {}", certificate);

            assertTrue(!privateKey.isEmpty());
            assertTrue(!publicKey.isEmpty());
            assertTrue(!certificate.isEmpty());

        } finally {
            clientPool.returnToPool(client);
        }
    }


    @Test(expected=SecurityIDNotFoundException.class)
    public void testGetApplicationSecurityInfoInactive() throws TException {
        log.info("Test Get Application Security Info");

        EzSecurityToken ezToken = getTestEzSecurityToken(true);

        List<String> auths = new ArrayList<>();
        auths.add("U");

        EzSecurityRegistration.Client client = null;

        String id = "19009";
        Set<String> admins = new HashSet<>();

        admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");

        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "Mammoth", "TS", auths, null, id, admins, "App Dn 1");
            // don't promote
            client.getAppCerts(ezToken, id);
        } finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testAddAdmin() throws TException {
    	
    	EzSecurityToken ezToken = this.getTestEzSecurityToken();
    	EzSecurityRegistration.Client client = null;
    	
    	List<String> authorizations = new LinkedList<>();
    	authorizations.add("U");
    	
    	try {
    		client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
    		client.registerApp(ezToken, "The Application Name", "The Classification" , authorizations, null, "1324", null, "App Dn 1");
    		client.addAdmin(ezToken, "1324", "CN=Best Darn Admin There Ever Was,O=426,OU=Administration,C=Madagascar");
    		
    		ezToken.getTokenPrincipal().setPrincipal("CN=Best Darn Admin There Ever Was,O=426,OU=Administration,C=Madagascar");
    		ezToken.setExternalProjectGroups(new HashMap<String, List<String>>());
    		
    		ApplicationRegistration appReg = client.getRegistration(ezToken, "1324");
    		
    		assertTrue(appReg != null);
    		assertTrue(appReg.getAdmins().contains("CN=Best Darn Admin There Ever Was,O=426,OU=Administration,C=Madagascar"));
    	}
    	finally {
    		clientPool.returnToPool(client);
    	}
    }
    
    
    @Test(expected = SecurityIDNotFoundException.class)
    public void testNotAdminNorUserOfRegistration() throws TException {
    	
    	EzSecurityToken ezToken = this.getTestEzSecurityToken();
    	EzSecurityRegistration.Client client = null;
    	
    	List<String> authorizations = new LinkedList<>();
    	authorizations.add("U");
    	
    	Set<String> admins = new HashSet<>();
    	admins.add("CN=John Doe,CN=Administrator,O=42six,OU=Administration,ST=Maryland,C=USA");
    	
    	try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.registerApp(ezToken, "The Application Name", "The Classification", authorizations, null, "5678", admins, "App Dn 1");
            client.addAdmin(ezToken, "5678", "CN=Best Darn Admin There Ever Was,O=426,OU=Administration,C=Madagascar");

            ezToken.getTokenPrincipal().setPrincipal("CN=Kermit The Frog");
            ezToken.setExternalProjectGroups(new HashMap<String, List<String>>());

            client.getRegistration(ezToken, "5678");

        } finally {
    		clientPool.returnToPool(client);
    	}
    }
    
    @Test
    public void testRemoveAdmin() throws TException {
    	EzSecurityToken ezToken = this.getTestEzSecurityToken();
    	EzSecurityRegistration.Client client = null;
    	
    	List<String> auths = new LinkedList<>();
    	auths.add("U");
    	
    	Set<String> admins = new HashSet<>();
    	admins.add("CN=John Doe, CN=Administrator,O=42six,OU=Administration,ST=Marylnad,C=USA");
    	
    	try {
    		client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
    		client.registerApp(ezToken, "Awesome App!", "C", auths, null, "3456", admins, "App Dn 1");
    		
    		client.removeAdmin(ezToken, "3456", "CN=John Doe, CN=Administrator,O=42six,OU=Administration,ST=Marylnad,C=USA");
    		
    		ApplicationRegistration appReg = client.getRegistration(ezToken, "3456");
    		
    		assertTrue(appReg != null);
    		assertEquals(1, appReg.getAdmins().size()); // owner is an admin
    	}
    	finally {
    		clientPool.returnToPool(client);
    	}
    }

    @Test
    public void testAppTokenCanGetOwnRegistration() throws TException {
        String securityId = "12345";
        EzSecurityToken token = getTestEzSecurityToken();
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(securityId);

        EzSecurityRegistration.Client client = null;
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);

            client.registerApp(token, "App 123", "apple", Lists.newArrayList("U"), null, securityId, null, null);
            client.getRegistration(appToken, securityId);
        } finally {
            if (client != null)
                clientPool.returnToPool(client);
        }
    }


    /**
     * This test is for a bug when an application is promoted when it is already active
     */
    @Test
    public void testDoublePromote() throws TException {
        String appName = "Dune";
        String authLevel = "A";
        List<String> auths = Lists.newArrayList("C", "D");
        EzSecurityToken token = getTestEzSecurityToken(true);

        EzSecurityRegistration.Client client = clientPool.getClient(EzSecurityServicesConstants.REGISTRATION_SERVICE_NAME, EzSecurityRegistration.Client.class);
        try {
            ApplicationRegistration application;
            List<ApplicationRegistration> applications;

            // Register a new app and promote it
            String id1 = client.registerApp(token, appName, authLevel, auths, null, null, null, null);
            client.promote(token, id1);
            client.promote(token, id1);

            application = client.getRegistration(token, id1);
            applications = client.getAllRegistrations(token, RegistrationStatus.ACTIVE);
            Assert.assertEquals(1, applications.size());
            Assert.assertTrue(applications.contains(application));
        } finally {
            clientPool.returnToPool(client);
        }
    }


    
}
