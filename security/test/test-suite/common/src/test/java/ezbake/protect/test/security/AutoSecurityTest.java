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

package ezbake.protect.test.security;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbake.thrift.ThriftServerPool;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbakehelpers.accumulo.AccumuloHelper;

public class AutoSecurityTest {
    
    private static Logger log = LoggerFactory.getLogger(AutoSecurityTest.class);
    
    public static final String LOOKUP_TABLE = "ezsecurity_lookup";
    public static final String REG_TABLE = "ezsecurity_reg";

    
    private static ThriftServerPool serverPool;
    private static Properties configuration;
    
    private static Random random = new Random();
    
    private static int port;
    
    private static String id = "securityId";
    private static String id1 = "EzPy";
    
    private static String x509_cert_app;
    private static String x509_cert_ca;
    private static String app_pk;
    private static String app_public;
    
    @BeforeClass
    public static void init() throws Exception {
        
        InputStream is = AutoSecurityTest.class.getClassLoader().getResourceAsStream("pki/application.crt");
        x509_cert_app = read(is);
        x509_cert_ca = read(AutoSecurityTest.class.getClassLoader().getResourceAsStream("pki/ezbakeca.crt"));
        app_pk = read(AutoSecurityTest.class.getClassLoader().getResourceAsStream("pki/application.priv"));
        app_public = read(AutoSecurityTest.class.getClassLoader().getResourceAsStream("pki/application.pub"));
              
        log.debug("Using App Cert {}", x509_cert_app);
        log.debug("Using Ca Cert {}", x509_cert_ca);
        log.debug("Using Private Key {}", app_pk);
        log.debug("Using Public Key {}", app_public);
        
        configuration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

        Connector c = new AccumuloHelper(configuration).getConnector();
        
        AccumuloRegistrationManager arm = new AccumuloRegistrationManager(configuration);
        
        arm.register("5000","owner", "appName", "visibilityLevel", Lists.newArrayList("U"), new HashSet<String>(), "appDn");
        AppPersistenceModel app = arm.getRegistration(new String[]{"U"}, "5000", "owner", RegistrationStatus.PENDING);
        app.setX509Cert(x509_cert_app);
        app.setPrivateKey(app_pk);
        app.setPublicKey(app_public);
        
        arm.register("6000", "owner", "sappName", "visibilityLevel", Lists.newArrayList("U"), new HashSet<String>(), "appDn");
        AppPersistenceModel app1 = arm.getRegistration(new String[] {"U"}, "6000", "owner", RegistrationStatus.PENDING);
        app1.setX509Cert(x509_cert_app);
        app1.setPrivateKey(app_pk);
        app1.setPublicKey(app_public);
        
        arm.update(app, RegistrationStatus.PENDING);
        arm.update(app1, RegistrationStatus.PENDING);
    }
    
    @Test
    public void test() throws RegistrationException, SecurityIDNotFoundException, IOException, AccumuloException, AccumuloSecurityException {
        AutoSecurity autoSecurity = new AutoSecurity(configuration, true);
        
        log.debug("Value {}", SecurityID.ReservedSecurityId.EzSecurity.getId());
        AppPersistenceModel secModel = new AppPersistenceModel();
        
        secModel.setId(SecurityID.ReservedSecurityId.EzSecurity.getId());
        secModel.setOwner("owner");
        secModel.setPublicKey(app_public);
        secModel.setStatus(RegistrationStatus.PENDING);
        secModel.setAppName("Security App");
        secModel.setFormalAuthorizations(Lists.newArrayList("U"));
        secModel.setAuthorizationLevel("U");
        secModel.setAppDn("Fake Dn");
        writeRegistration(secModel);
        
        AppPersistenceModel caModel = new AppPersistenceModel();
        
        caModel.setId(SecurityID.ReservedSecurityId.CA.getId());
        caModel.setOwner("owner");
        caModel.setStatus(RegistrationStatus.PENDING);
        caModel.setAppName("Ca App Name");
        caModel.setFormalAuthorizations(Lists.newArrayList("U"));
        caModel.setAuthorizationLevel("U");
        caModel.setAppDn("Fake Dn");
        caModel.setX509Cert(x509_cert_ca);
        
        writeRegistration(caModel);
        
        Map<String, Path> map = autoSecurity.downloadCerts(new String[]{"U"}, "owner", RegistrationStatus.PENDING);
        
        Set<String> keySet = map.keySet();
        
        for(String k : keySet) {
            File[] certs = new File(map.get(k).toString()).listFiles();
            assertTrue(certs != null);
            assertTrue(certs.length == 7);
        }
    }
    
    /**
     * Serialize an application registration to accumulo
     *
     * @param registration the record to be written
     * @return true if there were no errors
     */
    protected boolean writeRegistration(AppPersistenceModel registration) throws RegistrationException {
        boolean status = false;
        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            MultiTableBatchWriter bw = connector.createMultiTableBatchWriter(1000000L, 1000L, 10);

            
            bw.getBatchWriter(REG_TABLE).addMutations(registration.getObjectMutations());
            
            bw.getBatchWriter(LOOKUP_TABLE).addMutations(registration.getLookupMutations());

            bw.close();

            status = true;
        } catch (IOException e) {
            log.error("IO Exception" + e);
            throw new RegistrationException("Error: IOException " + e);
        } catch (TableNotFoundException e) {
            log.error("Table not found for registration" + e);
            throw new RegistrationException("Error: Accumulo Misconfigured - table is not found " + e);
        } catch (MutationsRejectedException e) {
            log.error("Invalid mutation" + e);
            throw new RegistrationException("Error: Mutation Rejected " + e);
        } catch (AccumuloSecurityException e) {
            throw new RegistrationException("Accumulo Security Exception: " + e);
        } catch (AccumuloException e) {
            throw new RegistrationException("Accumulo : " + e);
        } catch (AppPersistCryptoException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return status;
    }

    protected void writeRow(Map.Entry<Key, Value> row, Text value) throws RegistrationException {
        Connector connector = null;
        BatchWriter writer = null;
        try {
            connector = new AccumuloHelper(configuration).getConnector();
            writer = connector.createBatchWriter(REG_TABLE, 1000000L, 1000L, 10);
            Mutation m = new Mutation(row.getKey().getRow());
            m.put(row.getKey().getColumnFamily(), row.getKey().getColumnQualifier(), new ColumnVisibility("U"), new Value(value.getBytes()));
            writer.addMutation(m);
        } catch (IOException e) {
            throw new RegistrationException("Error: IOException " + e);
        } catch (TableNotFoundException e) {
            throw new RegistrationException("Error: Accumulo Misconfigured - table is not found " + e);
        } catch (MutationsRejectedException e) {
            throw new RegistrationException("Error: Mutation Rejected " + e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    throw new RegistrationException("Error: Mutation Rejected " + e);
                }
            }
        }
    }
    
    protected static String read(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String file = "";
        String line = null;
        
        while((line=reader.readLine()) != null) {
            file += line + "\n";
        }
        
        return file;
    }
}
