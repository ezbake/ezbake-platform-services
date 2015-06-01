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

package ezbake.protect.ezca;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import ezbake.common.properties.EzProperties;
import ezbake.configuration.*;
import ezbake.persist.EzPersist;
import ezbake.persist.FilePersist;
import ezbake.persist.exception.EzPKeyError;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.AppCerts;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.crypto.utils.CryptoUtil;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbakehelpers.accumulo.AccumuloHelper;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 7:26 AM
 */
public class EzCABootstrap {
    private static final Logger logger = LoggerFactory.getLogger(EzCABootstrap.class);
    public static final String CA_NAME_KEY = "ezbake.ca.name";
    public static final String CA_NAME_DEF = "ezbakeca";
    public static final String CLIENTS_NAMES_KEY = "ezca.autogen.clients";
    public static final String CLIENTS_NAMES_DEF = "_Ez_Security,_Ez_EFE,_Ez_Registration,_Ez_Deployer";
    public static final String SECURITY_ENCRYPT = "ezbake.security.certs.encrypt";

    private Parameters parameters;
    private String ca_cert;
    private byte[] ca_jks;
    private String ezsec_pub;
    private Properties properties;

    private AppPersistenceModel regAppModel;
    
    private List<AppPersistenceModel> apps;

    public EzCABootstrap(String[] args) throws EzConfigurationLoaderException {
        parameters = new Parameters(args);
        properties = new EzConfiguration(
                new DirectoryConfigurationLoader(),
                new OpenShiftConfigurationLoader(),
                new DirectoryConfigurationLoader(Paths.get(parameters.configDir))
        ).getProperties();
    }

    public EzCABootstrap(Properties p, String[] args) {
        parameters = new Parameters(args);
        properties = p;
    }

    public int run() throws DataLengthException, IllegalStateException, InvalidCipherTextException, EzConfigurationLoaderException {
        EzProperties ezConfiguration = new EzProperties(properties, true);
        boolean isEncrypting = ezConfiguration.getBoolean(SECURITY_ENCRYPT, false);

        // Get what we need to bootstrap from ezconfiguration
        String caRowId = ezConfiguration.getProperty(CA_NAME_KEY, CA_NAME_DEF);
        String clients = ezConfiguration.getProperty(CLIENTS_NAMES_KEY, CLIENTS_NAMES_DEF);
        
        // Override the clients if passed on cli
        if (parameters.clientNames != null && !parameters.clientNames.isEmpty()) {
            clients = parameters.clientNames;
        }
        List<String> clientRowIds = Lists.newArrayList(Splitter.on(",").split(clients));

        // Create the persist layer connector for reading the things
        logger.info("EzPersist instance configured for directory {}", parameters.directory);
        EzPersist ezPersist = new FilePersist(parameters.directory);

        // List of apps to bootstrap
        List<AppPersistenceModel> apps = new ArrayList<AppPersistenceModel>();
     
        // Get the CA or fail
        try {
            Map<String, String> caRow = ezPersist.row(caRowId, AccumuloRegistrationManager.REG_TABLE);
            AppPersistenceModel cap = new AppPersistenceModel();
            cap.populateEzPersist(caRow);
            cap.setEncrypting(isEncrypting);
            cap.setAuthorizationLevel("U");

            // Save ca certs on the instance
            ca_cert = cap.getX509Cert();
            ca_jks = CryptoUtil.load_jks(ca_cert);

            logger.info("CA certificate found, ID: {}, Name: {}", cap.getId(), cap.getAppName());
            apps.add(cap);
        } catch (EzPKeyError ezPKeyError) {
            logger.error("CA Certificate not found with EzPersist. Cannot continue bootstrapping", ezPKeyError);
            return 1;
        } catch (AppPersistCryptoException e) {
            logger.error("Unable to encrypt keys. Cannot continue bootstrapping", e);
            return 1;
        } catch (IOException e) {
            logger.error("Failed to load keys. Cannot continue bootstrapping", e);
            return 1;
        }

        // Get the client certificates
        logger.info("Looking for client certificates: {}", clientRowIds);
        for (String client : clientRowIds) {
            try {
                Map<String, String> clientRows = ezPersist.row(client, AccumuloRegistrationManager.REG_TABLE);
                AppPersistenceModel apm = new AppPersistenceModel();
                apm.setEncrypting(isEncrypting);
                apm.populateEzPersist(clientRows);
                apm.setAuthorizationLevel("U");
                
                if(SecurityID.ReservedSecurityId.isReserved(apm.getId())) {
                    SecurityID.ReservedSecurityId id = SecurityID.ReservedSecurityId.fromEither(apm.getId());
                    switch (id) {
                        case EzSecurity:
                            // Save EzSecurity public key on the instance
                            ezsec_pub = apm.getPublicKey();
                            break;
                        case Registration:
                            regAppModel = apm;
                            break;
                    }
                }

                logger.info("Client certificate found, ID: {}, Name: {}", apm.getId(), apm.getAppName());
                apps.add(apm);
            } catch (EzPKeyError ezPKeyError) {
                logger.warn("Certificate not found for client {}. Not bootstrapping it. {}", client,
                        ezPKeyError.getMessage());
            } catch (AppPersistCryptoException e) {
                logger.warn("Failed to encrypt app keys {}", client, e);
            }
        }
        
        if(regAppModel == null) {
            throw new RuntimeException("Did not find a registration app model");
        }
        try {
            if(isEncrypting) {
                String passcode = getPassPhrase();
            
                for(AppPersistenceModel app : apps) {
                    app.setPasscode(passcode);
                    app.encryptKeyIfNotDoneSo();
                }
            }
        } catch (NoSuchAlgorithmException | InvalidKeySpecException
                | AppPersistCryptoException | PKeyCryptoException e2) {
            logger.warn("Failed encrypting keys", e2);
            return 1;
        }
        
      
        // Push certificates to Accumulo
        if (!parameters.dryRun) {
            try {
                AccumuloHelper helper = new AccumuloHelper(ezConfiguration);
                Connector connector = helper.getConnector(false);

                if (!connector.tableOperations().exists(AccumuloRegistrationManager.REG_TABLE)) {
                    logger.info("Creating Registrations table {}", AccumuloRegistrationManager.REG_TABLE);
                    connector.tableOperations().create(AccumuloRegistrationManager.REG_TABLE);
                }
                if (!connector.tableOperations().exists(AccumuloRegistrationManager.LOOKUP_TABLE)) {
                    logger.info("Creating Registrations table {}", AccumuloRegistrationManager.LOOKUP_TABLE);
                    connector.tableOperations().create(AccumuloRegistrationManager.LOOKUP_TABLE);
                }
                MultiTableBatchWriter bw = connector.createMultiTableBatchWriter(1000000L, 1000L, 10);

                // Write the certs
                List<String> skipRows = new ArrayList<>();
                for (AppPersistenceModel apm : apps) {
                    logger.info("Bootstrapping application certificate for {}({})", apm.getId(), apm.getAppName());
                    String id = apm.getId();

                    // Make sure the things we bootstrap are within the reserved block of Security IDs
                    if (!SecurityID.isReserved(id)) {
                        logger.error("Not bootstrapping application {}. Not a reserved Security ID (possible collisions)");
                        continue;
                    }

                    if (SecurityID.ReservedSecurityId.isReserved(id)) {
                        SecurityID.ReservedSecurityId sid = SecurityID.ReservedSecurityId.fromEither(id);
                        switch (sid) {
                            case CA:
                                // Don't upload the CA private key
                                skipRows.add("private_key");
                                break;
                            case EzSecurity:
                                break;
                            case EFE:
                                break;
                            case Registration:
                                break;
                        }
                    }

                    // Initialize the public key if private key was there
                    if ((apm.getPublicKey() == null || apm.getPublicKey().isEmpty()) && apm.getPrivateKey() != null) {
                        logger.debug("No public key available - generating from private");
                        try {
                            logger.debug("Attempting to get public key from private key {}", apm.getPrivateKey());
                            apm.setPublicKey(RSAKeyCrypto.getPublicFromPrivatePEM(apm.getPrivateKey()));
                        } catch (DecoderException e) {
                            logger.warn("Unable to discern public key value - invalid private key", e);
                        }
                    }

                    // force all applications into active status
                    apm.setStatus(RegistrationStatus.ACTIVE);
                    bw.getBatchWriter(AccumuloRegistrationManager.REG_TABLE).addMutations(
                            mutationsFromRowMap(apm.ezPersistRows(), id, skipRows));
                    bw.getBatchWriter(AccumuloRegistrationManager.LOOKUP_TABLE).addMutations(apm.getLookupMutations());

                    // Clear out skip rows
                    skipRows.clear();
                }
                logger.info("Flushing updates and closing batch writer");
                bw.close();
            } catch (TableNotFoundException e) {
                logger.error("Table: {} not found", AccumuloRegistrationManager.REG_TABLE);
                return 1;
            } catch (MutationsRejectedException e) {
                logger.error("Invalid mutations");
                return 1;
            } catch (DataLengthException | AppPersistCryptoException e) {
                logger.error("Failed encrypting keys: {}", e.getMessage());
                return 1;
            } catch (AccumuloSecurityException | TableExistsException | AccumuloException | IllegalStateException e) {
                logger.error(e.getMessage());
                return 1;
            } catch (IOException e) {
                logger.error("Failed connecting to Accumulo");
                throw new RuntimeException(e);
            }
        }

        if (parameters.outDir != null) {
            try {
                ensureDirectory(parameters.outDir);
                // Write Tarballs for each certificate
                for (AppPersistenceModel apm : apps) {
                    AppCerts certs = certsForApp(apm, ca_cert, ca_jks, ezsec_pub);

                    if (SecurityID.ReservedSecurityId.isReserved(apm.getId())) {
                        SecurityID.ReservedSecurityId sid = SecurityID.ReservedSecurityId.fromEither(apm.getId());
                        switch (sid) {
                            case CA:
                                // Don't write a tar for CA, instead write the CA certificate
                                createAndWriteText(
                                        AppCerts._Fields.EZBAKECA_CRT.getFieldName().replaceAll("_", "."),
                                        parameters.outDir,
                                        certs.getApplication_crt());
                                continue;
                            case EzSecurity:
                            case EFE:
                            case Deployer:
                            case Registration:
                        }
                    }

                    createAndWriteTarball(apm.getAppName(), certs, parameters.outDir);
                }
            } catch (IOException e) {
                logger.warn("Unable to create output directory {}", parameters.outDir, e);
            }
        }

        this.apps = apps;
        
        return 0;
    }

    public static List<Mutation> mutationsFromRowMap(Map<String, Object> rows, String overrideId, List<String> skipCols) {
        List<Mutation> mutations = new ArrayList<Mutation>();

        for (Map.Entry row : rows.entrySet()) {
            String[] keyParts;
            if(row.getKey() instanceof String) {
                keyParts = EzPersist.keyParts((String)row.getKey());
            } else if(row.getKey() instanceof byte[]) {
                keyParts = EzPersist.keyParts(new String((byte[])row.getKey()));
            } else {
                logger.debug("Invalid key type: {}", row.getKey().getClass().getSimpleName());
                continue;
            }
            
            if (keyParts.length < 2) {
                logger.debug("Invalid key: not enough elements, {}", keyParts.length);
                continue;
            }
            if (skipCols != null && skipCols.contains(keyParts[1])) {
                logger.info("Skipping bootstrap of {} for app {}", skipCols, keyParts[0]);
                continue;
            }

            String rowId = (overrideId != null)? overrideId : keyParts[0];
            String colf = keyParts[1];

            logger.debug("Adding mutation for {}:{}", rowId, colf);
            Mutation m = new Mutation(rowId);
            if(row.getValue() instanceof String) {
                m.put(colf, "", (String)row.getValue());
            } else if(row.getValue() instanceof byte[]) {
                m.put(colf.getBytes(), "".getBytes(), (byte[])row.getValue());
            }
            mutations.add(m);
        }
        return mutations;
    }

    protected static AppCerts certsForApp(AppPersistenceModel apm, String caCert, byte[] caJks, String ezsecPubKey) {
        AppCerts appCerts = new AppCerts();
        boolean hasPrivate = false;
        boolean hasCert = false;

        // Add application's actual certificates
        if (apm.getPublicKey() != null && !apm.getPublicKey().isEmpty()) {
            appCerts.setApplication_pub(apm.getPublicKey().getBytes());
        }
        try {
            if (apm.getPrivateKey() != null && !apm.getPrivateKey().isEmpty()) {
                try {
                    appCerts.setApplication_priv(apm.getPrivateKey().getBytes());
                } catch (AppPersistCryptoException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                hasPrivate = true;
            }
        } catch (AppPersistCryptoException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (apm.getX509Cert() != null && !apm.getX509Cert().isEmpty()) {
            appCerts.setApplication_crt(apm.getX509Cert().getBytes());
            hasCert = true;
        }
        if (hasPrivate && hasCert && caCert != null) {
            try {
                appCerts.setApplication_p12(CryptoUtil.load_p12(caCert, apm.getX509Cert(), apm.getPrivateKey()));
            } catch (AppPersistCryptoException | IOException e) {
                e.printStackTrace();
            }
        }

        // Add CA certificates
        if (caCert != null) {
            appCerts.setEzbakeca_crt(caCert.getBytes());
        }
        if (caJks != null) {
            appCerts.setEzbakeca_jks(caJks);
        }

        // Add EzSecurity public key
        if (ezsecPubKey != null) {
            appCerts.setEzbakesecurityservice_pub(ezsecPubKey.getBytes());
        }

        return appCerts;
    }

    protected static void addTarArchiveEntry(final TarArchiveOutputStream tao, String name, byte[] data) {
        TarArchiveEntry tae = new TarArchiveEntry(name);
        try {
            tae.setSize(data.length);
            tao.putArchiveEntry(tae);
            tao.write(data);
            tao.closeArchiveEntry();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createAndWriteText(String name, String filePath, byte[] text) {
        OutputStream fof = null;
         try {
             File outputFile = new File(filePath, name);
             outputFile.createNewFile();
             outputFile.setWritable(false, false);
             outputFile.setWritable(true, true);
             outputFile.setReadable(false, false);
             outputFile.setReadable(true, true);
             fof = new FileOutputStream(outputFile);
             fof.write(text);
         } catch (IOException e) {
             logger.error("Error creating output file", e);
         } finally {
             if (fof != null) {
                 try {
                     fof.close();
                 } catch (IOException e) {
                     logger.warn("Unable to close output stream", e);
                 }
             }
         }

    }

    public static void createAndWriteTarball(String name, AppCerts certs, String filePath) {
        TarArchiveOutputStream os = null;
        try {
            File outputFile = new File(filePath, name + ".tar.gz");
            outputFile.createNewFile();
            outputFile.setWritable(false, false);
            outputFile.setWritable(true, true);
            outputFile.setReadable(false, false);
            outputFile.setReadable(true, true);
            FileOutputStream fos = new FileOutputStream(outputFile);

            CompressorOutputStream cos = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, fos);
            os = new TarArchiveOutputStream(cos);

            // For each field in the app certs, create an entry in the tar archive
            for (AppCerts._Fields field : AppCerts._Fields.values()) {
                Object o = certs.getFieldValue(field);
                if (o instanceof byte[]) {
                    String fieldName = field.getFieldName().replace("_", ".");
                    addTarArchiveEntry(os, fieldName, (byte[]) o);
                }
            }

        } catch (FileNotFoundException e) {
            logger.error("Unable to write tarball", e);
        } catch (CompressorException e) {
            logger.error("Error compressing tarball", e);
        } catch (IOException e) {
            logger.error("Error creating output file for tarball", e);
        } finally {
            if (os != null) {
                try {
                    os.finish();
                    os.close();
                } catch (IOException e) {
                    logger.warn("Unable to close output stream", e);
                }
            }
        }
    }

    public void ensureDirectory(String path) throws IOException {
        File directory = new File(path);

        if (directory.exists() && !directory.isDirectory()) {
            throw new IOException("Unable to create directory. " + path + " already exists and is not a directory");
        }

        if (!directory.mkdirs() && !directory.isDirectory()) {
            throw new IOException("Unable to make directories, mkdirs returned false");
        }

        if (!directory.setExecutable(false, false) || !directory.setExecutable(true, true)) {
            throw new IOException("Created directory, but unable to set executable");
        }
        if (!directory.setReadable(false, false) || !directory.setReadable(true, true)) {
            throw new IOException("Created directory, but unable to set readable");
        }
        if (!directory.setWritable(false, false) || !directory.setWritable(true, true)) {
            throw new IOException("Created directory, but unable to set writable");
        }
    }

    /* Generate Appropriate Passphrase used by all ApplicationModels */
    public String getPassPhrase() throws AppPersistCryptoException, NoSuchAlgorithmException, InvalidKeySpecException, PKeyCryptoException {
        String passcode = "";
        
        String pk = regAppModel.getPrivateKey();
        RSAKeyCrypto crypto = new RSAKeyCrypto(pk, true);
        
        byte[] cipherData = crypto.sign(SecurityID.ReservedSecurityId.Registration.getCn().getBytes()); 
        passcode = new String(Base64.encode(cipherData));
        logger.debug("Generate The Passcode {}", passcode);
        return passcode;
    }
    
    public List<AppPersistenceModel> getAppModels() {
        return this.apps;
    }
    
    public static void main(String[] args) throws DataLengthException, IllegalStateException, InvalidCipherTextException, EzConfigurationLoaderException {
        EzCABootstrap bootstrap = new EzCABootstrap(args);
        if (bootstrap.parameters.help) {
            bootstrap.parameters.printUsage();
            System.exit(0);
        }
        bootstrap.run();
    }
    
    
}
