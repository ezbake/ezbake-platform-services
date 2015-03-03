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
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbakehelpers.accumulo.AccumuloHelper;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 10/9/13
 * Time: 10:26 AM
 *
 * updated gdrocella 03/25/14
 */
public class AccumuloRegistrationManager implements RegistrationManager {
    private static Logger log = LoggerFactory.getLogger(AccumuloRegistrationManager.class);
    public static final String LOOKUP_TABLE = "ezsecurity_lookup";
    public static final String REG_TABLE = "ezsecurity_reg";

    private Properties configuration;
    private String visibilityToken;
    private String[] scanAuthorizations;

    @Inject
    public AccumuloRegistrationManager(Properties ezConfiguration) throws AccumuloException, AccumuloSecurityException {
        this(ezConfiguration, REG_TABLE);
    }
    public AccumuloRegistrationManager(Properties ezConfiguration, String table) throws AccumuloException, AccumuloSecurityException {
        this.configuration = ezConfiguration;
        this.scanAuthorizations = ezConfiguration.getProperty("ezsecurity.auths", "U").split(",");

        authParser(this.scanAuthorizations);
        try {
            Connector conn = new AccumuloHelper(configuration).getConnector();
            if (!conn.tableOperations().exists(REG_TABLE)) {
                try {
                    log.info("Table {} did not exist. Creating it", REG_TABLE);
                    conn.tableOperations().create(REG_TABLE);
                } catch (TableExistsException e) {
                    log.info("Table exists exception creating {}, just ignoring", REG_TABLE);
                }
            }
            if (!conn.tableOperations().exists(LOOKUP_TABLE)) {
                try {
                    log.info("Table {} did not exist. Creating it", LOOKUP_TABLE);
                    conn.tableOperations().create(LOOKUP_TABLE);
                } catch (TableExistsException e) {
                    log.info("Table exists exception creating {}, just ignoring", LOOKUP_TABLE);
                }
            }
        } catch (IOException e) {
            log.error("Unexpected IOException checking/creating Accumulo tables", e);
        }
    }

    public void authParser(String[] auths) {
        StringBuilder visBuilder = new StringBuilder();
        visBuilder.append("(");
        for (int i = 0, length = auths.length; i < length; ++i) {
            visBuilder.append(auths[i]);
            if (i < length-1) {
                visBuilder.append("&");
            }
        }
        visBuilder.append(")");
        visibilityToken = visBuilder.toString();
        log.info("Visibility Token: " + visibilityToken);
    }

    private static void validateNewSecurityId(String id) throws RegistrationException {
        if (SecurityID.isReserved(id)) {
            log.warn("Received request to register with reserved security ID: {}", id);
            throw new RegistrationException("Requested security ID is reserved: " + id);
        }
    }

    /**
     * Register a client in the registration database
     *
     * @param id the unique ID for this record
     * @param owner the user who owns the record
     * @param appName pretty name for the application
     * @param visibilityLevel visibility level of the application
     * @param visibility visibility assigned to the application
     * @throws RegistrationException
     */
    @Override
    public void register(String id, String owner, String appName, String visibilityLevel, List<String> visibility, Set<String> admins, String appDn) throws RegistrationException {
        log.info("Registering application {} for {}", id, owner);
        validateNewSecurityId(id);

        AppPersistenceModel app = new AppPersistenceModel();
        app.setId(id);
        app.setAppName(appName);
        app.setOwner(owner);
        app.setAuthorizationLevel(visibilityLevel);
        app.setFormalAuthorizations(visibility);
        app.setStatus(RegistrationStatus.PENDING);
        app.setAdmins(admins);
        app.setAppDn(appDn);

        register(app);
    }

    @Override
    public void register(AppPersistenceModel appPersistenceModel) throws RegistrationException {
        appPersistenceModel.setStatus(RegistrationStatus.PENDING);
        writeRegistration(appPersistenceModel);
    }

    /**
     * Set the application status to active
     *
     * @param auths the scan authorizations for the request
     * @param id the unique ID for the record to be promoted
     * @throws SecurityIDNotFoundException
     */
    @Override
    public void approve(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        setStatus(id, RegistrationStatus.ACTIVE);
    }

    @Override
    public void deny(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        log.debug("deny {}", id);
        setStatus(id, RegistrationStatus.DENIED);
    }

    @Override
    public void delete(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        log.debug("deleting application id: {}", id);

        AppPersistenceModel apm = getRegistration(auths, id, null, null);

        apm.getLookupDeleteMutations();

        try {
            Connector connector = new AccumuloHelper(configuration).getConnector(false);
            MultiTableBatchWriter deleter = connector.createMultiTableBatchWriter(getDefaultBatchWriterConfig());

            deleter.getBatchWriter(LOOKUP_TABLE).addMutations(apm.getLookupDeleteMutations());
            deleter.getBatchWriter(REG_TABLE).addMutations(apm.getObjectDeleteMutations());

            deleter.close();
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
            // Ignored - should not happen
        } catch (IOException e) {
            log.error("Failed to delete application - unable to connect: {}", e.getMessage());
            throw new RegistrationException("Unable to delete registration: "+id + " - " + e.getMessage());
        }

    }

    /**
     * Update the values in a registration, and move it to pending status
     *
     * @param registration the new registration object
     * @param status status to change to
     * @throws SecurityIDNotFoundException
     */
    @Override
    public void update(AppPersistenceModel registration, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        log.info("Updating {}", registration.getId());
        if (status != null) {
            setStatus(registration.getId(), status);
            registration.setStatus(status);
        }
        if (!containsId(scanAuthorizations, registration.getId())) {
            throw new SecurityIDNotFoundException("Security ID: " + registration.getId() + " not found");
        }
        writeRegistration(registration);
    }

    @Override
    public void setStatus(String id, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            Scanner statLookup = connector.createScanner(REG_TABLE, new Authorizations(scanAuthorizations));
            AppPersistenceModel.setScanOptions(statLookup, id, true, true, true);

            AppPersistenceModel model;
            Iterator<Map.Entry<Key, Value>> iterator = statLookup.iterator();
            if (!iterator.hasNext()) {
                throw new SecurityIDNotFoundException("Security ID: " + id + " was not found");
            } else {
                List<Map.Entry<Key, Value>> rows = new ArrayList<Map.Entry<Key, Value>>();
                while (iterator.hasNext()) {
                    rows.add(iterator.next());
                }
                model = AppPersistenceModel.fromRowsNoPrivate(rows);
            }

            MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(1000000L, 1000L, 10);
            writer.getBatchWriter(REG_TABLE).addMutation(AppPersistenceModel.getMutation(id,
                    AppPersistenceModel.APP_REG_FIELDS.STATUS.getValue(), "", status.toString()));
            writer.getBatchWriter(LOOKUP_TABLE).addMutations(model.getLookupDeleteMutations());
            model.setStatus(status);
            writer.getBatchWriter(LOOKUP_TABLE).addMutations(model.getLookupMutations());
            writer.close();
        } catch (TableNotFoundException e) {
            log.error("Failed to set application status - table not found: {}", e.getMessage());
            throw new RegistrationException("Unable to set status: "+e.getMessage());
        } catch (IOException|AccumuloException|AccumuloSecurityException e) {
            log.error("Failed to set application status - accumulo error: {}", e.getMessage());
            throw new RegistrationException("Unable to set status: "+e.getMessage());
        }
    }

    @Override
    public RegistrationStatus getStatus(String[] auths, String id) throws SecurityIDNotFoundException {
        RegistrationStatus status;
        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            Scanner scanner = connector.createScanner(REG_TABLE, new Authorizations(scanAuthorizations));
            AppPersistenceModel.setScanOptions(scanner, id, true);

            Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
            if (i.hasNext()) {
                Map.Entry<Key, Value> entry = i.next();
                status = RegistrationStatus.valueOf(entry.getValue().toString());
            } else {
                throw new SecurityIDNotFoundException("Security id: " + id + " was not found");
            }
        } catch (IOException e) {
            log.error("Failed to get application status - unable to connect: {}", e.getMessage());
            throw new SecurityIDNotFoundException("Unable to get status: "+e.getMessage());
        } catch (TableNotFoundException e) {
            log.error("Failed to get application status - table not found: {}", e.getMessage());
            throw new SecurityIDNotFoundException("Unable to get status: "+e.getMessage());
        }
        return status;
    }

    @Override
    public AppPersistenceModel getRegistration(String[] auths, String id, String owner, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        AppPersistenceModel registration;



        List<Map.Entry<Key, Value>> rows = getApplicationRow(auths, id, owner, status);
        try {
            registration = AppPersistenceModel.fromRows(rows);
        } catch (AppPersistCryptoException e) {
            log.error("Failed decrypting private key {}", e.getMessage());
            throw new RegistrationException("Unable to decrypt app private key");
        }

        return registration;
    }

    /**
     * Looks up the registration by primary key and returns true if it exists
     *
     * @param auths accumulo scan auths
     * @param id registration id
     * @return true if the registration exists
     * @throws RegistrationException
     */
    @Override
    public boolean containsId(String[] auths, String id) throws RegistrationException {
        boolean contains;
        try {
            contains = getRegistration(auths,id, null, null) != null;
        }
        catch(SecurityIDNotFoundException e) {
            contains = false;
        }

        return contains;
    }

    @Override
    public List<AppPersistenceModel> all(String[] auths,
                                                      String owner,
                                                      RegistrationStatus status) throws RegistrationException {
        List<AppPersistenceModel> registrations = new ArrayList<AppPersistenceModel>();

        Map<Text, List<Map.Entry<Key, Value>>> rows = new HashMap<Text, List<Map.Entry<Key, Value>>>();
        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            ScannerBase scanIter;

            List<Range> idsToScanFor = new ArrayList<Range>();
            if (owner != null && !owner.isEmpty()) {
                // Lookup the users rows first
                Scanner scanner = connector.createScanner(LOOKUP_TABLE, new Authorizations(auths));
                scanner.setRange(new Range(owner));
                if (status != null) {
                    scanner.fetchColumnFamily(new Text(status.toString()));
                }
                for (Map.Entry<Key, Value> aScanner : scanner) {
                    String id = aScanner.getKey().getColumnQualifier().toString();
                    log.debug("Adding id to all query: {}", id);
                    idsToScanFor.add(new Range(new Text(id)));
                }
                scanner.close();
                // return empty list if there are no ids to scan for
                if (idsToScanFor.isEmpty()) {
                    return Collections.emptyList();
                }
                BatchScanner bs = connector.createBatchScanner(REG_TABLE, new Authorizations(auths), 4);
                bs.setRanges(idsToScanFor);
                scanIter = bs;
            } else if (status != null) {
                Scanner scanner = connector.createScanner(LOOKUP_TABLE, new Authorizations(auths));
                scanner.setRange(new Range(status.toString()));
                for (Map.Entry<Key, Value> entry : scanner) {
                    log.debug("Adding id to status query: {}", entry.getKey().getColumnFamily());
                    idsToScanFor.add(new Range(entry.getKey().getColumnFamily()));
                }
                scanner.close();
                // return empty list if there are no ids to scan for
                if (idsToScanFor.isEmpty()) {
                    return Collections.emptyList();
                }
                BatchScanner bs = connector.createBatchScanner(REG_TABLE, new Authorizations(auths), 4);
                bs.setRanges(idsToScanFor);
                scanIter = bs;
            } else {
                scanIter = connector.createScanner(REG_TABLE, new Authorizations(auths));
            }

            // run over the scanner, collecting the rows by ID
            for (Map.Entry<Key, Value> entry : scanIter) {
                // Don't return reserved certs
                if (SecurityID.ReservedSecurityId.isReserved(entry.getKey().getRow().toString())) {
                    continue;
                }
                List<Map.Entry<Key, Value>> row = rows.get(entry.getKey().getRow());
                if (row == null) {
                    row = new ArrayList<Map.Entry<Key, Value>>();
                }
                row.add(entry);
                rows.put(entry.getKey().getRow(), row);
            }
            scanIter.close();

            // now that the rows are collected by ID, build the objects
            for (List<Map.Entry<Key, Value>> entry : rows.values()) {
                try {
                    registrations.add(AppPersistenceModel.fromRows(entry));
                } catch (AppPersistCryptoException e) {
                    log.error("Failed decrypting private key {}", e.getMessage());
                }
            }
        } catch (TableNotFoundException e) {
            log.error("Error: Accumulo Misconfigured - table is not found " + e);
            throw new RegistrationException("Error: Accumulo Misconfigured - table is not found " + e);
        } catch (IOException e) {
            log.error("Error: IOException " + e);
            throw new RegistrationException("Error: IOException " + e);
        }

        return registrations;
    }

    protected List<Map.Entry<Key, Value>> getApplicationRow(String[] auths, String id, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        return getApplicationRow(auths, id, null, status);
    }
    protected List<Map.Entry<Key, Value>> getApplicationRow(String[] auths, String id, String owner, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        List<Map.Entry<Key, Value>> row = new ArrayList<Map.Entry<Key, Value>>();

        try {
            Connector connector = new AccumuloHelper(configuration).getConnector(false);

            if (status != null || owner != null) {
                BatchScanner lookScanner = connector.createBatchScanner(LOOKUP_TABLE, new Authorizations(auths), 4);
                List<Range> ranges = Lists.newArrayList();
                if (status != null) {
                    ranges.add(new Range(status.toString()));
                    lookScanner.fetchColumnFamily(new Text(id));
                }
                if (owner != null) {
                    ranges.add(new Range(owner));
                    lookScanner.fetchColumnFamily(new Text(id));
                }
                lookScanner.setRanges(ranges);

                boolean statusFound = status == null;
                boolean ownerFound = owner == null;
                for (Map.Entry<Key, Value> lookup : lookScanner) {
                    if (status != null) {
                        if (lookup.getKey().getRow().toString().equals(status.toString())) {
                            statusFound = true;
                        }
                    }
                    if (owner != null) {
                        if (lookup.getKey().getRow().toString().equals(owner)) {
                            ownerFound = true;
                        }
                    }
                }
                lookScanner.close();
                if (!statusFound || !ownerFound) {
                    throw new SecurityIDNotFoundException("No application found with security ID: " + id + ", owner: " +
                            owner + ", and status: " + status);
                }
            }

            Scanner scanner = connector.createScanner(REG_TABLE, new Authorizations(auths));
            scanner.setRange(new Range(id));

            log.debug("Get Application Row {} with auths {}", id, auths);
            // Take the first row from the scanner (should be an exact match)
            for (Map.Entry<Key, Value> aScanner : scanner) {
                row.add(aScanner);
            }
            scanner.close();
        } catch (TableNotFoundException e) {
            log.error("Error: Accumulo is Misconfigured - table is not found", e);
            throw new RegistrationException("Error: Accumulo is Misconfigured - table is not found" + e);
        } catch (IOException e) {
            log.error("Error: " + e);
            throw new RegistrationException("Error: " + e);
        }

        if(row.isEmpty()) {
            throw new SecurityIDNotFoundException("Security ID [" + id + "] Does not exist in the Accumulo Database.");
        }

        return row;
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
            log.error("Failed encrypting private key {}", e.getMessage());
        }
        return status;
    }

    protected void writeRow(Map.Entry<Key, Value> row, Text value) throws RegistrationException {
        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            BatchWriter writer = connector.createBatchWriter(REG_TABLE, 1000000L, 1000L, 10);
            Mutation m = new Mutation(row.getKey().getRow());
            m.put(row.getKey().getColumnFamily(), row.getKey().getColumnQualifier(), new ColumnVisibility(visibilityToken), new Value(value.getBytes()));
            writer.addMutation(m);
            writer.close();
        } catch (IOException e) {
            throw new RegistrationException("Error: IOException " + e);
        } catch (TableNotFoundException e) {
            throw new RegistrationException("Error: Accumulo Misconfigured - table is not found " + e);
        } catch (MutationsRejectedException e) {
            throw new RegistrationException("Error: Mutation Rejected " + e);
        }
    }
    
    /**
     * 
     * @param auths
     * @param id
     * @param admin
     * @throws TException
     * @throws SecurityIDNotFoundException
     * @throws RegistrationException
     */
    @Override
    public void addAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException {
    	log.info("add admin {}, {}, {}", auths, id, admin);

        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            Scanner scanner = connector.createScanner(REG_TABLE, new Authorizations(scanAuthorizations));
            AppPersistenceModel.setScanOptions(scanner, id, true, false, true);

            // Get current admins
            Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
            if (!i.hasNext()) {
                throw new SecurityIDNotFoundException("Security ID: " + id + " not found");
            }
            scanner.close();

            List<Map.Entry<Key, Value>> rows = new ArrayList<Map.Entry<Key, Value>>();
            while (i.hasNext()) {
                rows.add(i.next());
            }

            // Model with only the admins and status
            AppPersistenceModel model = AppPersistenceModel.fromRows(rows);
            model.getAdmins().add(admin);

            MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(1000000L, 1000L, 10);
            writer.getBatchWriter(REG_TABLE).addMutations(model.getObjectMutations());
            writer.getBatchWriter(LOOKUP_TABLE).addMutations(model.getLookupMutations());
            writer.close();
        } catch (IOException | TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new RegistrationException(e.getMessage());
        } catch (AppPersistCryptoException e) {
            log.error("Failed encrypting private key {}", e.getMessage());
        }
    }


    @Override
    public void removeAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException {
        log.info("remove admin {}, {}, {}", auths, id, admin);

        try {
            Connector connector = new AccumuloHelper(configuration).getConnector();
            Scanner scanner = connector.createScanner(REG_TABLE, new Authorizations(scanAuthorizations));
            AppPersistenceModel.setScanOptions(scanner, id, true, false, true);

            // Get current admins
            Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
            if (!i.hasNext()) {
                throw new SecurityIDNotFoundException("Security ID: " + id + " not found");
            }
            scanner.close();

            List<Map.Entry<Key, Value>> rows = new ArrayList<Map.Entry<Key, Value>>();
            while (i.hasNext()) {
                rows.add(i.next());
            }

            // Model with only the admins
            AppPersistenceModel model = AppPersistenceModel.fromRows(rows);
            model.getAdmins().remove(admin);

            MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(1000000L, 1000L, 10);
            writer.getBatchWriter(REG_TABLE).addMutations(model.getObjectMutations());

            // Clear all admins, and add back just the removed one for the delete mutations
            model.getAdmins().clear();
            model.getAdmins().add(admin);
            writer.getBatchWriter(LOOKUP_TABLE).addMutations(model.getLookupDeleteMutations());

            writer.close();
        } catch (IOException | TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            throw new RegistrationException(e.getMessage());
        } catch (AppPersistCryptoException e) {
            log.error("Failed decrypting private key {}", e.getMessage());
        }
    }


    private static BatchWriterConfig getDefaultBatchWriterConfig() {
        return new BatchWriterConfig()
                .setMaxMemory(1000000L)
                .setMaxLatency(1000, TimeUnit.MILLISECONDS)
                .setMaxWriteThreads(10)
                .setTimeout(60, TimeUnit.SECONDS);
    }
}

