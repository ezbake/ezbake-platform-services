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

package ezbake.services.provenance.thrift;

import com.google.common.collect.Sets;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.common.properties.EzProperties;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityConstant;
import ezbake.security.thrift.RegistrationException;
import ezbake.services.centralPurge.thrift.ezCentralPurgeServiceConstants;
import ezbake.services.provenance.graph.GraphDb;
import ezbake.services.provenance.graph.Utils;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.thrift.ThriftClientPool;
import org.apache.accumulo.core.data.*;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ezbake.services.provenance.graph.Utils.convertDateTime2Millis;
import static ezbake.util.AuditEvent.event;

public class ProvenanceServiceImpl extends EzBakeBaseThriftService implements ProvenanceService.Iface {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceServiceImpl.class);
    private static final AuditLogger auditLogger = new AuditLogger(ProvenanceServiceImpl.class);
    public static final String TABLE = "ezprovenance_object_access";
    public static final String ID_GENERATOR_KEY = "provenance.id.generator.key";
    public static final String ADDDOCUMENT_MAXSIZE_KEY = "provenance.adddocuments.maxsize";
    private static String purgeAppName;

    private GraphDb graphDb;
    private Properties ezProperties;
    private EzbakeSecurityClient ezbakeSecurityClient;
    private Set<Long> auditGroups;
    private Random randomGenerator;
    private int addDocumentsMaxSize;

    @Override
    public TProcessor getThriftProcessor() {
        try {
            logger.debug("Starting getThriftProcessor");
            init();
            return new ProvenanceService.Processor(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connector getAccumuloConnector() throws IOException {
        AccumuloHelper helper = new AccumuloHelper(this.ezProperties);
        return helper.getConnector(StringUtils.isNotEmpty(helper.getAccumuloNamespace()));
    }

    private void init() throws EzSecurityTokenException, Exception {
        this.ezProperties = getConfigurationProperties();
        this.ezbakeSecurityClient = new EzbakeSecurityClient(this.ezProperties);
        this.randomGenerator = new Random();
        ThriftClientPool pool = null;

        // initialize adddocument maxsize
        EzProperties properties = new EzProperties(this.ezProperties, false);
        this.addDocumentsMaxSize = properties.getInteger(ADDDOCUMENT_MAXSIZE_KEY, 500);

        EzSecurityToken provenanceServiceToken = this.ezbakeSecurityClient.fetchAppToken();
        AuditEvent evt = event(AuditEventType.ApplicationInitialization.getName(), provenanceServiceToken)
                .arg("event", "init");

        try {
            // initialize graph database
            this.graphDb = new GraphDb(this.ezProperties);

            // create ezprovenance_object_access table if not exist
            Connector conn = getAccumuloConnector();
            if (!conn.tableOperations().exists(TABLE)) {
                try {
                    logger.info("Table {} did not exist. Creating it", TABLE);
                    conn.tableOperations().create(TABLE);
                } catch (TableExistsException e) {
                    logger.info("Table exists exception creating {}, just ignoring", TABLE);
                } catch (AccumuloSecurityException e) {
                    logger.error("Unexpected AccumuloSecurityException checking/creating Accumulo tables", e);
                } catch (AccumuloException e) {
                    logger.error("Unexpected ACCUMULOException checking/creating Accumulo tables", e);
                }
            }

            // get auditgroups from ezgroups service
            pool = new ThriftClientPool(this.ezProperties);
            Set<String> appGroupName = new HashSet<>();
            appGroupName.add(EzGroupsConstants.APP_GROUP + "." + ProvenanceServiceConstants.SERVICE_NAME);
            String targetId = pool.getSecurityId(EzGroupsConstants.SERVICE_NAME);
            ezbake.security.client.EzSecurityTokenWrapper wrapper = this.ezbakeSecurityClient.fetchAppToken(targetId);
            EzGroups.Client client = pool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
            this.auditGroups = client.getGroupsMask(wrapper, appGroupName);

        } catch (IOException e) {
            logger.error("Unexpected IOException checking/creating Accumulo tables", e);
            evt.failed();
            evt.arg(e.getClass().getName(), e);
        } catch (TException e) {
            logger.error("Unexpected TException getting EzProvenanceService group", e);
            evt.failed();
            evt.arg(e.getClass().getName(), e);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
            if (pool != null) {
                pool.close();
            }
        }
    }

    @Override
    public void shutdown() {
        logger.info("shutting down provenance service...");

        if (this.graphDb != null) {
            this.graphDb.shutdown();
        }

        try {
            this.ezbakeSecurityClient.close();
        } catch (final IOException e) {
            logger.error("exception to close security client: ", e);
        }

        super.shutdown();
    }

    @Override
    public boolean ping() {
        //should return true if the procedure is in a state that it can be used
        return this.graphDb != null;
    }

    // get the purge app name
    private String getPurgeAppName() {
        if (purgeAppName == null) {
            purgeAppName = this.ezbakeSecurityClient.getRegisteredSecurityId(ezCentralPurgeServiceConstants.SERVICE_NAME);
        }
        return StringUtils.isEmpty(purgeAppName) ? ezCentralPurgeServiceConstants.SERVICE_NAME : purgeAppName;
    }

    private void validateSecurityToken(EzSecurityToken token) throws EzSecurityTokenException {
        this.ezbakeSecurityClient.validateReceivedToken(token);
    }

    private void validateAdminSecurityToken(EzSecurityToken token) throws EzSecurityTokenException {
        this.ezbakeSecurityClient.validateReceivedToken(token);

        if (!Utils.isAdminApplication(token, this.getPurgeAppName())) {
            logger.error(String.format("Not admin application. securityId = %s, expected securityId = %s", Utils.getApplication(token), getPurgeAppName()));
            throw new EzSecurityTokenException("Not admin application");
        }
    }

    private void validateDbStatus() throws TException {
        if (this.graphDb == null) {
            throw new TException("graph db not initialized");
        }
    }

    /**
     * Allows apps / users to add new age off rules to the system.
     *
     * @param securityToken
     * @param name                     The unique name for AgeOffRule
     * @param retentionDurationSeconds
     * @return the i64 id of the new AgeOffRule vertex
     * @throws ProvenanceAgeOffRuleNameExistsException         when the name already exists
     * @throws ProvenanceIllegalAgeOffDurationSecondsException when the retentionDurationSeconds is set to 0
     * @throws ProvenanceIllegalAgeOffRuleNameException        when the name is an empty string
     * @throws org.apache.thrift.TException
     * @throwsezbake.base.thrift.EzSecurityTokenException when the EzSecurityToken is not valid
     */
    @Override
    public long addAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, String name, long retentionDurationSeconds, int maximumExecutionPeriod) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNameExistsException, ProvenanceIllegalAgeOffDurationSecondsException, ProvenanceIllegalAgeOffRuleNameException, org.apache.thrift.TException {

        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), securityToken)
                .arg("event", "addAgeOffRule")
                .arg("name of age off rule", name);
        try {
            validateSecurityToken(securityToken);

            if (name == null || name.isEmpty()) {
                throw new ProvenanceIllegalAgeOffRuleNameException("The name of the age off rule cannot be empty");
            }

            if (retentionDurationSeconds <= 0) {
                throw new ProvenanceIllegalAgeOffDurationSecondsException("The duration seconds must be greater than 0");
            }

            if (maximumExecutionPeriod < 1 || maximumExecutionPeriod > 90) {
                throw new ProvenanceIllegalMaximumExecutionPeriodException("The maximumExecutionPeriod must be between 1 and 90 days");
            }
            validateDbStatus();
            return this.graphDb.addAgeOffRule(securityToken, name, retentionDurationSeconds, maximumExecutionPeriod);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public AgeOffRule getAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, String name) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getAgeOffRule")
                .arg("age off name", name);
        try {
            validateSecurityToken(securityToken);

            if (name == null || name.isEmpty()) {
                throw new ProvenanceAgeOffRuleNotFoundException("The name of the age off rule cannot be empty");
            }

            validateDbStatus();
            return this.graphDb.getAgeOffRule(name);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public AgeOffRule getAgeOffRuleById(ezbake.base.thrift.EzSecurityToken securityToken, long ruleId) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getAgeOffRuleById")
                .arg("age off ruleId", ruleId);
        try {
            validateSecurityToken(securityToken);
            validateDbStatus();

            return this.graphDb.getAgeOffRule(ruleId);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void updateAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, String name, long retentionDurationSeconds) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, ProvenanceIllegalAgeOffDurationSecondsException, ProvenanceIllegalAgeOffRuleNameException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "updateAgeOffRule")
                .arg("age off name", name);
        try {
            validateSecurityToken(securityToken);

            if (name == null || name.isEmpty()) {
                throw new ProvenanceIllegalAgeOffRuleNameException("The name of the age off rule cannot be empty");
            }

            if (retentionDurationSeconds <= 0) {
                throw new ProvenanceIllegalAgeOffDurationSecondsException("The duration seconds must be greater than 0");
            }

            validateDbStatus();
            this.graphDb.updateAgeOffRule(securityToken, name, retentionDurationSeconds, this.getPurgeAppName());
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public List<AgeOffRule> getAllAgeOffRules(ezbake.base.thrift.EzSecurityToken securityToken, int limit, int page) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getAllAgeOffRules");
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.getAllAgeOffRules(limit, page);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public int countAgeOffRules(ezbake.base.thrift.EzSecurityToken securityToken) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "countAgeOffRules");
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.countAgeOffRules();
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public long addDocument(ezbake.base.thrift.EzSecurityToken securityToken, String uri, List<InheritanceInfo> parents, List<AgeOffMapping> ageOffRules) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentExistsException, ProvenanceAgeOffRuleNotFoundException, ProvenanceParentDocumentNotFoundException, ProvenanceCircularInheritanceNotAllowedException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), securityToken)
                .arg("event", "addDocument")
                .arg("uri", uri);
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.addDocument(securityToken, uri, parents, ageOffRules);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public Map<String, AddDocumentResult> addDocuments(ezbake.base.thrift.EzSecurityToken securityToken, Set<AddDocumentEntry> documents, Set<AgeOffMapping> ageOffRules) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, ProvenanceExceedsMaxBatchSizeException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), securityToken)
                .arg("event", "addDocuments");

        try {
            validateSecurityToken(securityToken);
            validateDbStatus();

            if (documents.size() > this.addDocumentsMaxSize) {
                throw new ProvenanceExceedsMaxBatchSizeException("The size of documents exceeds the limit: " + this.addDocumentsMaxSize);
            }
            return this.graphDb.addDocuments(securityToken, documents, ageOffRules);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public int getAddDocumentsMaxSize(ezbake.base.thrift.EzSecurityToken securityToken) throws ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), securityToken)
                .arg("event", "getAddDocumentsMaxSize");

        try {
            validateSecurityToken(securityToken);
            return this.addDocumentsMaxSize;
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public AgeOffInitiationResult startAgeOffEvent(ezbake.base.thrift.EzSecurityToken securityToken, long ruleId, ezbake.base.thrift.DateTime effectiveTime) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), securityToken)
                .arg("event", "startAgeOffEvent")
                .arg("ruleId", ruleId);
        try {
            validateAdminSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.ageOff(securityToken, ruleId, effectiveTime);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void markDocumentAsAged(ezbake.base.thrift.EzSecurityToken securityToken, Set<Long> agedDocumentIds) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "markDocumentAsAged")
                .arg("agedDocumentIds", agedDocumentIds);
        try {
            validateAdminSecurityToken(securityToken);
            validateDbStatus();
            this.graphDb.markAsAged(new ArrayList<Long>(agedDocumentIds));
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public DocumentInfo getDocumentInfo(ezbake.base.thrift.EzSecurityToken securityToken, long id, String uri) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentInfo")
                .arg("documentId", id)
                .arg("uri", uri);
        try {
            validateSecurityToken(securityToken);

            if (id == 0 && StringUtils.isEmpty(uri)) {
                throw new ProvenanceDocumentNotFoundException("Neither id nore uri is present");
            }

            validateDbStatus();
            return this.graphDb.getDocumentInfo(id, uri);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public DerivedResult getDocumentAncestors(ezbake.base.thrift.EzSecurityToken securityToken, List<String> uris) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentAncestors")
                .arg("uris", uris);
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.getAncestors(uris);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }

    }

    @Override
    public DerivedResult getDocumentDescendants(ezbake.base.thrift.EzSecurityToken securityToken, List<String> uris) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentDescendants")
                .arg("uris", uris);
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.getDescendants(uris);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public PurgeInitiationResult markDocumentForPurge(ezbake.base.thrift.EzSecurityToken securityToken, List<String> uris, String name, String description) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "markDocumentForPurge")
                .arg("purge name", name)
                .arg("purge description", description)
                .arg("uris", uris);
        try {
            validateAdminSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.markForPurge(securityToken, uris, name, description);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public PositionsToUris getDocumentUriFromId(ezbake.base.thrift.EzSecurityToken securityToken, List<Long> positionsList) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentUriFromId")
                .arg("positionsList", positionsList);
        try {
            validateSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.getUriFromId(positionsList);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public ConversionResult getDocumentConvertedUrisFromIds(ezbake.base.thrift.EzSecurityToken securityToken, Set<Long> ids) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentConvertedUrisFromIds")
                .arg("ids", ids);
        try {
            validateSecurityToken(securityToken);
            validateDbStatus();
            return this.graphDb.getConvertedUrisFromIds(ids);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public ConversionResult getDocumentConvertedUrisFromUris(ezbake.base.thrift.EzSecurityToken securityToken, Set<String> uris) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getDocumentConvertedUrisFromUris")
                .arg("uris", uris);
        try {
            validateSecurityToken(securityToken);
            validateDbStatus();
            return this.graphDb.getConvertedUrisFromUris(uris);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public PurgeInfo getPurgeInfo(ezbake.base.thrift.EzSecurityToken securityToken, long purgeId) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenancePurgeIdNotFoundException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getPurgeInfo")
                .arg("purgeId", purgeId);
        try {
            validateAdminSecurityToken(securityToken);
            validateDbStatus();
            return this.graphDb.getPurgeInfo(purgeId);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public List<Long> getAllPurgeIds(ezbake.base.thrift.EzSecurityToken securityToken) throws
            ezbake.base.thrift.EzSecurityTokenException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "getAllPurgeIds");
        try {
            validateAdminSecurityToken(securityToken);

            validateDbStatus();
            return this.graphDb.getAllPurgeIds();
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void updatePurge(ezbake.base.thrift.EzSecurityToken securityToken, long purgeId, Set<Long> completelyPurged, String note, boolean resolved) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenancePurgeIdNotFoundException, ProvenanceDocumentNotInPurgeException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "updatePurge")
                .arg("purgeId", purgeId)
                .arg("note", note)
                .arg("resolved", resolved);
        try {
            validateAdminSecurityToken(securityToken);
            validateDbStatus();
            this.graphDb.updatePurge(securityToken, purgeId, completelyPurged, note, resolved);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void removeDocumentAgeOffRuleInheritance(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, long parentId, String parentUri) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, ProvenanceAlreadyAgedException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "removeDocumentAgeOffRuleInheritance")
                .arg("documentId", documentId)
                .arg("documentUri", documentUri)
                .arg("parentId", parentId)
                .arg("parentUri", parentUri);
        try {
            validateSecurityToken(securityToken);

            if (documentId <= 0 && StringUtils.isEmpty(documentUri)) {
                throw new ProvenanceDocumentNotFoundException("Document id/uri not specified");
            }
            if (parentId <= 0 && StringUtils.isEmpty(parentUri)) {
                throw new ProvenanceDocumentNotFoundException("Parent document id/uri not specified");
            }
            validateDbStatus();
            this.graphDb.removeDocAgeOffRuleInheritance(documentId, documentUri, parentId, parentUri);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void removeDocumentExplicitAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, long ageOffRuleId) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, ProvenanceAgeOffRuleNotFoundException, ProvenanceAlreadyAgedException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "removeDocumentExplicitAgeOffRule")
                .arg("documentId", documentId)
                .arg("documentUri", documentUri)
                .arg("ageOffRuleId", ageOffRuleId);
        try {
            validateSecurityToken(securityToken);

            if (documentId <= 0 && StringUtils.isEmpty(documentUri)) {
                throw new ProvenanceDocumentNotFoundException("Document id/uri not specified");
            }
            validateDbStatus();
            this.graphDb.removeDocExplicitAgeOffRule(documentId, documentUri, ageOffRuleId);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void addDocumentExplicitAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, AgeOffMapping ageOffMapping) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, ProvenanceAgeOffRuleNotFoundException, ProvenanceAlreadyAgedException, ProvenanceAgeOffExistsException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "addDocumentExplicitAgeOffRule")
                .arg("documentId", documentId)
                .arg("documentUri", documentUri)
                .arg("ageOffRuleId", ageOffMapping.getRuleId());
        try {
            validateSecurityToken(securityToken);

            if (documentId <= 0 && StringUtils.isEmpty(documentUri)) {
                throw new ProvenanceDocumentNotFoundException("Document id/uri not specified");
            }
            validateDbStatus();
            this.graphDb.addDocExplicitAgeOffRule(securityToken, documentId, documentUri, ageOffMapping);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public void addDocumentInheritanceInfo(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, InheritanceInfo inheritanceInfo) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, ProvenanceCircularInheritanceNotAllowedException, ProvenanceAlreadyAgedException, ProvenanceAgeOffInheritanceExistsException, org.apache.thrift.TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), securityToken)
                .arg("event", "addDocumentInheritanceInfo")
                .arg("documentId", documentId)
                .arg("documentUri", documentUri)
                .arg("parentURI", inheritanceInfo.getParentUri());
        try {
            validateSecurityToken(securityToken);

            if (documentId <= 0 && StringUtils.isEmpty(documentUri)) {
                throw new ProvenanceDocumentNotFoundException("Document id/uri not specified");
            }
            validateDbStatus();
            this.graphDb.addDocumentInheritanceInfo(securityToken, documentId, documentUri, inheritanceInfo);
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }


    protected void writeRow(String row, String columnFamily, String columnQualifier, String visibility, String value) throws TException {
        Connector connector = null;
        BatchWriter writer = null;
        try {
            connector = getAccumuloConnector();
            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(1000000L);
            config.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            config.setMaxWriteThreads(10);
            writer = connector.createBatchWriter(TABLE, config);
            Mutation m = new Mutation(row);
            m.put(columnFamily, columnQualifier, new ColumnVisibility(visibility), new Value(value.getBytes()));
            writer.addMutation(m);
            writer.flush();
        } catch (IOException e) {
            throw new TException("Error: IOException " + e);
        } catch (TableNotFoundException e) {
            throw new TException("Error: Accumulo Misconfigured - table is not found " + e);
        } catch (MutationsRejectedException e) {
            throw new TException("Error: Mutation Rejected " + e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    throw new TException("Error: Mutation Rejected " + e);
                }
            }
        }
    }

    /* create, read, write, manage, delete */

    @Override
    public void recordObjectAccess(ezbake.base.thrift.EzSecurityToken securityToken, String documentUri, ObjectAccessType accessType) throws TException {
        validateSecurityToken(securityToken);
        String accessStr;
        AuditEvent evt = null;
        switch (accessType) {
            case CREATE:
                accessStr = "create";
                evt = event(AuditEventType.FileObjectCreate.getName(), securityToken);
                break;
            case READ:
                accessStr = "read";
                evt = event(AuditEventType.FileObjectAccess.getName(), securityToken);
                break;
            case WRITE:
                accessStr = "write";
                evt = event(AuditEventType.FileObjectModify.getName(), securityToken);
                break;
            case MANAGE:
                accessStr = "manage";
                evt = event(AuditEventType.FileObjectModify.getName(), securityToken);
                break;
            case DELETE:
                evt = event(AuditEventType.FileObjectDelete.getName(), securityToken);
                accessStr = "delete";
                break;
            default:
                accessStr = "unknown";
                evt = event(AuditEventType.FileObjectAccess.getName(), securityToken);
        }
        evt.arg("event", "recordObjectAccess")
                .arg("accessType", accessType.name())
                .arg("documentUri", documentUri);
        // verify table created
        try {

            Integer append = randomGenerator.nextInt();

            Long currentTime = System.currentTimeMillis();
            String principalJson = new String(new TSerializer(new TSimpleJSONProtocol.Factory()).serialize(securityToken.tokenPrincipal), EzSecurityConstant.CHARSET);
            String record = String.format("{timestamp:%d uri:%s access:%s principal:%s}", currentTime, documentUri, accessStr, principalJson);
            writeRow(documentUri, String.format("%019d", currentTime), String.format("%d", append), "U", record);
            writeRow(securityToken.tokenPrincipal.principal, String.format("%019d", currentTime), String.format("%d", append), "U", record);
        } catch (UnsupportedEncodingException e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw new TException("Unable to encode " + EzSecurityConstant.CHARSET + " string");
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }

    }

    private ResultsAndContinuation fetchUDorDU(ezbake.base.thrift.EzSecurityToken securityToken,
                                               String userPrincipal,
                                               ezbake.base.thrift.DateTime startDateTime,
                                               ezbake.base.thrift.DateTime stopDateTime,
                                               int numToFetch,
                                               AccumuloContinuationPoint continuationPoint)
            throws TException, ezbake.base.thrift.EzSecurityTokenException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "fetchUDorDU")
                .arg("userPrincipal", userPrincipal);

        Connector connector;
        Key startKey;
        Key stopKey;
        String startDateTimeString = String.format("%019d", convertDateTime2Millis(startDateTime));
        String stopDateTimeString = String.format("%019d", convertDateTime2Millis(stopDateTime));
        ResultsAndContinuation rtn = new ResultsAndContinuation();

        rtn.continuationPoint = new AccumuloContinuationPoint();
        rtn.continuationPoint.startAtBeginning = false;
        rtn.results = new ArrayList<String>();
        String[] authlist = securityToken.authorizations.formalAuthorizations.toArray(new String[securityToken.authorizations.formalAuthorizations.size()]);

        if (numToFetch > 10000) {
            numToFetch = 10000;
        }

        try {
            connector = getAccumuloConnector();
            // do a lookup in the lookup table first

            Scanner scanner = connector.createScanner(TABLE, new Authorizations(authlist));
            if (continuationPoint.startAtBeginning) {
                startKey = new Key(new Text(userPrincipal), new Text(startDateTimeString));
            } else {
                // move on to the following key
                startKey = new Key(new Text(continuationPoint.rowId), new Text(continuationPoint.colFam), new Text(continuationPoint.colQual)).followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            }
            stopKey = new Key(new Text(userPrincipal), new Text(stopDateTimeString));

            scanner.setRange(new Range(startKey, stopKey));

            Iterator<Map.Entry<Key, Value>> lookupIter = scanner.iterator();
            int i = 0;
            Map.Entry<Key, Value> current;
            while (lookupIter.hasNext() && (i < numToFetch)) {
                current = lookupIter.next();
                rtn.results.add(current.getValue().toString());
                rtn.continuationPoint.rowId = current.getKey().getRow().toString();
                rtn.continuationPoint.colFam = current.getKey().getColumnFamily().toString();
                rtn.continuationPoint.colQual = current.getKey().getColumnQualifier().toString();
                i++;
            }
        } catch (IOException e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            logger.error("Unexpected IOException thrown getting accumulo connector", e);
            throw new RegistrationException(e.getMessage());
        } catch (TableNotFoundException e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            logger.error("Registrations table does not exist", e);
            throw new RegistrationException(e.getMessage());
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
        return rtn;
    }

    @Override
    public ResultsAndContinuation fetchUsersDocuments(ezbake.base.thrift.EzSecurityToken securityToken,
                                                      String userPrincipal,
                                                      ezbake.base.thrift.DateTime startDateTime,
                                                      ezbake.base.thrift.DateTime stopDateTime,
                                                      int numToFetch,
                                                      AccumuloContinuationPoint continuationPoint)
            throws TException, ezbake.base.thrift.EzSecurityTokenException {

        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "fetchUsersDocuments")
                .arg("userPrincipal", userPrincipal);
        try {
            validateSecurityToken(securityToken);
            if (Sets.intersection(securityToken.authorizations.getPlatformObjectAuthorizations(), this.auditGroups).size() > 0) {
                return fetchUDorDU(securityToken, userPrincipal, startDateTime, stopDateTime, numToFetch, continuationPoint);
            } else {
                throw new ezbake.base.thrift.EzSecurityTokenException("User does not have read access in the apps.EzProvenanceService group");
            }
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }


    /*
    Allows an application to fetch the documents accessed by a user
    */
    @Override
    public ResultsAndContinuation fetchDocumentUsers(ezbake.base.thrift.EzSecurityToken securityToken,
                                                     String documentUri,
                                                     ezbake.base.thrift.DateTime startDateTime,
                                                     ezbake.base.thrift.DateTime stopDateTime,
                                                     int numToFetch,
                                                     AccumuloContinuationPoint continuationPoint)
            throws TException, ezbake.base.thrift.EzSecurityTokenException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), securityToken)
                .arg("event", "fetchUsersDocuments")
                .arg("documentUri", documentUri);
        try {
            validateSecurityToken(securityToken);
            if (Sets.intersection(securityToken.authorizations.getPlatformObjectAuthorizations(), this.auditGroups).size() > 0) {
                return fetchUDorDU(securityToken, documentUri, startDateTime, stopDateTime, numToFetch, continuationPoint);
            } else {
                throw new ezbake.base.thrift.EzSecurityTokenException("User does not have read access in the apps.EzProvenanceService group");
            }
        } catch (Exception e) {
            evt.failed();
            evt.arg(e.getClass().getName(), e);
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }
    }
}

