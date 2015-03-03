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

package ezbake.publisher.service;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.common.openshift.OpenShiftUtil;
import ezbake.common.properties.EzProperties;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.publisher.thrift.ContentPublisher;
import ezbake.publisher.thrift.PublishData;
import ezbake.publisher.thrift.PublishResult;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.services.provenance.thrift.*;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.util.AuditLoggerConfigurator;
import ezbake.warehaus.*;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ContentPublisherService extends EzBakeBaseThriftService implements ContentPublisher.Iface {
    private static final Logger logger = LoggerFactory.getLogger(ContentPublisherService.class);
    private ThriftClientPool pool;
    private EzProperties props;
    private EzbakeSecurityClient security;
    private static AuditLogger auditLogger;

    public static final String SSR_TOPIC = "SSR";
    public static final String GROUP_ID = "ContentPublisherService";

    static {
        AuditLoggerConfigurator.setAdditivity(true);
        auditLogger = AuditLogger.getAuditLogger(ContentPublisherService.class);
    }

    public ContentPublisherService(ThriftClientPool pool, EzbakeSecurityClient security, EzProperties props) {
        this.props = props;
        this.pool = pool;
        this.security = security;
    }

    // No arg constructor for ThriftRunner support
    public ContentPublisherService() {}

    @Override
    public TProcessor getThriftProcessor() {
        props = new EzProperties(getConfigurationProperties(), true);
        pool = new ThriftClientPool(props);
        security = new EzbakeSecurityClient(props);
        return new ContentPublisher.Processor(this);
    }

    @Override
    public boolean ping() {
        // Check provenance, warehaus, INS, and locksmith
        ProvenanceService.Client provenance = null;
        WarehausService.Client warehaus = null;
        InternalNameService.Client ins = null;
        EzLocksmith.Client locksmith = null;

        boolean healthy;

        try {
            provenance = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
            healthy = provenance.ping();

            warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
            healthy = healthy && warehaus.ping();

            ins = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
            healthy = healthy && ins.ping();

            locksmith = pool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class);
            healthy = healthy && locksmith.ping();

            return healthy;
        } catch (TException e) {
            logger.error("Could not retrieve Thrift client, service is not healthy", e);
            return false;
        } finally {
            // Return all of the service clients to the pool
            pool.returnToPool(provenance);
            pool.returnToPool(warehaus);
            pool.returnToPool(ins);
            pool.returnToPool(locksmith);
        }
    }

    @Override
    public PublishResult publish(PublishData data, Visibility visibility, EzSecurityToken token) throws org.apache.thrift.TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "publish");
        auditArgs.put("uri", data.getEntry().getUri());
        auditLog(token, AuditEventType.FileObjectCreate, auditArgs);
        WarehausService.Client warehaus = null;
        ProvenanceService.Client provenance = null;
        SSRJSON ssrjson = data.getSsrjson();
        PublishResult result = new PublishResult();

        if (data.isSetProvenance()) {
            try {
                provenance = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
                EzSecurityToken provenanceToken = security.fetchDerivedTokenForApp(token, pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME));
                long provenanceDocumentId = provenance.addDocument(provenanceToken, data.getProvenance().getUri(), data.getProvenance().getParents(), data.getProvenance().getAgeOffRules());
                result.setProvenanceDocumentId(provenanceDocumentId);
            } catch (ProvenanceDocumentExistsException e) {
                if (data.isIgnoreProvenanceDuplicate()) {
                    logger.warn("Duplicate document found in provenance for {}, ignoring based on input", data.getEntry().getUri());
                    // TODO add the document ID to the result here somehow
                } else {
                    logger.error("Duplicate document found in provenance for {}, failing based on input", data.getEntry().getUri(), e);
                    throw new TException("Duplicate document found, not ignoring based on input", e);
                }
            } catch (ProvenanceCircularInheritanceNotAllowedException | ProvenanceAgeOffRuleNotFoundException
                    | ProvenanceParentDocumentNotFoundException e) {
                logger.error("Error indexing data in provenance", e);
                throw new TException(e);
            } finally {
                pool.returnToPool(provenance);
            }
        }

        try {
            warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
            PutRequest putRequest = new PutRequest();
            PutUpdateEntry entry = new PutUpdateEntry(data.getEntry(), visibility);
            putRequest.addToEntries(entry);
            EzSecurityToken warehausToken = security.fetchDerivedTokenForApp(token, pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME));
            IngestStatus status = warehaus.put(putRequest, warehausToken);

            // Handle failed ingest
            if (status.getStatus() != IngestStatusEnum.SUCCESS) {
                logger.error("Could not properly ingest record into warehouse for URI {}", data.getEntry().getUri());
                throw new TException(String.format("Could not properly ingest record into warehouse for URI %s", data.getEntry().getUri()));
            } else {
                result.setWarehouseTimestamp(status.getTimestamp());
            }
            logger.debug("Successfully inserted uri {} into Warehaus", data.getEntry().getUri());
        } finally {
            pool.returnToPool(warehaus);
        }

        Pair<String, Set<String>> info = getINSInfo(data.getFeedname(), security.fetchDerivedTokenForApp(token, pool.getSecurityId(InternalNameServiceConstants.SERVICE_NAME)));

        EzBroadcaster broadcaster = null;
        try {
            try {
                for (String topic : info.getValue1()) {
                    broadcaster = getBroadcaster(token, GROUP_ID, topic);
                    broadcaster.registerBroadcastTopic(topic);
                    
                    auditArgs = Maps.newHashMap();
                    auditArgs.put("topic", topic);
                    auditLog(token, AuditEventType.ImportOfInformation, auditArgs);
                    
                    broadcaster.broadcast(topic, visibility, data.getEntry().getParsedData());
                } 
            } finally {
                try {
                    if (broadcaster != null) {
                        broadcaster.close();
                    }
                } catch (IOException ex) {
                    logger.error("Error closing broadcaster", ex);
                }
            }

            if (ssrjson != null) {
                broadcaster = getBroadcaster(token, GROUP_ID, SSR_TOPIC);
                broadcaster.registerBroadcastTopic(SSR_TOPIC);
                
                auditArgs = Maps.newHashMap();
                auditArgs.put("topic", SSR_TOPIC);
                auditLog(token, AuditEventType.ImportOfInformation, auditArgs);
                
                broadcaster.broadcast(SSR_TOPIC, ssrjson.getSsr().getVisibility(),
                        ThriftUtils.serialize(ssrjson));
            }
        } catch (IOException e) {
            logger.error("Error during broadcast", e);
            throw new TException(e);
        } finally {
            try {
                if (broadcaster != null) {
                    broadcaster.close();
                }
            } catch (IOException ex) {
                logger.error("Error closing broadcaster", ex);
            }
        }

        return result;
    }

    private Pair<String, Set<String>> getINSInfo(String feedName, EzSecurityToken token) throws TException {
        InternalNameService.Client insClient = null;
        String applicationSecurityId = new EzSecurityTokenWrapper(token).getApplicationSecurityId();

        try {
            insClient = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);

            Set<String> topics = insClient.getTopicsForFeed(applicationSecurityId, feedName);
            String prefix = insClient.getURIPrefix(applicationSecurityId, feedName);

            return new Pair<String, Set<String>>(prefix, topics);
        } catch (Exception ex) {
            logger.error("Failed to communicate with INS", ex);
            throw new TException("Failed to communicate with INS", ex);
        } finally {
            pool.returnToPool(insClient);
        }
    }

    private EzBroadcaster getBroadcaster(EzSecurityToken token, String groupId, String topic) {
        EzBroadcaster ezbroadcaster = null;
        if (props.getBoolean(EzBroadcaster.PRODUCTION_MODE, true) || OpenShiftUtil.inOpenShiftContainer()) {
            EzLocksmith.Client locksmith = null;
            try {
                locksmith = pool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class);
                String key = locksmith.retrievePublicKey(token, topic, null);
                ezbroadcaster = EzBroadcaster.create(props, groupId, key, topic, false);
            } catch (TException e) {
                throw new RuntimeException("Could not initialize broadcaster without key from locksmith", e);
            } finally {
                pool.returnToPool(locksmith);
            }
        } else {
            ezbroadcaster = EzBroadcaster.create(props, groupId);
        }
        ezbroadcaster.registerBroadcastTopic(topic);
        return ezbroadcaster;
    }
    
    private void auditLog(EzSecurityToken userToken, AuditEventType eventType, 
            Map<String, String> args) {
        AuditEvent event = new AuditEvent(eventType, userToken);
        for (String argName : args.keySet()) {
            event.arg(argName, args.get(argName));
        }
        if (auditLogger != null) {
            auditLogger.logEvent(event);
        }
    }
}
