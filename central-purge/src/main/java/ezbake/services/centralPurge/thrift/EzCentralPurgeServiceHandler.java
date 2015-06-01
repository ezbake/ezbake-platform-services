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

package ezbake.services.centralPurge.thrift;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.mongodb.*;
import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.ezpurge.ServicePurgeClient;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.centralPurge.helpers.DelayedServicePurgeState;
import ezbake.services.centralPurge.helpers.EzCentralPurgeServiceHelpers;
import ezbake.services.provenance.thrift.*;
import ezbake.thrift.ThriftClientPool;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;
import ezbakehelpers.mongoutils.MongoHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

import static ezbake.common.time.DateUtils.getCurrentDateTime;
import static ezbake.services.centralPurge.helpers.EzCentralPurgeServiceHelpers.*;
import static ezbake.util.AuditEvent.event;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

//import ezbake.common.time.DateUtils;

/**
 * Created by jpercivall on 7/2/14.
 */

public class EzCentralPurgeServiceHandler extends ezbake.base.thrift.EzBakeBaseThriftService implements EzCentralPurgeService.Iface  {


    private final Logger logger = getLogger(EzCentralPurgeServiceHandler.class);

    private static final String
            COMMON_APP_NAME = "common_services",
            PROVENANCE_SERVICE_NAME = ProvenanceServiceConstants.SERVICE_NAME,
            EZBAKE_BASE_PURGE_SERVICE_NAME=ezCentralPurgeServiceConstants.SERVICE_NAME,
            PURGE_COLLECTION = "purgeCollection",
            AGEOFF_COLLECTION = "ageOffCollection";


    //private final String COMMON_APP_NAME = "common_services";
    //private final String EZBAKE_BASE_PURGE_SERVICE_NAME = ezCentralPurgeServiceConstants.SERVICE_NAME;
    //private final String PROVENANCE_SERVICE_NAME = ProvenanceServiceConstants.SERVICE_NAME;
    private String purgeAppSecurityId;
    private EzbakeSecurityClient securityClient;
    private Properties configuration;
    private static final AuditLogger auditLogger= new AuditLogger(EzCentralPurgeServiceHandler.class);
    private boolean initialized=false;
    private DelayQueue<DelayedServicePurgeState> servicePollDelayQueue;

    // This class runs a purge for every age off rule with out of date documents
    private class AutomaticAgeOff implements Runnable{
        public void run() {
            try {
                ProvenanceService.Client client = null;
                EzSecurityToken centralTokenForProvenance = null;

                // Get app token and set up audit event
                try {
                    centralTokenForProvenance = securityClient.fetchAppToken();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), centralTokenForProvenance)
                        .arg("event", "automatic ageOff");
                List<Long> ageOffIds = new LinkedList<>();
                ThriftClientPool pool = null;
                try {
                    pool = new ThriftClientPool(configuration);
                    // Get every ageOffRule, then start an ageOffEvent for each.
                    centralTokenForProvenance = securityClient.fetchAppToken(getProvenanceSecurityId(pool));
                    client = getProvenanceThriftClient(pool);
                    List<AgeOffRule> ageOffRules = client.getAllAgeOffRules(centralTokenForProvenance, 0, 0);
                    for (AgeOffRule ageOffRule : ageOffRules) {
                        try {
                            executeAgeOff(centralTokenForProvenance, ageOffRule.getId(), true);
                            ageOffIds.add(ageOffRule.getId());
                        } catch (Exception e) {
                            logError(e,evt,"Automatic ageOff failed [" + e.getClass().getName() + ":" + e.getMessage() + "] for this rule:" + ageOffRule.getName() + "(id:" + ageOffRule.getId() + ")");
                        }
                    }
                } catch (Exception e){
                    logError(e,evt,"Automatic ageOff failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
                }  finally {
                    evt.arg("ageOffIds", ageOffIds);
                    auditLogger.logEvent(evt);
                    logEventToPlainLogs(logger,evt);
                    if (client != null)
                        returnClientToPool(client,pool);
                    if (pool!=null)
                        pool.close();
                }
            } catch (Exception e){
                e.printStackTrace();
                logger.error("Automatic ageOff failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
                auditLogger.log("Automatic ageOff failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
            }
        }
    }

    // TODO: determine where logger/stdOut go in ezcentos
    // This class polls each purge for a status update
    private class AutomaticUpdate implements Runnable{
        @Override
        public void run() {
            try {
                logger.debug("Starting AutomaticUpdate");
                DelayedServicePurgeState delayedServicePurgeState;
                EzSecurityToken centralPurgeServiceToken = null;
                try {
                    centralPurgeServiceToken = securityClient.fetchAppToken();
                } catch (TException e) {
                    e.printStackTrace();
                }
                List<String> servicesUpdated = new LinkedList<>();
                AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), centralPurgeServiceToken)
                        .arg("event", "automatic update");
                try {
                    // While there is still a delayedServicePurgeState in the queue with an expired poll
                    while ((delayedServicePurgeState = servicePollDelayQueue.poll()) != null) {
                        ServicePurgeState servicePurgeState = delayedServicePurgeState.getServicePurgeState();
                        PurgeState purgeState = servicePurgeState.getPurgeState();
                        PurgeStatus status = purgeState.getPurgeStatus();
                        String appName = delayedServicePurgeState.getApplicationName();
                        String serviceName = delayedServicePurgeState.getServiceName();

                        // If the last update indicates the service is still working on that purge then ask the service for an update
                        if (serviceStillRunning(status)) {
                            try {
                                purgeState = getServiceClientAndUpdate(purgeState, appName, serviceName);
                                status = purgeState.getPurgeStatus();
                                servicesUpdated.add(appName + "_" + serviceName + ":" + purgeState.getPurgeId() + ":" + purgeState.getPurgeStatus());

                                // If the purge is still being worked on by that purge then put it back in the queue
                                if (serviceStillRunning(status)) {
                                    DelayedServicePurgeState newDelayedServicePurgeState = new DelayedServicePurgeState(servicePurgeState, appName, serviceName);
                                    servicePollDelayQueue.offer(newDelayedServicePurgeState);
                                }
                            } catch (Exception e) {
                                logError(e,evt,"Automatic update failed ["+e.getClass().getName()+":" +e.getMessage()+"] for this app/service:"+appName+"_"+serviceName);
                            }
                        } else {
                            // (usually will only get here if the service ran the initial beginPurge synchronously)
                            try {
                                updatePurge(securityClient.fetchAppToken().deepCopy(), purgeState, delayedServicePurgeState.getApplicationName(), delayedServicePurgeState.getServiceName());
                                servicesUpdated.add(appName + "_" + serviceName + ":" + purgeState.getPurgeStatus());
                            } catch (Exception e){
                                logError(e,evt,"Automatic update failed ["+e.getClass().getName()+":" +e.getMessage()+"] for this app/service:"+appName+"_"+serviceName);
                            }
                        }
                    }
                } catch (Exception e){
                    logError(e,evt,"Automatic update failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
                }  finally {
                    evt.arg("Updated services", servicesUpdated);
                    auditLogger.logEvent(evt);
                    logEventToPlainLogs(logger,evt);
                }
            } catch (Exception e){
                e.printStackTrace();
                auditLogger.log("Automatic update failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
                logger.error("Automatic update failed ["+e.getClass().getName()+":" +e.getMessage()+"]");
            }
        }
    };

    // This class starts the initial Purge for all services, allows beginPurge to be started asynchronously
    private class Purger implements Runnable {
        PurgeInitiationResult result;
        EzSecurityToken token;
        CentralPurgeType centralPurgeType;

        Purger(PurgeInitiationResult result, EzSecurityToken token, CentralPurgeType centralPurgeType) {
            this.result = result;
            this.token = token;
            this.centralPurgeType = centralPurgeType;
        }

        public void run() {
            logger.debug("Starting Purger");
            CentralPurgeState centralPurgeState = null;
            AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), token)
                    .arg("event", "startingPurge")
                    .arg("PurgeId",result.getPurgeId())
                    .arg("purge type",centralPurgeType);
            try {
                // starts the purge and updates the backend
                centralPurgeState = getCentralPurgeState(result.purgeId);

                Map<String, ApplicationPurgeState> appMap = centralPurgeState.getApplicationStates();
                appMap = servicePurger(token, result.getPurgeId(), result.getToBePurged(), appMap, false, centralPurgeType,evt);
                centralPurgeState.setApplicationStates(appMap);

                updateCentralPurgeState(centralPurgeState, result.getPurgeId());
            } catch (UnknownHostException e) {
                logError(e,evt,"Purger unable to reach MongoDB:["+e.getClass().getName()+":" +e.getMessage()+"] on purgeId="+result.getPurgeId());
            } catch (Exception e){
                logError(e,evt,"Purger failed ["+e.getClass().getName()+":" +e.getMessage()+"] on purgeId="+result.getPurgeId());
            } finally {
                auditLogger.logEvent(evt);
                logEventToPlainLogs(logger,evt);
            }
        }
    }

    // This class starts the initial ageOff for all services, allows ageOffs to be started asynchronously
    private class AgeOffEventPurger implements Runnable {
        AgeOffInitiationResult result;
        EzSecurityToken token;
        boolean synchronous = false;
        AuditEvent evt;

        AgeOffEventPurger (AgeOffInitiationResult result, EzSecurityToken token,AuditEvent evt) {
            this.result = result;
            this.token = token;
            this.evt = evt;
        }

        public void run()  {
            logger.info("Starting AgeOffEventPurger");

            try {
                // starts the ageOff and updates the backend
                CentralAgeOffEventState centralAgeOffEventState = getCentralAgeOffEventState(result.getAgeOffId());

                Map<String,ApplicationPurgeState> appMap= centralAgeOffEventState.getApplicationStates();
                appMap=servicePurger(token, result.getAgeOffId(),result.getAgeOffDocumentIds(), appMap, synchronous,CentralPurgeType.NORMAL,evt);

                centralAgeOffEventState.setApplicationStates(appMap);

                updateCentralAgeOffEventState(centralAgeOffEventState, result.getAgeOffId());
            } catch (UnknownHostException e) {
                logError(e,evt,"CentralPurgeService unable to reach MongoDB in AgeOffEventPurger:["+e.getClass().getName()+":" +e.getMessage()+"]");
            } catch (Exception e) {
                logError(e, evt, "AgeOffEventPurger failed [" + e.getClass().getName() + ":" + e.getMessage() + "] on ageOffId=" + result.getAgeOffId());
            }
        }
    }

    public EzCentralPurgeServiceHandler(){
    }

    // A method used by ThriftRunner to start the service
    @Override
    public TProcessor getThriftProcessor() {
        try {
            logger.debug("Starting getThriftProcessor");
            init();
            return new EzCentralPurgeService.Processor(this);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // TODO: unit test all the things
    // TODO: integration test
    /* Sets up the service with two other running threads. One runs every 5 seconds checking for updates on purges.
     * The other checks every midnight for documents that need to be aged off. Also checks to see if the CentralPurge
     * MongoDB backend is in sync with the Provenance titan backend
     */
    private void init() throws TException {
        configuration = getConfigurationProperties();
        securityClient = new EzbakeSecurityClient(configuration);
        EzSecurityToken centralPurgeServiceToken = securityClient.fetchAppToken();
        AuditEvent evt = event(AuditEventType.ApplicationInitialization.getName(), centralPurgeServiceToken)
                .arg("event", "init");
        try {
            logger.info("Starting init");
            servicePollDelayQueue = new DelayQueue<>();
            EzSecurityTokenWrapper ezSecurityTokenWrapper = new EzSecurityTokenWrapper(centralPurgeServiceToken);
            purgeAppSecurityId = ezSecurityTokenWrapper.getSecurityId();

            // Set an instance of Calendar for Midnight.
            Calendar c = Calendar.getInstance();
            c.add(Calendar.DAY_OF_MONTH, 1);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);

            //c.add(Calendar.MINUTE, 1);

            // Starts each of the delayed/recurring threads
            final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
            final Runnable automaticAgeOff = new AutomaticAgeOff();
            final Runnable automaticUpdate = new AutomaticUpdate();

            final ScheduledFuture<?> automaticAgeOffHandle = scheduler.scheduleAtFixedRate(automaticAgeOff, c.getTimeInMillis() - System.currentTimeMillis(), 24 * 3600000, MILLISECONDS);
            final ScheduledFuture<?> automaticUpdateHandle = scheduler.scheduleAtFixedRate(automaticUpdate, 2, 2, SECONDS);
            // Just for testing
            // final ScheduledFuture<?> automaticAgeOffHandle = scheduler.scheduleAtFixedRate(automaticAgeOff, c.getTimeInMillis() - System.currentTimeMillis(), 60000*10, MILLISECONDS);

            initialized = true;
            ProvenanceService.Client client = null;
            ThriftClientPool pool = null;
            try {
                pool = new ThriftClientPool(configuration);
                EzSecurityToken centralTokenForProvenance = securityClient.fetchAppToken(getProvenanceSecurityId(pool));
                client = getProvenanceThriftClient(pool);
                if (client.ping()) {
                    boolean ageOffOutOfSync = false;
                    boolean purgeOutOfSync = false;

                    // Check to see if there are any ageOffEvents which reference ageOffRules not in Provenance's backend
                    List<AgeOffRule> ageOffRules = client.getAllAgeOffRules(centralTokenForProvenance, 0, 0);
                    List<Long> provenanceAgeOffRules = new LinkedList<>();
                    for(AgeOffRule ageOffRule : ageOffRules){
                        provenanceAgeOffRules.add(ageOffRule.getId());
                    }
                    List<Long> ageOffEvents = this.getAllAgeOffEvents(centralPurgeServiceToken);
                    List<CentralAgeOffEventState> ageOffEventStates = this.getAgeOffEventState(centralPurgeServiceToken, ageOffEvents);
                    // note: there can be rules in the provenance service that don't yet have an event trying to age them
                    for (CentralAgeOffEventState centralAgeOffEventState : ageOffEventStates) {
                        if (!provenanceAgeOffRules.contains(centralAgeOffEventState.getAgeOffRuleId())) {
                            ageOffOutOfSync = true;
                            break;
                        }
                    }

                    // Check to see if there are any purgeIds in either CentralPurge's MongoDB or Provenance's Titan that aren't in the other
                    List<Long> provenancePurgeIds = client.getAllPurgeIds(centralTokenForProvenance);
                    List<CentralPurgeState> centralPurgeStates = this.getPurgeState(centralPurgeServiceToken, getAllPurgeIdsInMongo());
                    List<Long> centralPurgePurgeIds = new LinkedList<>();
                    for (CentralPurgeState centralPurgeState : centralPurgeStates) {
                        centralPurgePurgeIds.add(centralPurgeState.getPurgeInfo().getId());
                    }/*
                    for (CentralAgeOffEventState centralAgeOffEventState : ageOffEventStates){
                        centralPurgePurgeIds.add(centralAgeOffEventState.getAgeOffEventInfo().getId());
                    }*/
                    // Need to verify if either contain a purgeId that is not in the other
                    if (!centralPurgePurgeIds.containsAll(provenancePurgeIds) || !provenancePurgeIds.containsAll(centralPurgePurgeIds)) {
                        purgeOutOfSync = true;
                    }

                    // If either out of sync, throw an exception indicating that the service is still running but the backends are out of sync
                    if (ageOffOutOfSync || purgeOutOfSync) {
                        throw new CentralPurgeServiceException("Initialized but MongoDB out of sync with Provenance's titan DB, check ageOffCollection & purgeCollection");
                    }

                } else {
                    throw new CentralPurgeServiceException("Initialized but unable to reach provenance service");
                }
            } catch (Exception e){
                logError(e,evt,e.getMessage());
            }  finally {
                if (client != null)
                    returnClientToPool(client,pool);
                if (pool!=null)
                    pool.close();
            }
        } catch (Exception e){
            logError(e, evt, "Init failed [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an error in init:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally{
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
        }
    }

    /*  beginPurge()
     * This method should use the provided URIs to start a new purge event within
     * the provenance service utilizing that service’s markForPurge(). It should
     * return the ezProvenance.PurgeInitiationResult generated by that service
     * as an indication that the purge has begun, then discover all purge
     * services within the system and begin calling the beginPurge() methods for
     * each of those applications.
     *
     * Initially, it is probably best to call one service, wait for it to complete,
     * then call the next service. The service must not block other
     * requests while the purge is running.
     *
     * Additionally, this method should create some state internal to the
     * EzCentralPurgeService (likely backed by MongoDB) to be able to support
     * getPurgeState() queries while the purge is ongoing and after it has completed.
     *
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     * @param uris  This is the list of uris for which we must purge all descendant
     *              documents.
     * @param name  A human-readable name for this purge. This service should enforce
     *              the uniqueness of names.
     * @param description A human-readable (potentially long) description of why this
     *              purge is taking place
     * @returns The method returns the ezProvenance.PurgeInitiation result generated by
     *          the provenance service’s markForPurge() method that is called by this
     *          method.
     */
    @Override
    public PurgeInitiationResult beginPurge(final EzSecurityToken token, final List<String> uris, String name, final String description) throws EzSecurityTokenException, TException {
        return beginPurgeHelper(token, uris, name, description, CentralPurgeType.NORMAL);
    }

    // A begin Purge method for Virus purges
    @Override
    public PurgeInitiationResult beginVirusPurge(EzSecurityToken token, List<String> uris, String name, String description) throws EzSecurityTokenException, TException {
        return beginPurgeHelper(token, uris, name, description, CentralPurgeType.VIRUS);
    }

    // Just a helper method for the beginPurge methods
    private PurgeInitiationResult beginPurgeHelper(EzSecurityToken token, List<String> uris, String name, String description, CentralPurgeType centralPurgeType) throws EzSecurityTokenException, TException {

        ProvenanceService.Client client= null;
        PurgeInitiationResult result=null;
        securityClient = new EzbakeSecurityClient(configuration);
        EzSecurityToken centralTokenForProvenance = null;

        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), token)
                .arg("event", "beginPurge")
                .arg("name",name)
                .arg("description",description)
                .arg("purge type",centralPurgeType);

        ThriftClientPool pool = null;
        try {
            pool = new ThriftClientPool(configuration);
            validateCentralPurgeSecurityToken(token);
            centralTokenForProvenance = securityClient.fetchDerivedTokenForApp(token,getProvenanceSecurityId(pool));
            client=getProvenanceThriftClient(pool);

            // Enforce uniqueness of names
            for(Long purgeId: client.getAllPurgeIds(centralTokenForProvenance)){
                PurgeInfo purgeInfoInside = client.getPurgeInfo(centralTokenForProvenance,purgeId);
                if( purgeInfoInside!=null && name.equals(purgeInfoInside.getName())){
                    throw new CentralPurgeServiceException("A purge with that name has already been created. Purge name must be unique.");
                }
            }

            // Tell Provenance service that the purge is occurring and get the started purge's information
            result = client.markDocumentForPurge(centralTokenForProvenance, uris, name, description);
            evt.arg("purgeId", result.purgeId);
            PurgeInfo purgeInfo = client.getPurgeInfo(centralTokenForProvenance,result.getPurgeId());

            // Initialize the CentralPurgeState information
            Map<String,ApplicationPurgeState> appStates = initializeApplicationState(result.purgeId);
            CentralPurgeState centralPurgeState = new CentralPurgeState();
            centralPurgeState.setCentralStatus(CentralPurgeStatus.ACTIVE);
            centralPurgeState.setPurgeInfo(purgeInfo);
            centralPurgeState.setCentralPurgeType(centralPurgeType);
            centralPurgeState.setApplicationStates(appStates);

            updateCentralPurgeState(centralPurgeState,purgeInfo.getId());

            // Tell services to start the purge (asynchronously) and stops the thread when finished
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(new Purger(result, token, centralPurgeType));
            executorService.shutdown();

        } catch (CentralPurgeServiceException e){
            logError(e,evt,e.getMessage());
            throw e;
        }  catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in beginPurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in beginPurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in beginPurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in beginPurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            if(client!=null)
                returnClientToPool(client,pool);
            if (pool!=null)
                pool.close();
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
        }
        return result;
    }

    /* getPurgeState()
     * This method is used by the UI to get the detailed status of a list of purge
     * event IDs. The UI can fetch the list of all purge event IDs by calling
     * the provenance service’s getAllPurgeIds().
     *
     * The UI can achieve paging by fetching the entire list from the provenance
     * service’s getAllPurgeIds() and then only requesting a slice of that list
     * from the central purge service.
     *
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     * @param purgIds This is the list of purges for which the UI is requesting
     *                detailed information
     * @returns PurgeState The state for the selected purgeIds
     */
    @Override
    public List<CentralPurgeState> getPurgeState(EzSecurityToken token, List<Long> purgeIds) throws EzSecurityTokenException, TException {

        DBCollection purgeColl = null;
        Mongo mongoClient = null;
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), token)
                .arg("event", "getPurgeState")
                .arg("purgeIds", purgeIds);
        try {
            validateCentralPurgeSecurityToken(token);

            // Get access to the purge collection within Mongo
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);

            List<CentralPurgeState> result = new ArrayList<CentralPurgeState>();

            // Gets all centralPurgeStates that are in the purgeIds list
            BasicDBObject query = new BasicDBObject(EzCentralPurgeServiceHelpers.PurgeId, new BasicDBObject("$in", purgeIds));
            DBCursor cursor = purgeColl.find(query);

            // Decodes each centralPurgeState form how it's stored in MongoDB and adds it to the return variable
            for (DBObject dbObject : cursor) {
                CentralPurgeState centralPurgeState = decodeCentralPurgeState((DBObject) dbObject.get(CentralPurgeStateString));
                result.add(centralPurgeState);
            }
            return result;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in getPurgeState:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in getPurgeState:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in getPurgeState:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in getPurgeState:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if(mongoClient!=null)
                mongoClient.close();
        }
    }

    /* beginManualAgeOff()
     * This method is used by the UI to initiate the manual execution of an age off
     * operation on the specified age off rule. The UI should call the provenance
     * service to get the list of age off rules.
     *
     *
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     * @param purgeIds This is the list of purges for which the UI is requesting
     *                detailed information
     * @returns PurgeState The state for the selected purgeIds
     */
    @Override
    public AgeOffEventInfo beginManualAgeOff(EzSecurityToken token, long ruleId) throws EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, TException {
       return executeAgeOff(token, ruleId, false);
    }

    /* updatePurge()
     * This method is called by the individual applications services that actually purge
     * data to inform the Central Purge Service of completion or error of a purge.
     *
     * The individual applications should call this method if their purge status changes
     * to one of:
     *        STOPPING,
     *        ERROR,
     *        FINISHED_COMPLETE,
     *        FINISHED_INCOMPLETE
     *
     * The Central Purge Service should also poll the state of the individual applications
     * periodically for this information.
     *
     * Keep in mind that to an application service there is no difference between an ageOff
     * event and a manual purge. So this method must handle both cases.
     * 
     * @param token The token must be validated and requests from ANY APPLICATION
     *              should be allowed. If the token is not valid, generate a
     *              EzSecurityTokenException.
     *              The calling application’s security ID can be found in this
     *              token to identify the calling application.
     * @param state This is the purge state that the application is updating.
     */
    @Override
    public void updatePurge(EzSecurityToken token, PurgeState inputPurgeState,String applicationName, String serviceName) throws EzSecurityTokenException, TException {
        DBObject dbObject=null;
        Map<String, ApplicationPurgeState> appStatesMap = null;
        CentralPurgeState centralPurgeState=null;
        CentralAgeOffEventState centralAgeOffEventState=null;
        Set<Long> centralCompletelyPurgedSet = null;
        Set<Long> centralToBePurgedSet = null;
        DBCollection purgeColl = null;
        DBCollection ageOffColl = null;
        Mongo mongoClient = null;


        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), token)
                .arg("event", "update purge")
                .arg("purgeId",inputPurgeState.getPurgeId())
                .arg("service name",serviceName)
                .arg("application name",applicationName);

        ThriftClientPool pool = null;
        try {
            pool = new ThriftClientPool(configuration);
            securityClient.validateReceivedToken(token);
            // Validates that the application that is calling update purge is allowed to update for the passed appName
            String securityId = "";
            EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(token);
            // note: if the  centralPurgeService is calling then any appName can be updated
            if(wrapper.getSecurityId().equals(purgeAppSecurityId)){
                securityId = purgeAppSecurityId;
            } else {
                securityId = pool.getSecurityId(getSecurityName(applicationName,serviceName));
            }

            if (!securityId.equals(wrapper.getSecurityId())) {
                throw new EzSecurityTokenException("The security id for the token does match the applicationName passed");
            }

            // Get access to the ageoff collection within Mongo
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            Long purgeId = inputPurgeState.getPurgeId();

            // Attempt to get the CentralPurgeState
            BasicDBObject query= new BasicDBObject(EzCentralPurgeServiceHelpers.PurgeId, purgeId);
            DBCursor cursor = purgeColl.find(query);

            boolean ageOff = false;
            CentralPurgeStatus centralPurgeStatus;

            // Check to see if the id passed corresponds to a purge event
            if(cursor.hasNext()){
                //Set the map of application states and the set of ids to purge
                dbObject=cursor.next();
                centralPurgeState = decodeCentralPurgeState((DBObject) dbObject.get(CentralPurgeStateString));
                appStatesMap= centralPurgeState.getApplicationStates();

                PurgeInfo purgeInfo = centralPurgeState.getPurgeInfo();
                centralCompletelyPurgedSet = purgeInfo.getPurgeDocumentIds();
                centralToBePurgedSet = purgeInfo.getPurgeDocumentIds();
                centralPurgeStatus = centralPurgeState.getCentralStatus();
            }
            else{
                query= new BasicDBObject(EzCentralPurgeServiceHelpers.AgeOffEventId, purgeId);
                // If it doesn't exist as a purge, check to see if it is an ageOffEvent
                cursor = ageOffColl.find(query);

                if(cursor.hasNext()) {
                    //Set the map of application states and the set of ids to purge
                    dbObject=cursor.next();
                    centralAgeOffEventState = decodeCentralAgeOffEventState((DBObject) dbObject.get(CentralAgeOffStateString));
                    appStatesMap= centralAgeOffEventState.getApplicationStates();
                    AgeOffEventInfo ageOffEventInfo = centralAgeOffEventState.getAgeOffEventInfo();
                    centralToBePurgedSet = ageOffEventInfo.getPurgeSet();
                    centralCompletelyPurgedSet = ageOffEventInfo.getPurgeSet();
                    centralPurgeStatus = centralAgeOffEventState.getCentralStatus();
                    ageOff=true;
                }
                else{
                    throw new CentralPurgeServiceException("No purge with purgeId:" + purgeId);
                }
            }

            ServicePurgeState servicePurgeState = null;
            Map<String, ServicePurgeState> servicePurgeStatesMap = null;
            ApplicationPurgeState applicationPurgeState = null;
            // Gets the mongoDB entry for the service that is updating it's purge status.
            try {
                applicationPurgeState = appStatesMap.get(applicationName);
                servicePurgeStatesMap = applicationPurgeState.getServicePurgestates();
                servicePurgeState = servicePurgeStatesMap.get(serviceName);
                if(servicePurgeState == null){
                    throw new NullPointerException("Failed to find ["+applicationName+"_"+serviceName+"] for purgeId"+inputPurgeState.getPurgeId()+" to update");
                }
            } catch( NullPointerException e){
                throw e;
            }
            // Update the ServicePurgeState and put it back
            servicePurgeState.setTimeLastPoll(getCurrentDateTime());
            servicePurgeState.setPurgeState(inputPurgeState);
            servicePurgeStatesMap.put(serviceName,servicePurgeState);
            appStatesMap.put(applicationName, applicationPurgeState);
            boolean interventionNeeded = false;
            boolean stopped = true;
            Set<Long> servicePurged;


            /* These nested loops check each service to get an update of the CompletelyPurgedSet, see if any purge
             * service is still running and if manual intervention is/will be needed.
             */
            // Loop through all apps
            for (String appNameIter : appStatesMap.keySet()){
                ApplicationPurgeState applicationPurgeStateInner = appStatesMap.get(appNameIter);
                Map<String, ServicePurgeState> servicePurgeStates = applicationPurgeStateInner.getServicePurgestates();

                //Loop through all services
                for(String serviceNameIter : servicePurgeStates.keySet()) {
                    PurgeState applicationServicePurgeState = servicePurgeStates.get(serviceNameIter).getPurgeState();
                    servicePurged = applicationServicePurgeState.getPurged();
                    applicationServicePurgeState.getPurged().removeAll(applicationServicePurgeState.getNotPurged());

                    //update based on current service
                    centralCompletelyPurgedSet = Sets.intersection(centralCompletelyPurgedSet,servicePurged);
                    if(serviceStillRunning(applicationServicePurgeState.getPurgeStatus())){
                        stopped = false;
                    }
                    if (!(applicationServicePurgeState.getNotPurged().isEmpty())){
                        interventionNeeded = true;
                    }
                }
            }

            // If all of the ids that needed to be purged have been purged then it resolved automatically
            boolean resolved = false;
            if (centralCompletelyPurgedSet.containsAll(centralToBePurgedSet)){
                resolved = true;
                centralPurgeStatus = CentralPurgeStatus.RESOLVED_AUTOMATICALLY;
            }
            // If one of the services has a document that couldn't be
            // automatically resolved, manual intervention is needed
            if(centralPurgeStatus!=CentralPurgeStatus.RESOLVED_MANUALLY && centralPurgeStatus!=CentralPurgeStatus.RESOLVED_AUTOMATICALLY){
                if(interventionNeeded) {
                    if (stopped) {
                        centralPurgeStatus = CentralPurgeStatus.STOPPED_MANUAL_INTERVENTION_NEEDED;
                    } else {
                        centralPurgeStatus = CentralPurgeStatus.ACTIVE_MANUAL_INTERVENTION_WILL_BE_NEEDED;
                    }
                }
            } else {
                resolved = true;
            }

            if(ageOff==false) {
                // If it is a purge event, update the CentralPurgeState in MongoDB
                centralPurgeState.setApplicationStates(appStatesMap);
                centralPurgeState.setCentralStatus(centralPurgeStatus);

                dbObject.put(CentralPurgeStateString, encodeCentralPurgeState(centralPurgeState));
                purgeColl.update(query, dbObject,true,false);

                // Also need to update the purge in the ProvenanceService
                ProvenanceService.Client provenanceClient=null;
                try {
                    provenanceClient = getProvenanceThriftClient(pool);
                    EzSecurityToken centralTokenForProvenance = securityClient.fetchDerivedTokenForApp(token,getProvenanceSecurityId(pool));
                    provenanceClient.updatePurge(centralTokenForProvenance,purgeId , centralCompletelyPurgedSet, null, resolved);
                    PurgeInfo purgeInfo = provenanceClient.getPurgeInfo(centralTokenForProvenance,purgeId);
                    centralPurgeState.setPurgeInfo(purgeInfo);
                    updateCentralPurgeState(centralPurgeState,purgeId);
                } finally {
                    if(provenanceClient!=null)
                        returnClientToPool(provenanceClient,pool);
                }
            }
            else{
                // If it is an ageOffEvent, update the CentralAgeOffState in MongoDB
                centralAgeOffEventState.setApplicationStates(appStatesMap);
                centralAgeOffEventState.setCentralStatus(centralPurgeStatus);

                AgeOffEventInfo ageOffEventInfo = centralAgeOffEventState.getAgeOffEventInfo();
                ageOffEventInfo.setCompletelyPurgedSet(centralCompletelyPurgedSet);
                ageOffEventInfo.setResolved(resolved);
                centralAgeOffEventState.setAgeOffEventInfo(ageOffEventInfo);
                dbObject.put(CentralAgeOffStateString, encodeCentralAgeOffEventState(centralAgeOffEventState));
                ageOffColl.update(query, dbObject,true,false);

                // If there are ids aged by all services then tell the provenance service
                if(!centralCompletelyPurgedSet.isEmpty()) {
                    ProvenanceService.Client provenanceClient = null;
                    try {
                        provenanceClient = getProvenanceThriftClient(pool);
                        EzSecurityToken centralTokenForProvenance = securityClient.fetchDerivedTokenForApp(token,getProvenanceSecurityId(pool));
                        provenanceClient.markDocumentAsAged(centralTokenForProvenance, centralCompletelyPurgedSet);
                    } finally {
                        if (provenanceClient != null)
                            returnClientToPool(provenanceClient,pool);
                    }
                }
            }
            evt.arg("status",servicePurgeState.getPurgeState().getPurgeStatus().name());
            logger.info("["+applicationName+"_"+serviceName+"] purgeId:"+inputPurgeState.getPurgeId()+" purgedIds:"+inputPurgeState.getPurged()+" status:"+inputPurgeState.getPurgeStatus());
        } catch (CentralPurgeServiceException e){
            logError(e,evt,e.getMessage());
            throw e;
        } catch (NullPointerException e){
            logError(e,evt,"CentralPurgeService encountered an exception in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in updatePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            if (pool!=null)
                pool.close();
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if(mongoClient!=null)
                mongoClient.close();
        }
    }

    /* resolvePurge()
     * This method allows the UI to mark a a purge as being resolved and supply a note
     * regarding  the resolution of the purge.

     * Manual resolution of a purge through the UI could happen for a couple reasons…
     * - Not all documents could be automatically purged, so an admin is marking
     *   successful completion of the purge.
     * - One application is not properly responding to a purge and admin interaction
     *   is required
     *
     * When this method is called, the Central Purge Service should make final updates
     * to the state of the purge stored in the Provenance Service and any state regarding
     * the purge stored elsewhere. This is done via that service’s updatePurge() method.
     *
     * When calling the provenance service’s updatePurge() method, the note should be
     * included and resolved should be set to True.
     *
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     * @param purgId This is the purge that is being marked as resolved
     * @param notes Notes as to why this purge was manually resolved.
     */
    @Override
    public void resolvePurge(EzSecurityToken token, long purgeId, String notes) throws EzSecurityTokenException, TException {
        ProvenanceService.Client provenanceClient= null;
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), token)
                .arg("event", "resolve purge")
                .arg("purgeId",purgeId)
                .arg("notes",notes);

        ThriftClientPool pool = null;
        try {
            pool = new ThriftClientPool(configuration);
            validateCentralPurgeSecurityToken(token);
            EzSecurityToken centralTokenForProvenance = securityClient.fetchDerivedTokenForApp(token,getProvenanceSecurityId(pool));

            // Get the centralPurgeState
            CentralPurgeState centralPurgeState = getCentralPurgeState(purgeId);
            if(centralPurgeState==null)
                throw new CentralPurgeServiceException("Did not find a purge with purgeID "+purgeId);
            CentralPurgeStatus centralStatus = centralPurgeState.getCentralStatus();
            if(centralStatus==CentralPurgeStatus.RESOLVED_AUTOMATICALLY || centralStatus==CentralPurgeStatus.RESOLVED_MANUALLY)
                throw new CentralPurgeServiceException("The purge with purgeID "+purgeId+" has already been resolved");
            PurgeInfo purgeInfo = centralPurgeState.getPurgeInfo();

            // Update the purge in the provenance client
            provenanceClient = getProvenanceThriftClient(pool);
            notes = ", Manually resolved with note: "+notes;
            provenanceClient.updatePurge(centralTokenForProvenance,purgeId,purgeInfo.getCompletelyPurgedDocumentIds(), notes, true);
            purgeInfo = provenanceClient.getPurgeInfo(centralTokenForProvenance,purgeId);

            // Update the purge in Mongo
            centralPurgeState.setCentralStatus(CentralPurgeStatus.RESOLVED_MANUALLY);
            centralPurgeState.setPurgeInfo(purgeInfo);

            // Cancel all services that are still running the purge
            Map<String, ApplicationPurgeState> appMap = centralPurgeState.getApplicationStates();
            appMap=cancelServices(token, appMap,purgeId,evt);
            centralPurgeState.setApplicationStates(appMap);

            updateCentralPurgeState(centralPurgeState, purgeId);
        } catch (CentralPurgeServiceException e){
            logError(e,evt,e.getMessage());
            throw e;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in resolvePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in resolvePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in resolvePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in resolvePurge:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            if (pool!=null)
                pool.close();
            if (provenanceClient!=null)
                returnClientToPool(provenanceClient,pool);
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
        }

    }

    /* getAgeOffEventState()
     * This method is used by the UI to get the detailed status of a list of ageOff
     * event IDs. The UI can fetch the list of all ageOff event IDs by calling
     * the getAllAgeOffEvents() method.
     *
     * The UI can achieve paging by fetching the entire list from the
     * getAllAgeOffEvents() and then only requesting a slice of that list
     * from this method.
     *
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     * @param purgIds This is the list of purges for which the UI is requesting
     *                detailed information
     * @returns List<AgeOffEventInfo> The list of info for the selected ageOffEventIds
     */
    @Override
    public List<CentralAgeOffEventState> getAgeOffEventState(EzSecurityToken token, List<Long> ageOffEventIds) throws EzSecurityTokenException, TException {

        DBCollection ageOffColl = null;
        Mongo mongoClient = null;
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), token)
                .arg("event", "get age off event state")
                .arg("age off event ids", ageOffEventIds);
        try {
            validateCentralPurgeSecurityToken(token);

            // Get access to the ageoff collection within Mongo

            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            List<CentralAgeOffEventState> result = new ArrayList<>();
            BasicDBObject query;
            DBCursor cursor;

            // Just need to get and decode the CentralAgeOffStates matching the ids from MongoDB
            query= new BasicDBObject(EzCentralPurgeServiceHelpers.AgeOffEventId,new BasicDBObject("$in",ageOffEventIds));
            cursor = ageOffColl.find(query);
            for (DBObject dbObject : cursor) {
                CentralAgeOffEventState centralAgeOffEventState = decodeCentralAgeOffEventState((DBObject) dbObject.get(CentralAgeOffStateString));
                result.add(centralAgeOffEventState);
            }

            return result;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in getAgeOffEventState:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in getAgeOffEventState:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in getAgeOffEventState:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in getAgeOffEventState:["+e.getClass().getName()+":" +e.getMessage()+"]");
        }  finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if (mongoClient !=null)
                mongoClient.close();
        }
    }

    /* getAllAgeOffEvents()
     * @param token The token must be validated and only requests from within the
     *              this same application should be allowed. Others should throw
     *              EzSecurityTokenException.
     *
     * @returns This method should find all AgeOffEvents and return the ageOffEvent id for
     * each
     */
    @Override
    public List<Long> getAllAgeOffEvents(EzSecurityToken token) throws TException {
        List<Long> result = null;
        DBCollection ageOffColl = null;
        Mongo mongoClient =null;

        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), token)
                .arg("event", "get all age off events");
        try {
            validateCentralPurgeSecurityToken(token);

            // Get access to the ageoff collection within Mongo

            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            // Just get all ids from MongoDB
            result = new ArrayList<Long>();
            DBCursor cursor = ageOffColl.find();
            while(cursor.hasNext()) {
                result.add((Long) cursor.next().get(EzCentralPurgeServiceHelpers.AgeOffEventId));
            }

            return result;

        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in getAllAgeOffEvents:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in getAllAgeOffEvents:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in getAllAgeOffEvents:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in getAllAgeOffEvents:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if (mongoClient != null)
                mongoClient.close();
        }
    }

   /* resolveAgeOffEvent()
    * This method allows the UI to mark an ageOffEvent as being resolved and supply a note
    * regarding  the resolution of the purge.

    * Manual resolution of an ageOffEvent through the UI could happen for a couple reasons
    * - Not all documents could be automatically purged, so an admin is marking
    *   successful completion of the ageOffEvent.
    * - One application is not properly responding to a purge and admin interaction
    *   is required
    *
    * When this method is called, the Central Purge Service should make final updates
    * to any state regarding the ageOffEvent stored elsewhere. This is done via that
    * service’s updatePurge() method.
    *
    * When calling the provenance service’s updatePurge() method, the note should be
    * included and resolved should be set to True.
    *
    * @param token The token must be validated and only requests from within the
    *              this same application should be allowed. Others should throw
    *              EzSecurityTokenException.
    * @param purgId This is the purge that is being marked as resolved
    * @param notes Notes as to why this purge was manually resolved.
    */
    @Override
    public void resolveAgeOffEvent(EzSecurityToken token, long ageOffEventId, String notes) throws EzSecurityTokenException, TException {

        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), token)
                .arg("event", "resolve age off event")
                .arg("age off event id",ageOffEventId)
                .arg("notes",notes);

        try {
            validateCentralPurgeSecurityToken(token);

            // Get and update the CentralAgeOffStates from MongoDB
            CentralAgeOffEventState centralAgeOffEventState = getCentralAgeOffEventState(ageOffEventId);
            centralAgeOffEventState.setCentralStatus(CentralPurgeStatus.RESOLVED_MANUALLY);
            AgeOffEventInfo ageOffEventInfo = centralAgeOffEventState.getAgeOffEventInfo();
            notes = ageOffEventInfo.getDescription()+", Manually resolved with note: "+notes;
            ageOffEventInfo.setDescription(notes);
            ageOffEventInfo.setResolved(true);
            centralAgeOffEventState.setAgeOffEventInfo(ageOffEventInfo);

            // Cancel all services that are still running the purge
            Map<String, ApplicationPurgeState> appMap = centralAgeOffEventState.getApplicationStates();
            appMap = cancelServices(token, appMap, ageOffEventId,evt);
            centralAgeOffEventState.setApplicationStates(appMap);

            updateCentralAgeOffEventState(centralAgeOffEventState, ageOffEventId);
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in resolveAgeOffEvent:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in resolveAgeOffEvent:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in resolveAgeOffEvent:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in resolveAgeOffEvent:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
        }
    }

    @Override
    public CentralPurgeQueryResults getPagedSortedFilteredPurgeStates( EzSecurityToken token, List<CentralPurgeStatus> statuses, int pageNum, int numPerPage) throws EzSecurityTokenException, CentralPurgeServiceException{

        DBCollection purgeColl = null;
        Mongo mongoClient = null;
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), token)
                .arg("event", "getPagedSortedFilteredPurgeStates")
                .arg("statuses", statuses)
                .arg("pageNum", pageNum)
                .arg("numPerPage", numPerPage);
        try {
            validateCentralPurgeSecurityToken(token);

            // Get access to the purge collection within Mongo
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);

            List<CentralPurgeState> result = new ArrayList<>();
            List<Integer> statusesValues = new LinkedList<>();
            for(CentralPurgeStatus status: statuses){
                statusesValues.add(status.getValue());
            }


            // Gets all centralPurgeStates that are in the statuses and pages
            BasicDBObject query = new BasicDBObject(EzCentralPurgeServiceHelpers.CentralPurgeStateString+"."+EzCentralPurgeServiceHelpers.CentralPurgeStatusString, new BasicDBObject("$in", statusesValues));
            DBCursor cursor = purgeColl.find(query).sort(new BasicDBObject(PurgeId,-1)).skip(pageNum > 0 ? ((pageNum - 1) * numPerPage) : 0).limit(numPerPage);

            // Decodes each centralPurgeState from how it's stored in MongoDB and adds it to the return variable
            for (DBObject dbObject : cursor) {
                CentralPurgeState centralPurgeState = decodeCentralPurgeState((DBObject) dbObject.get(CentralPurgeStateString));
                result.add(centralPurgeState);
            }
            CentralPurgeQueryResults centralPurgeQueryResults = new CentralPurgeQueryResults();
            centralPurgeQueryResults.setPurgeStates(result);
            centralPurgeQueryResults.setCount(purgeColl.count(query));
            return centralPurgeQueryResults;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in getPagedSortedFilteredPurgeStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in getPagedSortedFilteredPurgeStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in getPagedSortedFilteredPurgeStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in getPagedSortedFilteredPurgeStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if(mongoClient!=null)
                mongoClient.close();
        }
    }

    @Override
    public CentralAgeOffEventQueryResults getPagedSortedFilteredAgeOffEventStates(EzSecurityToken token, List<CentralPurgeStatus> statuses,int pageNum, int numPerPage) throws EzSecurityTokenException, CentralPurgeServiceException{
        DBCollection ageOffColl = null;
        Mongo mongoClient = null;
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), token)
                .arg("event", "getPagedSortedFilteredAgeOffEventStates")
                .arg("statuses", statuses)
                .arg("pageNum", pageNum)
                .arg("numPerPage", numPerPage);
        try {
            validateCentralPurgeSecurityToken(token);

            // Get access to the ageoff collection within Mongo

            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            List<CentralAgeOffEventState> result = new ArrayList<>();
            List<Integer> statusesValues = new LinkedList<>();
            for(CentralPurgeStatus status: statuses){
                statusesValues.add(status.getValue());
            }


            // Gets all centralPurgeStates that are in the statuses and pages
            BasicDBObject query = new BasicDBObject(EzCentralPurgeServiceHelpers.CentralAgeOffStateString+"."+EzCentralPurgeServiceHelpers.CentralPurgeStatusString, new BasicDBObject("$in", statusesValues));
            DBCursor cursor = ageOffColl.find(query).sort(new BasicDBObject(AgeOffEventId,-1)).skip(pageNum > 0 ? ((pageNum - 1) * numPerPage) : 0).limit(numPerPage);

            for (DBObject dbObject : cursor) {
                CentralAgeOffEventState centralAgeOffEventState = decodeCentralAgeOffEventState((DBObject) dbObject.get(CentralAgeOffStateString));
                result.add(centralAgeOffEventState);
            }
            CentralAgeOffEventQueryResults centralAgeOffEventQueryResults = new CentralAgeOffEventQueryResults();
            centralAgeOffEventQueryResults.setAgeOffEventStates(result);
            centralAgeOffEventQueryResults.setCount(ageOffColl.count(query));
            return centralAgeOffEventQueryResults;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to validate token:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in getPagedSortedFilteredAgeOffEventStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in getPagedSortedFilteredAgeOffEventStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in getPagedSortedFilteredAgeOffEventStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in getPagedSortedFilteredAgeOffEventStates:["+e.getClass().getName()+":" +e.getMessage()+"]");
        }  finally {
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
            if (mongoClient !=null)
                mongoClient.close();
        }
    }

    // This method is used by resolvePurge and resolveAgeOff event to  cancel the services still running that purge
    private Map<String,ApplicationPurgeState> cancelServices(EzSecurityToken token,Map<String,ApplicationPurgeState> appMap,long id,AuditEvent evt) throws EzSecurityTokenException {

        for(String appName: appMap.keySet()) {
            try{
                ApplicationPurgeState applicationPurgeState = appMap.get(appName);
                Map<String,ServicePurgeState> servicePurgeStateMap = applicationPurgeState.getServicePurgestates();

                for (String serviceName : servicePurgeStateMap.keySet()) {
                    EzBakeBasePurgeService.Client individualServicePurgeClient= null;
                    ThriftClientPool pool = null;
                    try {
                        pool = new ThriftClientPool(configuration);
                        ServicePurgeState servicePurgeState = servicePurgeStateMap.get(serviceName);
                        PurgeStatus status = servicePurgeState.getPurgeState().getPurgeStatus();

                        if (serviceStillRunning(status)) {
                            EzSecurityToken centralTokenForServices = null;
                            centralTokenForServices = securityClient.fetchDerivedTokenForApp(token, pool.getSecurityId(getSecurityName(appName,serviceName)));

                            if(centralTokenForServices == null){
                                throw new PurgeException("Failed when getting a token targeting "+appName+"_"+serviceName);
                            }
                            individualServicePurgeClient = getServicePurgeThriftClient(appName, serviceName,pool);
                            servicePurgeState.setPurgeState(individualServicePurgeClient.cancelPurge(centralTokenForServices, id));

                            servicePurgeStateMap.put(serviceName, servicePurgeState);
                        }
                    } catch (Exception e) {
                        logError(e, evt, "CentralPurgeService failed when trying to cancel:" + appName + "_" + serviceName + " [" + e.getClass().getName() + ":" + e.getMessage() + "]");
                        e.printStackTrace();
                    } finally {
                        if (pool!=null)
                            pool.close();
                        if (individualServicePurgeClient != null) {
                            returnClientToPool(individualServicePurgeClient,pool);
                        }
                    }
                }
                appMap.put(appName,applicationPurgeState);
            } catch (Exception e) {
                logError(e, evt, "CentralPurgeService failed when trying to cancel:" + appName +" [" + e.getClass().getName() + ":" + e.getMessage() + "]");
                e.printStackTrace();
            }
        }
        return appMap;
    }

    // This event is run when an ageOffEvent or Purge is started. It starts the purge on all services
    private  Map<String,ApplicationPurgeState> servicePurger(EzSecurityToken token, Long id, Set<Long> purgeSet, Map<String,ApplicationPurgeState> appMap,  boolean synchronous, CentralPurgeType centralPurgeType, AuditEvent evt) throws Exception {
        EzBakeBasePurgeService.Client individualServicePurgeClient= null;

        try {
            // Get a map of every application to it's respective service
            ServicePurgeClient serviceDiscoveryPurgeClient = new ServicePurgeClient(configuration.getProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING));
            Multimap<String, String> allAppMap= serviceDiscoveryPurgeClient.getPurgeServices();
            Set<String> appNames = allAppMap.keySet();

            // Iterate through each app
            for(String appName: appNames) {
                try {
                    Collection<String> services = allAppMap.get(appName);
                    ApplicationPurgeState applicationPurgeState = new ApplicationPurgeState();
                    Map<String, ServicePurgeState> servicePurgeStateMap = new HashMap<>();
                    // Iterate through the services for that app
                    for (String serviceName : services) {
                        ThriftClientPool pool = null;
                        try {
                            pool = new ThriftClientPool(configuration);
                            EzSecurityToken centralTokenForServices = securityClient.fetchDerivedTokenForApp(token, pool.getSecurityId(getSecurityName(appName,serviceName)));

                            if(centralTokenForServices == null){
                                throw new PurgeException("Failed when getting a token targeting "+appName+"_"+serviceName);
                            }
                            individualServicePurgeClient = getServicePurgeThriftClient(appName, serviceName,pool);
                            PurgeState purgeState = null;
                            EzSecurityTokenWrapper ezSecurityTokenWrapper = new EzSecurityTokenWrapper(centralTokenForServices);
                            logger.info("App name passing for derivedToken:"+appName+" (it's securityID:"+pool.getSecurityId(appName)+") Token details: target:"+ezSecurityTokenWrapper.getTargetSecurityId()+" tokenId:"+ezSecurityTokenWrapper.getSecurityId()+" username"+ezSecurityTokenWrapper.getUsername());
                            // Call the service for the respective purge type
                            switch (centralPurgeType) {
                                case NORMAL:
                                    purgeState = individualServicePurgeClient.beginPurge(EZBAKE_BASE_PURGE_SERVICE_NAME, id, purgeSet, centralTokenForServices);
                                    break;
                                case VIRUS:
                                    purgeState = individualServicePurgeClient.beginVirusPurge(EZBAKE_BASE_PURGE_SERVICE_NAME, id, purgeSet, centralTokenForServices);
                            }
                            DateTime timeStamp = purgeState.getTimeStamp();
                            ServicePurgeState servicePurgeState = new ServicePurgeState();
                            servicePurgeState.setPurgeState(purgeState);
                            servicePurgeState.setTimeInitiated(timeStamp);
                            servicePurgeState.setTimeLastPoll(timeStamp);

                            // Should the thread wait until the service stops running?
                            if (!synchronous) {
                                // Add the service to the queue for updates
                                DelayedServicePurgeState delayedServicePurgeState = new DelayedServicePurgeState(servicePurgeState, appName, serviceName);
                                servicePollDelayQueue.offer(delayedServicePurgeState);
                                servicePurgeStateMap.put(serviceName, servicePurgeState);
                            } else {
                                // Wait for the service to stop purging, getting updates every suggestPollPeriod seconds
                                int suggestedPollPeriod = purgeState.getSuggestedPollPeriod();
                                while (serviceStillRunning(purgeState.getPurgeStatus())) {
                                    try {
                                        TimeUnit.MILLISECONDS.sleep(suggestedPollPeriod);
                                    } catch (InterruptedException ex) {
                                        Thread.currentThread().interrupt();
                                    }


                                    centralTokenForServices = securityClient.fetchDerivedTokenForApp(token, pool.getSecurityId(getSecurityName(appName,serviceName)));

                                    if(centralTokenForServices == null){
                                        throw new PurgeException("Failed when getting a token targeting "+appName+"_"+serviceName);
                                    }
                                    purgeState = individualServicePurgeClient.purgeStatus(centralTokenForServices, id);
                                    updatePurge(token, purgeState, appName, serviceName);
                                }
                            }
                        } catch (EzSecurityTokenException e) {
                            logError(e, evt, "CentralPurgeService failed when trying to get a token to purge " + appName + "_" + serviceName + ":[" + e.getClass().getName() + ":" + e.getMessage() + "]");
                        } catch (PurgeException e) {
                            logError(e, evt, "CentralPurgeService failed when trying to purge " + appName + "_" + serviceName + ":[" + e.getClass().getName() + ":" + e.getMessage() + "]");
                        } catch (TException e) {
                            logError(e, evt, "CentralPurgeService failed when trying to purge " + appName + "_" + serviceName + ":[" + e.getClass().getName() + ":" + e.getMessage() + "]");
                        } catch (Exception e){
                            logError(e, evt, "CentralPurgeService failed when trying to purge " + appName + "_" + serviceName + ":[" + e.getClass().getName() + ":" + e.getMessage() + "]");
                        } finally {
                            if (individualServicePurgeClient != null)
                                returnClientToPool(individualServicePurgeClient,pool);
                            if (pool!=null)
                                pool.close();
                        }
                        applicationPurgeState.setServicePurgestates(servicePurgeStateMap);
                    }
                    appMap.put(appName, applicationPurgeState);
                } catch (Exception e){
                    logError(e, evt, "CentralPurgeService failed when trying to get a token to purge " + appName + ":[" + e.getClass().getName() + ":" + e.getMessage() + "]");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new CentralPurgeServiceException(e.getMessage());
        }
        return appMap;
    }

    // A helper method for beginManualAgeOff to add a synchronous argument
    private AgeOffEventInfo executeAgeOff(EzSecurityToken token, long ruleId, boolean synchronous) throws TException {

        ProvenanceService.Client client= null;
        AgeOffEventInfo ageOffEventInfo = null;

        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), token)
                .arg("ruleId", ruleId)
                .arg("synchronous", synchronous);
        if(synchronous){
            evt.arg("event", "ran age off event");
        } else{
            evt.arg("event", "starting age off event");
        }
        ThriftClientPool pool = null;
        try {
            pool = new ThriftClientPool(configuration);
            validateCentralPurgeSecurityToken(token);
            EzSecurityToken centralTokenForProvenance = securityClient.fetchDerivedTokenForApp(token,getProvenanceSecurityId(pool));

            // Start the ageOffEvent in the provenance service
            client=getProvenanceThriftClient(pool);
            AgeOffInitiationResult ageOffInitiationResult = client.startAgeOffEvent(centralTokenForProvenance, ruleId, null);
            AgeOffRule ageOffRule = client.getAgeOffRuleById(centralTokenForProvenance,ruleId);

            // Initialize ageOffEvent fields
            ageOffEventInfo = new AgeOffEventInfo();
            Set<Long> completelyPurgedSet = new HashSet<>();
            Set<Long> purgeSet = ageOffInitiationResult.getAgeOffDocumentIds();
            DateTime dateTime= getCurrentDateTime();
            ageOffEventInfo.setTimeCreated(dateTime);
            ageOffEventInfo.setCompletelyPurgedSet(completelyPurgedSet);
            ageOffEventInfo.setId(ageOffInitiationResult.getAgeOffId());
            ageOffEventInfo.setPurgeSet(purgeSet);
            ageOffEventInfo.setDescription("Automatic AgeOffEvent for ruleId:"+ruleId+"; AgeOffRule user:["+ageOffRule.getUser()+"] AgeOffRule name:["+ageOffRule.getName()+"]");
            evt.arg("ageOffId",ageOffInitiationResult.getAgeOffId());

            // If a user started the ageOff then put user's name into the user field, else use the app name
            if(token.getType()==TokenType.USER) {
                ageOffEventInfo.setUser(token.getTokenPrincipal().getPrincipal());
            }
            else {
                ageOffEventInfo.setUser(token.getTokenPrincipal().getName());
            }

            // If there are no documents that need to be aged off then the event is resolved
            boolean resolved = purgeSet.isEmpty();
            if(resolved){
                ageOffEventInfo.setResolved(true);
            } else {
                ageOffEventInfo.setResolved(false);
            }

            // Initialize and set the CentralAgeOffEventState
            CentralAgeOffEventState centralAgeOffEventState = new CentralAgeOffEventState();
            Map<String, ApplicationPurgeState> appStates = initializeApplicationState(ageOffInitiationResult.getAgeOffId());
            centralAgeOffEventState.setApplicationStates(appStates);
            centralAgeOffEventState.setAgeOffEventInfo(ageOffEventInfo);
            centralAgeOffEventState.setAgeOffRuleId(ruleId);
            if (resolved){
                centralAgeOffEventState.setCentralStatus(CentralPurgeStatus.RESOLVED_AUTOMATICALLY);
            } else{
                centralAgeOffEventState.setCentralStatus(CentralPurgeStatus.ACTIVE);
            }
            updateCentralAgeOffEventState(centralAgeOffEventState, ageOffInitiationResult.getAgeOffId());

            // If the ageOffEvent is already resolved there is no need to run a purge
            if(!resolved) {
                if (synchronous) {
                    // If it is synchronous then run the purge and update the values
                    appStates = servicePurger(token, ageOffEventInfo.getId(), ageOffEventInfo.getPurgeSet(), appStates, synchronous, CentralPurgeType.NORMAL,evt);
                    //centralAgeOffEventState=getCentralAgeOffEventState(ageOffInitiationResult.getAgeOffId());
                    //centralAgeOffEventState.setApplicationStates(appStates);
                    //updateCentralAgeOffEventState(centralAgeOffEventState, ageOffInitiationResult.getAgeOffId());
                } else {
                    // If not start a thread running the AgeOffEventPurger
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    executorService.execute(new AgeOffEventPurger(ageOffInitiationResult, token, evt));
                    executorService.shutdown();
                }
            }

        } catch (ProvenanceAgeOffRuleNotFoundException e) {
            logError(e,evt,"CentralPurgeService failed when trying to ageOff"+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (EzSecurityTokenException e){
            logError(e,evt,"CentralPurgeService failed when trying to get a token in executeAgeOff:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw e;
        } catch (UnknownHostException e) {
            logError(e,evt,"CentralPurgeService unable to reach MongoDB in beginAgeOff:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService unable to reach MongoDB in beginAgeOff:["+e.getClass().getName()+":" +e.getMessage()+"]");
        } catch (Exception e){
            logError(e,evt,"CentralPurgeService encountered an exception in beginAgeOff:["+e.getClass().getName()+":" +e.getMessage()+"]");
            throw new CentralPurgeServiceException("CentralPurgeService encountered an exception in beginAgeOff:["+e.getClass().getName()+":" +e.getMessage()+"]");
        }  finally {
            if(client!=null)
                returnClientToPool(client,pool);
            auditLogger.logEvent(evt);
            logEventToPlainLogs(logger,evt);
        }
        return ageOffEventInfo;
    }

    // Gets a centralAgeOffEventState from mongoDB
    private CentralAgeOffEventState getCentralAgeOffEventState(Long ageOffId) throws UnknownHostException {

        Mongo mongoClient = null;
        DBCollection ageOffColl = null;
        try {
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            //Get the centralAgeOffEventState and return it
            BasicDBObject query = new BasicDBObject(EzCentralPurgeServiceHelpers.AgeOffEventId, ageOffId);
            DBCursor cursor = ageOffColl.find(query);
            DBObject dbObject = null;
            if (cursor.hasNext()) {
                dbObject = cursor.next();
            } else {
                return null;
            }
            return decodeCentralAgeOffEventState((DBObject) dbObject.get(CentralAgeOffStateString));

        } finally {
            if (mongoClient != null)
                mongoClient.close();
        }
    }

    // Updates a centralAgeOffEventState in the DB (inserts if doesn't already exist)
    private void updateCentralAgeOffEventState(CentralAgeOffEventState centralAgeOffEventState, Long ageOffId) throws UnknownHostException {

        DBCollection ageOffColl = null;
        Mongo mongoClient = null;
        try {
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            ageOffColl = mongoDB.getCollection(AGEOFF_COLLECTION);

            // Update the state if it exists, insert if not
            BasicDBObject query = new BasicDBObject(EzCentralPurgeServiceHelpers.AgeOffEventId, ageOffId);
            BasicDBObject ageOffEvent = new BasicDBObject()
                    .append(AgeOffEventId, ageOffId)
                    .append(CentralAgeOffStateString, encodeCentralAgeOffEventState(centralAgeOffEventState));
            boolean upsert = true;
            boolean multiUpdate = false;
            ageOffColl.update(query, ageOffEvent, upsert, multiUpdate);
        } catch (UnknownHostException e) {
            // TODO: log that couldn't connect to MongoDB
            throw e;
        } finally {
            if(mongoClient !=null)
                mongoClient.close();
        }
    }

    private List<Long> getAllPurgeIdsInMongo() throws UnknownHostException {
        List<Long> result = null;
        DBCollection purgeColl = null;
        Mongo mongoClient = null;

        try {
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);

            // Just get all ids from MongoDB
            result = new ArrayList<Long>();
            DBCursor cursor = purgeColl.find();
            while(cursor.hasNext()) {
                result.add((Long) cursor.next().get(EzCentralPurgeServiceHelpers.PurgeId));
            }

            return result;
        } finally {
            if(mongoClient!= null) {
                mongoClient.close();
            }
        }
    }

    // Gets a centralPurgeState from mongoDB
    private CentralPurgeState getCentralPurgeState(Long purgeId) throws UnknownHostException {

        DBCollection purgeColl = null;
        Mongo mongoClient = null;
        try {
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);

            //Get the centralPurgeState and return it
            BasicDBObject query = new BasicDBObject(PurgeId, purgeId);
            DBCursor cursor = purgeColl.find(query);
            DBObject dbObject = null;
            if (cursor.hasNext()) {
                dbObject = cursor.next();
            } else {
                return null;
            }
            return decodeCentralPurgeState((DBObject) dbObject.get(CentralPurgeStateString));
        } finally {
            if(mongoClient!= null) {
                mongoClient.close();
            }
        }
    }

    // Updates a centralPurgeState in the DB (inserts if doesn't already exist)
    private void updateCentralPurgeState(CentralPurgeState centralPurgeState, Long purgeId) throws UnknownHostException {
        DBCollection purgeColl = null;
        Mongo mongoClient = null;
        try {
            MongoConfigurationHelper mongoConfigurationHelper = new MongoConfigurationHelper(configuration);
            MongoHelper mongoHelper = new MongoHelper(configuration);
            mongoClient = mongoHelper.getMongo();
            DB mongoDB = mongoClient.getDB(mongoConfigurationHelper.getMongoDBDatabaseName());
            purgeColl = mongoDB.getCollection(PURGE_COLLECTION);

            // Update the state if it exists, insert if not
            BasicDBObject query = new BasicDBObject(PurgeId, purgeId);
            BasicDBObject purgeStatus = new BasicDBObject()
                    .append(PurgeId, centralPurgeState.getPurgeInfo().getId())
                    .append(CentralPurgeStateString, encodeCentralPurgeState(centralPurgeState));
            boolean upsert = true;
            boolean multiUpdate = false;
            purgeColl.update(query, purgeStatus, upsert, multiUpdate);
        } finally {
            if(mongoClient!=null)
                mongoClient.close();
        }
    }

    // Gets a service's client, then gets a status update for the specified purge and returns the purge state
    private PurgeState getServiceClientAndUpdate(PurgeState purgeState, String appName, String serviceName) throws TException {
        EzBakeBasePurgeService.Client appPurgeClient = null;
        ThriftClientPool pool = null;
        try {
            pool = new ThriftClientPool(configuration);

            EzSecurityToken centralTokenForServices = securityClient.fetchAppToken( pool.getSecurityId(getSecurityName(appName,serviceName)));

            if(centralTokenForServices == null){
                throw new PurgeException("Failed when getting a token targeting "+appName+"_"+serviceName);
            }
            appPurgeClient = getServicePurgeThriftClient(appName, serviceName,pool);
            purgeState = appPurgeClient.purgeStatus(centralTokenForServices, purgeState.getPurgeId());
            this.updatePurge(securityClient.fetchAppToken(), purgeState, appName, serviceName);

        } finally {
            if (appPurgeClient != null)
                returnClientToPool(appPurgeClient,pool);
            if (pool!=null)
                pool.close();
        }
        return purgeState;
    }

    private void validateCentralPurgeSecurityToken(EzSecurityToken token) throws EzSecurityTokenException {
        securityClient.validateReceivedToken(token);
        EzSecurityTokenWrapper ezSecurityTokenWrapper = new EzSecurityTokenWrapper(token);
        if (!isPurgeAppSecurityId(ezSecurityTokenWrapper)) {
            logger.debug("Could not validate central purge security token:securityId=["+ezSecurityTokenWrapper.getSecurityId() +"] actual=["+this.purgeAppSecurityId+"]");
            throw new EzSecurityTokenException("Not central purge security token: securityId=["+ezSecurityTokenWrapper.getSecurityId() +"] actual=["+this.purgeAppSecurityId+"]");
        }
    }

    // This method takes gets the Service purge client for a specified application name and service name
    private  EzBakeBasePurgeService.Client getServicePurgeThriftClient(String applicationName, String serviceName, ThriftClientPool pool) throws TException {
        if(applicationName.equals(COMMON_APP_NAME))
            return pool.getClient(serviceName, EzBakeBasePurgeService.Client.class);
        return pool.getClient(applicationName, serviceName, EzBakeBasePurgeService.Client.class);
    }

    // This method gets all services currently in the system. Then creates a purgeState for each within a Map.
    private Map<String,ApplicationPurgeState> initializeApplicationState(Long purgeId) throws CentralPurgeServiceException {
        ServicePurgeClient servicePurgeClient = null;
        LinkedHashMap<String, ApplicationPurgeState> appStates = null;
        try {
            // The ServicePurgeClient gets all the purge services currently in the system.
            appStates = new LinkedHashMap<String, ApplicationPurgeState>();
            servicePurgeClient = new ServicePurgeClient(configuration.getProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING));
            Multimap<String, String> allAppMap = servicePurgeClient.getPurgeServices();

            for(String app: allAppMap.keySet()){
                ApplicationPurgeState applicationPurgeState = new ApplicationPurgeState();
                Map<String,ServicePurgeState> servicePurgeStateMap = new HashMap<String, ServicePurgeState>();

                for(String service: allAppMap.get(app)) {
                    // Create a ServicePurgeState for each service
                    ServicePurgeState servicePurgeState = new ServicePurgeState();
                    PurgeState purgeState = new PurgeState();
                    purgeState.setPurgeStatus(PurgeStatus.WAITING_TO_START);
                    purgeState.setCancelStatus(CancelStatus.NOT_CANCELED);
                    Set<Long> purgedSet = new HashSet<>();
                    Set<Long> notPurgedSet = new HashSet<>();
                    DateTime dateTime =getCurrentDateTime();
                    purgeState.setPurged(purgedSet);
                    purgeState.setNotPurged(notPurgedSet);
                    purgeState.setTimeStamp(dateTime);
                    purgeState.setPurgeId(purgeId);
                    servicePurgeState.setPurgeState(purgeState);
                    servicePurgeState.setTimeInitiated(dateTime);
                    servicePurgeState.setTimeLastPoll(dateTime);
                    servicePurgeStateMap.put(service,servicePurgeState);
                }

                // Put the new ServicePurgeState into it's respective applicationPurgeState
                applicationPurgeState.setServicePurgestates(servicePurgeStateMap);
                appStates.put(app, applicationPurgeState);
            }
        } catch (Exception e) {
            throw new CentralPurgeServiceException(e.getMessage());
        } finally {
            if(servicePurgeClient!=null) {
                servicePurgeClient.close();
            }
        }
        return appStates;

    }

    // This method just checks if a Service purge status indicates that it is still running
    private boolean serviceStillRunning(PurgeStatus status){
        return (status ==  PurgeStatus.WAITING_TO_START || status ==  PurgeStatus.STARTING || status ==  PurgeStatus.PURGING || status ==  PurgeStatus.STOPPING);
    }

    private ProvenanceService.Client getProvenanceThriftClient(ThriftClientPool pool) throws TException {
        return pool.getClient( PROVENANCE_SERVICE_NAME, ProvenanceService.Client.class);
    }

    private String getProvenanceSecurityId(ThriftClientPool pool){
        return  pool.getSecurityId(PROVENANCE_SERVICE_NAME);
    }

    private void returnClientToPool(TServiceClient client,ThriftClientPool pool) {
        pool.returnToPool(client);
    }

    public boolean ping() {
        return initialized;
    }

    private String getSecurityName(String applicationName, String serviceName){
        String securityName;
        if (applicationName.equals(COMMON_APP_NAME)) {
            securityName = serviceName;
        } else {
            securityName = applicationName;
        }

        return securityName;
    }

    /**
     * <p>
     * Answers true if the given security token has an application security id
     * that is equal to the application security id from the purge service. If
     * they are not equivalent then false is returned.
     * </p>
     *
     * @param   ezSecurityTokenWrapper The security token that is checked to determine if
     *          it is from the purge service. Required.
     * @return  True if the token has an application security id that matches
     *          purge service's application security id and false if not.
     */
    private boolean isPurgeAppSecurityId(EzSecurityTokenWrapper ezSecurityTokenWrapper) {
        return ezSecurityTokenWrapper.getSecurityId().equals(this.purgeAppSecurityId);
    }

    private void logError(Exception e, AuditEvent evt,String loggerMessage){
        evt.failed();
        e.printStackTrace();
        evt.arg(e.getClass().getName(), e);
        logger.error(loggerMessage);
    }
}