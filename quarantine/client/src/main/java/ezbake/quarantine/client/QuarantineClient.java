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

package ezbake.quarantine.client;

import com.google.common.collect.Sets;
import ezbake.base.thrift.*;
import ezbake.quarantine.thrift.*;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class QuarantineClient {

    public static final int MAX_ITEM_COUNT = 20;
    private static final Logger logger = LoggerFactory.getLogger(QuarantineClient.class);
    protected ThriftClientPool pool;
    protected EzbakeSecurityClient client;
    protected String applicationName;

    /**
     * Initializes the quarantine processor object with the provided configuration.
     * The appId is used to communicate with the data warehouse and to obtain the security
     * token.
     * The client pool is used to communicate with the security service and quarantine service
     * @param props configurations to use for initialization
     */
    public QuarantineClient(Properties props) {
        this.pool = new ThriftClientPool(props);
        this.client = new EzbakeSecurityClient(props);
        this.applicationName = new EzBakeApplicationConfigurationHelper(props).getApplicationName();
    }

    /**
     * Returns all approved objects for a given pipeline id
     * @param pipelineId the pipeline id to use
     * @return all approved objects found for that pipeline id
     * @throws IOException
     * @throws ObjectNotQuarantinedException
     */
    public List<QuarantineResult> getApprovedObjectsForPipeline(String pipelineId, int pageOffset, short pageSize)
            throws ObjectNotQuarantinedException, IOException, TException {
        logger.info("called getApprovedObjectsForPipeline");
        return getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST), pageOffset, pageSize);
    }

    /**
     * Retrieves the last event for each pipe in the given pipeline
     * @param pipelineId id of the pipeline to use
     * @return set of EventWithCount containing the number of time an event
     *         occurred along with the event itself
     */
    public Set<EventWithCount> getPipeMetaForPipeline(String pipelineId) throws IOException {
        Set<ObjectStatus> allStatuses = getAllStatuses();
        return getPipeMetaForPipeline(pipelineId, allStatuses);
    }

    /**
     * Retrieves the last event that occurred in each pipe for the provided pipeline
     * and the statuses
     * @param pipelineId the pipeline to look through
     * @param statuses the status to use for querying
     * @return Set of EventWithCount, contains the number of time an event occurred and the event object
     * @throws IOException
     */
    public Set<EventWithCount> getPipeMetaForPipeline(String pipelineId, Set<ObjectStatus> statuses) throws IOException {
        Quarantine.Client client = getClient();
        try {
            EzSecurityToken token = getToken();
            return client.getObjectCountPerPipe(pipelineId, statuses, token);
        } catch (TException e) {
            logger.debug("Could not retrieve pipes meta for pipeline from quarantine", e);
            throw new IOException("Could not retrieve pipes meta for pipeline from quarantine", e);
        } finally {
            pool.returnToPool(client);
        }
    }

    /**
     * Quarantines serializable data.  This method is visible in both worker and generator class.
     * This data is automatically replayed after approval.
     * @param pipelineId the id of the pipeline
     * @param pipeId the id of the pipe where the data came from (could be generator or worker id)
     * @param object the Serializable object to save
     * @param visibility string identifying the visibility of the this data.
     * @param error a string describing the reason for failure
     */
    protected void sendObjectToQuarantine(String pipelineId, String pipeId, Serializable object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.flush();
        oos.close();
        byte[] qData = baos.toByteArray();
        logger.info("Sending serializable data to quarantine for pipeId: {} and error msg: {}", pipeId, error);
        sendToQuarantine(pipelineId, pipeId, qData, visibility, error, true, additionalMetadata);
    }

    /**
     * Wraps the supplied parameters in a Quarantine object.
     * @param pipeId the id of the pipe where the data came from (could be generator or worker id)
     * @param data the data to save (could be serializable object or raw data specified by the @param isSerializable argument)
     * @param visibility string representing who the data should be visible to.
     * @param error string describing the reason for failure
     * @param isSerializable specifies whether the byte data is serializable or raw data.
     * @throws IOException
     */
    private void sendToQuarantine(String pipelineId, String pipeId, byte[] data, Visibility visibility,
                                  String error, boolean isSerializable, AdditionalMetadata additionalMetadata) throws IOException {
        QuarantinedObject qo = new QuarantinedObject();
        qo.setPipelineId(pipelineId);
        qo.setPipeId(pipeId);
        qo.setVisibility(visibility);
        if(data != null){
            qo.setContent(data);
        }
        qo.setSerializable(isSerializable);
        sendToQuarantine(qo, error, additionalMetadata);
    }


    /**
     * Sends a serializable object to quarantine through the Quarantine service.
     * @param qo the object to sent to quarantine
     * @param error a message describing the reason for failure
     * @throws IOException
     */
    private void sendToQuarantine(QuarantinedObject qo, String error, AdditionalMetadata additionalMetadata) throws IOException {
        logger.info("Sending data to quarantine");
        qo.setApplicationName(applicationName);
        Quarantine.Client quarantineClient = getClient();
        try {
            EzSecurityToken token = getToken();
            quarantineClient.sendToQuarantine(qo, error, additionalMetadata, token);
        } catch (TException e) {
            logger.error("Could not push data to quarantine service");
            throw new IOException(e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Quarantines raw data.  This method is visible in both worker and generator.
     * The raw data is NOT automatically replayed.
     * @param pipeId the id of the pipe where the data came from (could be generator or worker id)
     * @param data the raw data to save
     * @param visibility string identifying the visibility of the this data.
     * @param error a string describing the reason for failure
     */
    protected void sendRawToQuarantine(String pipelineId, String pipeId, byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException{
        sendToQuarantine(pipelineId, pipeId, data, visibility, error, false, additionalMetadata);
        logger.info("Sending RAW data to quarantine for pipeId: {} and error msg: {}", pipeId, error);
    }

    /**
     * Retrieves object ids and time stamp of all objects associated with provided event text
     * for a given pipe within the pipeline
     * @param pipelineId the pipeline that contains the pipe to check
     * @param pipeId the pipe that contains the event
     * @param eventText the event text to check for
     * @param status a set containing the status objects that should be returned
     * @param pageNumber the starting page for this request
     * @param pageSize the number of items to return per page
     * @return map of object ids to time stamp
     * @throws IOException
     */
    public IdsResponse getObjectsForPipeAndEvent(String pipelineId, String pipeId, String eventText,
                                                       Set<ObjectStatus> status, int pageNumber, int pageSize) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        try {
            return quarantineClient.getObjectsForPipeAndEvent(pipelineId, pipeId, eventText, status, pageNumber, pageSize, getToken());
        } catch (ObjectNotQuarantinedException e) {
            logger.error("Could not push data to quarantine service");
            throw new IOException(e.getMessage());
        } catch (TException e) {
            logger.error("Exception occurred in quarantine service", e);
            throw new IOException("Exception occurred in quarantine service", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    public long getCountPerPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses)
            throws IOException {

        Quarantine.Client quarantineClient = getClient();

        try {
            return quarantineClient.getCountPerPipe(pipelineId, pipeId, statuses, getToken());
        } catch (TException e) {
            logger.debug("Could not retrieve ErrorCount from quarantine", e);
            throw new IOException("Could not retrieve ErrorCount from quarantine", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Retrieves list of events filtered by error message for a given pipe
     * @param pipelineId the pipeline that contains the pipe
     * @param pipeId the pipe to look through for events
     * @param statuses set of status used for further filtering the events
     * @return set of unique events
     * @throws IOException
     */
    public Set<EventWithCount> getEventCountPerPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses)
            throws IOException {

        Quarantine.Client quarantineClient = getClient();
        try {
            return quarantineClient.getEventCountPerPipe(pipelineId, pipeId, statuses, getToken());
        } catch (TException e) {
            logger.debug("Could not retrieve ErrorCount from quarantine", e);
            throw new IOException("Could not retrieve ErrorCount from quarantine", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    public List<String> getObjectIdsForPipeline(String pipelineId, Set<ObjectStatus> statuses, int pageOffset, short pageSize) throws IOException {
        logger.info("called getObjectCountForPipeline()");
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.getObjectsForPipeline(pipelineId, statuses, pageOffset, pageSize, token);
        } catch (TException e) {
            logger.debug("Could not retrieve IdsForPipeline from quarantine", e);
            throw new IOException("Could not retrieve IdsForPipeline from quarantine", e);
        } finally {
            if (quarantineClient != null){
                pool.returnToPool(quarantineClient);
            }
        }
    }

    /**
     * Returns list of Quarantined results with a given {@link ObjectStatus}
     * @return {@link List<ezbake.quarantine.thrift.QuarantineResult>}
     * @throws java.io.IOException
     */
    public List<QuarantineResult> getObjectsForPipeline(String pipelineId, Set<ObjectStatus> statuses, int pageOffset, short pageSize)
            throws ObjectNotQuarantinedException, TException, IOException {
        logger.info("called getObjectsForPipeline()");
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            List<String> ids = quarantineClient.getObjectsForPipeline(pipelineId, statuses, pageOffset, pageSize, token);

            if (logger.isDebugEnabled()) {
                for(String id: ids) {
                    logger.debug(String.format("FOUND ID: %s", id));
                }
            }

            logger.info("totaled to " + ids.size() + " ids!");
            return quarantineClient.getQuarantinedObjects(ids, token);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Retrieves a list of ids with the specified status for a given pipeline
     * @param pipelineId the id of the pipeline to use
     * @param statuses the status to use for retrieving
     * @return list of object ids
     * @throws IOException
     * @throws ObjectNotQuarantinedException
     */
    public List<String> getIdsForPipeline(String pipelineId, Set<ObjectStatus> statuses, int pageOffset, short pageSize) throws IOException, ObjectNotQuarantinedException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.getObjectsForPipeline(pipelineId, statuses, pageOffset, pageSize , token);
        } catch (TException e) {
            throw new IOException("Could not retrieve id list from quarantine", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }

    }

    /**
     * Retrieves an object for the provided id
     * @param objId the id to use for retrieving the object
     * @return QuarantineResult object
     * @throws IOException
     * @throws ObjectNotQuarantinedException
     */
    public QuarantineResult getObjectForId(String objId) throws IOException, ObjectNotQuarantinedException {
        List<String> ids = new ArrayList<String>();
        ids.add(objId);
        List<QuarantineResult> results = getObjectsForIds(ids);
        if(results != null && results.size() >= 1){
            return results.get(0);
        } else {
            throw new IOException("Quarantine object not found for id " + objId);
        }
    }

    /**
     * Retrieves a list of QuarantineResult objects for the provided ids.
     * It is the caller's responsibility to make sure that the list that was
     * passed in does not exceed MAX_ITEM_COUNT (currently set to 20)
     * in order to prevent out of memory errors
     * @param ids the list ids to use
     * @return the list of quarantine objects for the provided ids
     * @throws IOException
     * @throws ObjectNotQuarantinedException
     */
    public List<QuarantineResult> getObjectsForIds(List<String> ids) throws IOException, ObjectNotQuarantinedException {

        if(ids.size() > MAX_ITEM_COUNT){
            throw new IllegalArgumentException("The array size must be less than " + MAX_ITEM_COUNT);
        }
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.getQuarantinedObjects(ids, token);
        } catch (TException e) {
            logger.error("Could not push data to quarantine service");
            throw new IOException("Could not push data to quarantine service", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Changes the status of provided object ids to the new status and comment
     * @param ids a list of ids to update the status
     * @param newStatus the new status to set
     * @param comment a comment message representing the reason for update
     * @throws IOException
     */
    public void updateStatus(List<String> ids, ObjectStatus newStatus, String comment) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();
        try {
            quarantineClient.updateStatus(ids, newStatus, comment, token);
        } catch (TException e) {
            logger.error("Could not update quarantine status", e);
            throw new IOException("Could not update quarantine status", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Updates the status of a single object
     * @param id the id of the object to update the status for
     * @param newStatus the new status to set
     * @param comment a message representing the reason for update
     * @throws IOException
     */
    public void updateStatus(String id, ObjectStatus newStatus, String comment) throws IOException {
        List<String> idWrapper = new ArrayList<>();
        idWrapper.add(id);
        updateStatus(idWrapper, newStatus, comment);
    }

    public void updateEventStatus(String pipelineId, String pipeId, ObjectStatus oldStatus, ObjectStatus newStatus, String oldEvent, String comment) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            quarantineClient.updateStatusOfEvent(pipelineId, pipeId, oldStatus, newStatus, oldEvent, comment, token);
        } catch (TException e) {
            logger.error("Could not update event");
            throw new IOException("Could not update event", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    public EventWithCount getLatestEventForPipeline(String pipelineId, Set<ObjectStatus> statuses) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.getLatestEventForPipeline(pipelineId, statuses, token);
        } catch (TException e) {
            logger.error("Could not retrieve LatestEventForPipeline");
            throw new IOException("Could not retrieve LatestEventForPipeline", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    public QuarantineEvent getLatestEventForPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.getLatestEventForPipe(pipelineId, pipeId, statuses, token);
        } catch (TException e) {
            logger.error("Could not retrieve LatestEventForPipeline");
            throw new IOException("Could not retrieve LatestEventForPipeline", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Exports binary data for the provided object ids
     * @param ids list of object ids
     * @param key a password to use for encrypting the data
     * @return object data as byte buffer
     * @throws IOException
     */
    public ByteBuffer exportData(List<String> ids, String key) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.exportData(ids, key, token);
        } catch (TException e) {
            logger.error("Could not export data", e);
            throw new IOException("Could not export data", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Deletes the given objects from quarantine.
     * @param ids IDs for the objects to delete
     * @throws IOException
     */
    public void deleteObjects(List<String> ids) throws IOException {
        EzSecurityToken token = getToken();
        Quarantine.Client quarantineClient = getClient();

        try {
            quarantineClient.deleteFromQuarantine(ids, token);
        } catch (ObjectNotQuarantinedException e) {
            logger.error("Objects were not found in quarantine to delete");
            throw new IOException("Objects were not found in quarantine to delete", e);
        } catch (TException e) {
            logger.error("Could not delete from quarantine", e);
            throw new IOException("Could not delete from quarantine", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Deletes objects by event.
     * @param pipelineId the pipeline ID of the objects to delete
     * @param pipeId the pipe ID of the objects to delete
     * @param status the status of the objects to delete
     * @param eventText the event text of the objects to delete
     * @throws IOException
     */
    public void deleteObjectsByEvent(String pipelineId, String pipeId, ObjectStatus status, String eventText) throws IOException {
        EzSecurityToken token = getToken();
        Quarantine.Client quarantineClient = getClient();

        try {
            quarantineClient.deleteObjectsByEvent(pipelineId, pipeId, status, eventText, token);
        } catch (ObjectNotQuarantinedException e) {
            logger.error("Objects were not found in quarantine to delete");
            throw new IOException("Objects were not found in quarantine to delete", e);
        } catch (TException e) {
            logger.error("Could not delete from quarantine", e);
            throw new IOException("Could not delete from quarantine", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    /**
     * Retrieves all pipelines from Quarantine that the current user has access to.
     *
     * @return the list of pipelines which the current user has access to
     * @throws IOException
     */
    public List<String> getPipelinesForUser() throws IOException {
        EzSecurityToken token = getToken();
        Quarantine.Client quarantineClient = getClient();

        try {
            return quarantineClient.getPipelinesForUser(token);
        } catch (TException e) {
            logger.error("Could not retrieve pipelines for user", e);
            throw new IOException("Could not retrieve pipelines for user", e);
        } finally {
            pool.returnToPool(quarantineClient);
        }
    }

    public ImportResult importData(ByteBuffer dataToImport, String key) throws IOException {
        Quarantine.Client quarantineClient = getClient();
        EzSecurityToken token = getToken();

        try {
            return quarantineClient.importData(dataToImport, key, token);
        } catch (TException e) {
            logger.error("Unable import data ", e);
            throw new IOException("Error while importing data ", e);
        }
    }

    private Quarantine.Client getClient() throws IOException {
        try {
            return pool.getClient(QuarantineConstants.SERVICE_NAME, Quarantine.Client.class);
        } catch (TException e) {
            logger.error("Error Retrieving Quarantine Client");
            throw new IOException("Error Retrieving Quarantine Client", e);
        }
    }

    protected EzSecurityToken getToken() throws IOException {
        try {
            return client.fetchTokenForProxiedUser();
        } catch (EzSecurityTokenException e) {
            throw new IOException("Could not retrieve security token from ezbake security", e);
        }
    }

    /**
     * @return a set of all quarantined statuses
     */
    private Set<ObjectStatus> getAllStatuses() {
        return Sets.newHashSet(ObjectStatus.values());
    }
}
