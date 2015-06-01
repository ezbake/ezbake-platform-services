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

package ezbake.deployer.publishers;

import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.frack.submitter.thrift.PipelineNotRunningException;
import ezbake.frack.submitter.thrift.SubmitResult;
import ezbake.frack.submitter.thrift.Submitter;
import ezbake.frack.submitter.thrift.submitterConstants;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.security.lock.smith.thrift.KeyType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftClientPool;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class EzFrackPublisher implements EzPublisher {
    private static final Logger log = LoggerFactory.getLogger(EzFrackPublisher.class);
    private static final boolean DEBUG = false;

    private final ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;

    private static final String KEYS_DIR = "keys" + File.separator;

    @Inject
    public EzFrackPublisher(ThriftClientPool pool, Properties configs) {
        this.pool = pool;
        this.securityClient = new EzbakeSecurityClient(configs);
    }

    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        Submitter.Client client;
        String pipelineId = ArtifactHelpers.getServiceId(artifact);
        try {
            client = pool.getClient(submitterConstants.SERVICE_NAME, Submitter.Client.class);

            // Kill the pipeline first
            try {
                log.info("Attempting to kill pipeline to make sure that the new deployment succeeds");
                client.shutdown(pipelineId);
            } catch (PipelineNotRunningException e) {
                log.warn("Pipeline was not running, continuing deployment. Exception from submitter: ", e);
            }

            log.info("Deploying pipeline {}", pipelineId);
            byte[] tarGz = handleKeys(artifact, callerToken);
            SubmitResult result = client.submit(ByteBuffer.wrap(tarGz), pipelineId);
            log.info("Frack submission was successful? " + result.isSubmitted());
            log.info("Frack submission result message: \n" + result.getMessage());
        } catch (TException e) {
            String message = "Could not submit artifact to Frack";
            log.error(message, e);
            throw new DeploymentException(message);
        }

    }

    /**
     * Appends encryption keys to the artifact.
     *
     * @param artifact the artifact being deployed
     * @return byte array artifact with keys appended
     * @throws DeploymentException
     */
    private byte[] handleKeys(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        InternalNameService.Client client = null;
        EzLocksmith.Client lockSmithClient = null;

        //Retrieve app specific items
        String feedName = ArtifactHelpers.getServiceId(artifact);
        String appId = ArtifactHelpers.getSecurityId(artifact);
        Map<String, String> broadcastTopics = new HashMap<>();
        Map<String, String> listeningTopics = new HashMap<>();
        try {
            //Acquire clients
            client = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
            lockSmithClient = pool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class);

            //Acquire tokens
            EzSecurityToken insToken = securityClient.fetchDerivedTokenForApp(callerToken,
                    pool.getSecurityId(InternalNameServiceConstants.SERVICE_NAME));
            EzSecurityToken lockSmithToken = securityClient.fetchDerivedTokenForApp(callerToken,
                    pool.getSecurityId(EzLocksmithConstants.SERVICE_NAME));


            //Public keys for these topics (topics being broadcasted)
            Set<String> topicsBroadcasting = client.getApprovedTopicsForFeed(appId, feedName, insToken);
            //topics broadcasted
            for (String topic : topicsBroadcasting) {
                String privateKey = lockSmithClient.retrieveKey(lockSmithToken, topic, KeyType.RSA);
                String pubKey = RSAKeyCrypto.getPublicFromPrivatePEM(privateKey);
                //Add public key here
                broadcastTopics.put(topic, pubKey);
            }
            //Private keys of topic being listened to
            Set<String> topicsListen = client.getListeningTopicsForFeed(appId, feedName, insToken);
            for (String topic : topicsListen) {
                String privateKey = lockSmithClient.retrieveKey(lockSmithToken, topic, KeyType.RSA);
                listeningTopics.put(topic, privateKey);
            }
            return appendKeys(artifact, broadcastTopics, listeningTopics);
        } catch (Exception e) {
            log.error("Exception", e);
            throw new DeploymentException(e.getMessage());
        } finally {
            pool.returnToPool(client);
            pool.returnToPool(lockSmithClient);

        }
    }

    private byte[] appendKeys(DeploymentArtifact artifact,
                              Map<String, String> broadcastData,
                              Map<String, String> listeningData) throws DeploymentException {

        //Retrieve entries to append
        List<ArtifactDataEntry> entries = getEntriesToAppend(broadcastData, listeningData);
        ArtifactHelpers.addFilesToArtifact(artifact, entries);
        if (DEBUG)
            testCopyToVagrant(artifact.getArtifact());
        return artifact.getArtifact();
    }

    private List<ArtifactDataEntry> getEntriesToAppend(Map<String, String> broadcastData, Map<String, String> listeningData) {
        List<ArtifactDataEntry> entriesToAppend = new ArrayList<>(broadcastData.size() + listeningData.size() + 2);
        appendKeys(KEYS_DIR, ".priv", listeningData, entriesToAppend);
        appendKeys(KEYS_DIR, ".pub", broadcastData, entriesToAppend);
        return entriesToAppend;
    }

    private void appendKeys(String parentDirPath, String suffix, Map<String, String> dataMap,
                            List<ArtifactDataEntry> result) {
        for (String topic : dataMap.keySet()) {
            result.add(new ArtifactDataEntry(new TarArchiveEntry(parentDirPath + topic + suffix),
                    dataMap.get(topic).getBytes()));
        }
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        String pipelineId = ArtifactHelpers.getServiceId(artifact);
        Submitter.Client client = null;
        try {
            client = pool.getClient("fracksubmitter", Submitter.Client.class);
            client.shutdown(pipelineId);
            log.info("Pipeline {} shutdown successfully", pipelineId);
        } catch (PipelineNotRunningException e) {
            log.warn("Pipeline {} was not running, no action taken", pipelineId, e);
        } catch (TException e) {
            String message = String.format("Could not successfully shut down pipeline %s, please try again", pipelineId);
            log.error(message, e);
            throw new DeploymentException(message);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {

    }

    /**
     * Copies the provided byte array to vagrant dir
     * This should be used for debugging purposes only
     *
     * @param in byte to copy
     */
    private void testCopyToVagrant(byte[] in) {
        File f = new File("/vagrant/test10.tar.gz");
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(f);
            out.write(in);
        } catch (IOException e) {
            log.error("Failed copying to vagrant", e);
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (IOException e) {
                    //don't care
                }
        }

    }
}
