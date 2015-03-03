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

package ezbake.deployer;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.common.properties.EzProperties;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.EzDeployPublisher;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.deployer.publishers.EzReverseProxyRegister;
import ezbake.deployer.publishers.ezcentos.EzCentosDeployerModule;
import ezbake.deployer.publishers.local.LocalDeployerModule;
import ezbake.deployer.publishers.openShift.OpenShiftDeployerModule;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.SecurityID;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.DeploymentStatus;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static ezbake.deployer.utilities.ArtifactHelpers.getAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getDataSets;
import static ezbake.deployer.utilities.ArtifactHelpers.getFqAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;

/**
 * This class handles the generic behavior of re-publishing data sets a newly deployed artifact requires.
 * New DeployHandler implementations should extend from this class and implement the getArtifactFromStore() methods, as well
 * as call the publishRequiredDataSets() method prior to their publish call for their DeployedArtifact.  See AccumuloEzDeployerStore
 * for an example.
 * Reference Redmine task 2241 for details on use case.
 *
 * @author ehu
 */
public class EzBakeDeployerHandler extends EzBakeBaseThriftService implements EzBakeServiceDeployer.Iface {
    private static final Logger log = LoggerFactory.getLogger(EzBakeDeployerHandler.class);
    private final String insSecurityId;

    protected EzDeployPublisher publisher;
    protected final EzDeployerConfiguration deployerConfiguration;
    protected final EzDeployerStore ezDeployerStore;
    protected final EzbakeSecurityClient securityClient;
    private final EzReverseProxyRegister reverseProxyRegister;
    private final EzPublisher webPublisher;

    /**
     * Default constructor should only be used by Thrift Runner to bootstrap creating a real instance of EzBakeDeployerHandler
     */
    @SuppressWarnings("unused")
    public EzBakeDeployerHandler() {
        deployerConfiguration = null;
        ezDeployerStore = null;
        securityClient = null;
        insSecurityId = null;
        reverseProxyRegister = null;
        webPublisher = null;
    }

    @Inject
    public EzBakeDeployerHandler(EzDeployPublisher publisher, EzDeployerStore store, EzDeployerConfiguration ezDeployerConfiguration,
                                 EzbakeSecurityClient securityClient, EzReverseProxyRegister reverseProxyRegister,
                                 @EzPublisherMapping.WebApp EzPublisher webAppPublisher) throws IOException, DeploymentException {
        this.deployerConfiguration = ezDeployerConfiguration;
        this.publisher = publisher;
        this.ezDeployerStore = store;
        this.insSecurityId = SecurityID.ReservedSecurityId.INS_REG.getId();
        this.securityClient = securityClient;
        this.reverseProxyRegister = reverseProxyRegister;
        this.webPublisher = webAppPublisher;
        log.info("Validating environment");
        validateEnvironment();
    }

    @Override
    public final DeploymentMetadata deployService(ArtifactManifest manifest, ByteBuffer artifact, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.info("Artifact " + getFqAppId(manifest) + " requested to be deployed.");
        if (!StringUtils.isAlphanumeric(getServiceId(manifest))) {
            log.error("ServiceId must be alphanumeric: " + getServiceId(manifest));
            throw new DeploymentException("Service ids must be alphanumeric");
        }
        DeploymentArtifact deployedArtifact = ezDeployerStore.writeArtifactToStore(manifest, artifact);
        publishArtifact(deployedArtifact, token);
        return deployedArtifact.getMetadata();
    }

    @Override
    public final void publishArtifactLatestVersion(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.info("Publish latest version of artifact {} for application {} at {}", serviceId, applicationId,
                Calendar.getInstance().getTime().toString());
        publishArtifact(ezDeployerStore.getArtifactFromStore(applicationId, serviceId), token);
    }

    @Override
    public final void publishArtifact(String applicationId, String serviceId, String version, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.info("Publish artifact " + serviceId + " at version " + version);
        publishArtifact(ezDeployerStore.getArtifactFromStore(applicationId, serviceId, version), token);
    }

    private void publishArtifact(DeploymentArtifact deployedArtifact, EzSecurityToken token) throws DeploymentException {
        if (deployedArtifact.getArtifact() == null || deployedArtifact.getArtifact().length == 0) {
            throw new DeploymentException("A null or an empty byte array artifact cannot be deployed.  Perhaps loading from the the store failed.");
        }
        publishRequiredDataSets(publisher, deployedArtifact, token);
        if (deployedArtifact.getMetadata().getStatus() == DeploymentStatus.Denied) {
            throw new DeploymentException("Cannot publish an artifact that has been denied deployment");
        }
        publisher.publish(deployedArtifact, token);
        if (deployedArtifact.getMetadata().getStatus() != DeploymentStatus.Deployed) {
            deployedArtifact.getMetadata().setStatus(DeploymentStatus.Deployed);
            try {
                ezDeployerStore.updateDeploymentMetadata(deployedArtifact.getMetadata());
            } catch (TException ex) {
                log.error("Updating metadata to deployed failed", ex);
                throw new DeploymentException("Failed trying to update the metadata for the artifact being published");
            }
        }
    }

    @Override
    public void deleteArtifact(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        List<DeploymentMetadata> metadatas = getApplicationVersions(applicationId, serviceId, token);
        log.info("Number of artifacts to be deleted from the store {}.", metadatas.size());

        for (DeploymentMetadata metadata : metadatas) {
            ezDeployerStore.removeFromStore(metadata);
            log.info("Deleted artifact from the store with version {} and status {}.", metadata.getVersion(), metadata.getStatus());
        }
    }

    @Override
    public final void undeploy(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);

        DeploymentArtifact deployedArtifact = ezDeployerStore.getArtifactFromStore(applicationId, serviceId);
        publisher.unpublish(deployedArtifact, token);
        try {
            DeploymentMetadata metadata = getLatestApplicationVersion(applicationId, serviceId, token);
            if (deployedArtifact.getArtifact() == null || deployedArtifact.getArtifact().length == 0) {
                log.warn("Package undeployed, however, the deployment artifact is missing, so the metadata is being deleted");
                ezDeployerStore.removeFromStore(metadata);
            } else if (metadata.getStatus() == DeploymentStatus.Deployed) {
                metadata.setStatus(DeploymentStatus.Undeployed);
                ezDeployerStore.updateDeploymentMetadata(metadata);
            } else {
                log.warn("Artifact {} {} was unpublished, but wasn't in Deployed status",
                        ArtifactHelpers.getAppId(deployedArtifact), ArtifactHelpers.getServiceId(deployedArtifact));
            }
        } catch (TException ex) {
            log.error("Failed to update metadata", ex);
            throw new DeploymentException("Failed to update the deployment metadata after unpublishing the artifact: " +
                    ex.getMessage());
        }
    }

    @Override
    public final DeploymentMetadata getLatestApplicationVersion(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.debug("Get latest version for artifact " + serviceId);
        return ezDeployerStore.getLatestApplicationMetaDataFromStore(applicationId, serviceId);
    }

    @Override
    public List<DeploymentMetadata> listDeployed(String fieldName, String fieldValue, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        final EzDeployerStore.FieldName name = EzDeployerStore.FieldName.getByName(fieldName);
        return ezDeployerStore.getApplicationMetaDataMatching(name, fieldValue).toList();
    }


    @Override
    public final List<DeploymentMetadata> getApplicationVersions(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.info("Looking for application {} service {}", applicationId, serviceId);
        return Lists.newArrayList(expectMore(ezDeployerStore.getApplicationMetaDataFromStoreForAllVersions(applicationId, serviceId)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final TProcessor getThriftProcessor() {
        String deployMode = new EzProperties(getConfigurationProperties(), true).getProperty("ezDeploy.mode", "openshift");
        log.info("Deploy Mode: {}", deployMode);
        //Disabling SNI because java 7 treats unknown host warning as a fatal error and OpenShift apparently returns that warning. Yay!
        System.setProperty("jsse.enableSNIExtension", "false");
        Module deployerModule;
        switch (deployMode) {
            case "local":
                deployerModule = new LocalDeployerModule();
                break;
            case "ezcentos":
                deployerModule = new EzCentosDeployerModule();
                break;
            default:
                deployerModule = new OpenShiftDeployerModule();
                break;
        }
        log.info("Created deployer module");
        Injector injector = Guice.createInjector(new DeployerModule(),
                new ThriftRunnerConfigurationModule(getConfigurationProperties()),
                deployerModule);
        return new EzBakeServiceDeployer.Processor(injector.getInstance(EzBakeDeployerHandler.class));
    }

    @Override
    public void stageServiceDeployment(ArtifactManifest manifest, ByteBuffer artifact, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        log.info("Artifact " + getFqAppId(manifest) + " requested to be staged for deployment.");
        if (!StringUtils.isAlphanumeric(getServiceId(manifest))) {
            log.error("ServiceId must be alphanumeric: " + getServiceId(manifest));
            throw new DeploymentException("Service ids must be alphanumeric");
        }
        DeploymentMetadata oldVersion = null;
        try {
            oldVersion = ezDeployerStore.getLatestApplicationMetaDataFromStore(getAppId(manifest),
                    getServiceId(manifest));
        } catch (DeploymentException e) {
            //it's okay if it doesn't already exist
        }
        if (oldVersion != null) {
            switch (oldVersion.getStatus()) {
                case Denied:
                case Undeployed:
                case Staged:
                    //In these cases, the user is replacing the artifact, so go ahead and remove the old data
                    ezDeployerStore.removeFromStore(oldVersion);
                    break;
                default:
                    break;
            }
        }
        ezDeployerStore.writeArtifactToStore(manifest, artifact, DeploymentStatus.Staged);
    }

    @Override
    public void unstageServiceDeployment(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        DeploymentMetadata metadata = getLatestApplicationVersion(applicationId, serviceId, token);
        if (metadata.getStatus() != DeploymentStatus.Staged) {
            throw new DeploymentException("Cannot unstage this deployment because it is currently not in the Staged status");
        }
        log.info("Denying deployment of application {} and service {}.", applicationId, serviceId);
        metadata.setStatus(DeploymentStatus.Denied);
        ezDeployerStore.updateDeploymentMetadata(metadata);
    }

    @Override
    public void updateDeploymentMetadata(DeploymentMetadata metadata,
                                         EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        ezDeployerStore.updateDeploymentMetadata(metadata);
    }

    @Override
    public void reregister(String applicationId, String serviceId, EzSecurityToken token) throws TException {
        checkSecurityToken(token);
        DeploymentArtifact artifact = ezDeployerStore.getArtifactFromStore(applicationId, serviceId);
        if (artifact != null) {
            if (artifact.getMetadata().getManifest().getArtifactType() == ArtifactType.WebApp) {
                //Get the list of instances isn't a supported call of any other publishers, so we have to hard-code
                //using the OpenShift publisher here.
                EzOpenShiftPublisher openShiftPublisher = (EzOpenShiftPublisher)webPublisher;
                List<URL> applicationUrls = openShiftPublisher.findAllApplicationInstances(artifact);
                for (URL url : applicationUrls) {
                    reverseProxyRegister.publish(artifact, url);
                }
            } else {
                log.info("Artifact {} {} is not a web app, nothing to re-register", applicationId, serviceId);
            }
        } else {
            throw new DeploymentException(String.format("Failed to retrieve a valid artifact for %s %s",
                    applicationId, serviceId));
        }

    }
    

    /**
     * Get latest version of deployment artifact
     *
     * @param applicationId - Application id of the artifact
     * @param serviceId - service id of the artifact
     * @param token - caller's security token
    */
	@Override
    public DeploymentArtifact getLatestVersionOfDeploymentArtifact(
            String applicationId, String serviceId, EzSecurityToken token)
            throws DeploymentException, TException {
        checkSecurityToken(token);
        return ezDeployerStore.getArtifactFromStore(applicationId, serviceId);
    }


    /**
     * Validates the environment for the deployer.
     *
     * @throws DeploymentException
     * @throws IllegalStateException
     */
    private void validateEnvironment() throws DeploymentException, IllegalStateException {
        publisher.validate();
    }

    private void checkSecurityToken(EzSecurityToken token) throws DeploymentException {
        //Only the deployer and INS can deploy
        try {
            securityClient.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            log.error("Token validation failed. ", e);
            throw new DeploymentException("Token failed validation");
        }

        String fromId = token.getValidity().getIssuedTo();
        String forId = token.getValidity().getIssuedFor();
        if (!fromId.equals(forId) && !fromId.equals(insSecurityId)) {
            throw new SecurityException(String.format(
                    "This call can only be made from INS (%s) or Deployer services. From: %s - To: %s",
                    insSecurityId, fromId, forId));
        }
    }

    /**
     * Uses the specified publisher to publish new copies of all data sets the specified artifact uses.
     *
     * @param publisher - The publisher to publish the DataSets to
     * @param artifact  - The artifact to get the DataSets from
     * @param token     - The token of the caller
     * @throws DeploymentException
     */
    protected void publishRequiredDataSets(EzPublisher publisher, DeploymentArtifact artifact, EzSecurityToken token) throws DeploymentException {
        try {
            // get the data sets, we are assuming the Set contains application Ids of the data sets
            Iterable<String> dsets = getDataSets(artifact);
            if (dsets == null) dsets = Collections.emptySet();
            for (String dsetAppId : dsets) {
                String[] dsetSplitFQN = dsetAppId.split(",");
                String dsetAppName = dsetSplitFQN.length > 1 ? dsetSplitFQN[0] : "";
                String dsetServiceName = Iterables.getFirst(Lists.newArrayList(dsetSplitFQN), "");
                // get the dataset's original deployment artifact and make changes
                DeploymentArtifact dataSetArtifact = ezDeployerStore.getArtifactFromStore(dsetAppName, dsetServiceName);

                String newAppId = getAppId(artifact) + getAppId(dataSetArtifact);
                String newServiceId = getServiceId(dataSetArtifact);

                dataSetArtifact.getMetadata().getManifest().getApplicationInfo().setApplicationId(newAppId);
                dataSetArtifact.getMetadata().getManifest().getApplicationInfo().setServiceId(newServiceId);

                // publish modified datasetArtifact that is now respective to the deployed artifact.
                publisher.publish(dataSetArtifact, token);
            }
        } catch (TException te) {
            throw new DeploymentException(ExceptionUtils.getStackTrace(te));
        }
    }

    protected <T> FluentIterable<T> expectMore(FluentIterable<T> rows) throws DeploymentException {
        if (rows.isEmpty()) {
            DeploymentException e = new DeploymentException("Can not find application:version provided");
            log.error("Error getting metadata for artifact", e);
            throw e;
        }
        return rows;
    }
}
