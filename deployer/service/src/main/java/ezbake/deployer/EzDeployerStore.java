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

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.DeploymentStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * An abstraction for the EzDeployer to store/load artifacts that have been deployed to the PaaS
 */
public abstract class EzDeployerStore {
    /**
     * Returns {@link ezbake.services.deploy.thrift.DeploymentMetadata} for the latest version that has been deployed
     *
     * @param applicationId - the application identifier to use to lookup the artifact to grab from the store
     * @param serviceId     - The service identifier to use to find the artifact to grab from the store
     * @return {@link ezbake.services.deploy.thrift.DeploymentMetadata} for the latest version that has been deployed
     * @throws TException          - On any errors serializing/deserializing the thrift object
     * @throws DeploymentException - On any exception that may occur, like can't read from database
     */
    public abstract DeploymentMetadata getLatestApplicationMetaDataFromStore(String applicationId, String serviceId) throws TException, DeploymentException;

    /**
     * Returns {@link ezbake.services.deploy.thrift.DeploymentMetadata} for every version that has been deployed
     *
     * @param applicationId - the application identifier to use to lookup the artifact to grab from the store
     * @param serviceId     - The service identifier to use to find the artifact to grab from the store
     * @return List of {@link ezbake.services.deploy.thrift.DeploymentMetadata} for every version that has been deployed
     * @throws TException          - On any errors serializing/deserializing the thrift object
     * @throws DeploymentException - On any exception that may occur, like can't read from database
     */
    public abstract FluentIterable<DeploymentMetadata> getApplicationMetaDataFromStoreForAllVersions(String applicationId, String serviceId) throws TException, DeploymentException;

    /**
     * Writes the artifact in the store along side the Manifest for the artifact.  It will use the Manifest for indexing and index certain fields for lookup.
     *
     * @param manifest - The manifest to use for this artifact
     * @param artifact - The actualy tarball artifact to store
     * @return The Logical version of the artifact deployed
     * @throws DeploymentException - On any exception that may occur, like can't write to database
     * @throws TException          - On any errors serializing/deserializing the thrift object
     */
    public DeploymentArtifact writeArtifactToStore(ArtifactManifest manifest, ByteBuffer artifact) throws DeploymentException, TException {
        return writeArtifactToStore(manifest, artifact, DeploymentStatus.Deployed);
    }

    /**
     * Writes the artifact in the store along side the Manifest for the artifact.  It will use the Manifest for indexing and index certain fields for lookup.
     *
     * @param manifest - The manifest to use for this artifact
     * @param artifact - The actualy tarball artifact to store
     * @param status   - The status, whether this artifact is actually deployed or not
     * @return The Logical version of the artifact deployed
     * @throws DeploymentException - On any exception that may occur, like can't write to database
     * @throws TException          - On any errors serializing/deserializing the thrift object
     */
    public abstract DeploymentArtifact writeArtifactToStore(ArtifactManifest manifest, ByteBuffer artifact, DeploymentStatus status) throws DeploymentException, TException;

    /**
     * Remove data from the store
     *
     * @param metadata - The metadata representing the artifact to remove
     * @throws DeploymentException
     * @throws TException
     */
    public abstract void removeFromStore(DeploymentMetadata metadata) throws DeploymentException, TException;

    /**
     * Updates just the metadata
     *
     * @param metadata - The metadata to be updated
     * @throws DeploymentException
     * @throws TException
     */
    public abstract void updateDeploymentMetadata(DeploymentMetadata metadata) throws DeploymentException, TException;

    /**
     * Performs a search off of one of the Index Fields by {@link ezbake.deployer.EzDeployerStore.FieldName} that matches the value fieldValue exactly.
     * If no items are found, it will return an {@link java.lang.Iterable} that has no items in it.
     *
     * @param fieldName  - The field to lookup.  See {@link ezbake.deployer.EzDeployerStore.FieldName} for a list of valid fields
     * @param fieldValue - The value to match exactly against.
     * @return An iterable of {@link DeploymentMetadata} that matches the query
     * @throws TException          - A thrift exception that occurred (like bad serialized object in database)
     * @throws DeploymentException -  Any exception searching.
     */
    public abstract FluentIterable<DeploymentMetadata> getApplicationMetaDataMatching(FieldName fieldName, String fieldValue) throws TException, DeploymentException;


    /**
     * Returns the latest version of the DeploymentArtifact given the app's ID. The implementation is specific for
     * each deployer handler implementation.
     *
     * @param applicationId - the application identifier to use to lookup the artifact to grab from the store
     * @param serviceId     - The service identifier to use to find the artifact to grab from the store
     * @return The {@link ezbake.services.deploy.thrift.DeploymentArtifact} from the store.
     * @throws TException          - A thrift exception that occured
     * @throws DeploymentException - Any exception like the artifact is not found in the store.
     */
    public abstract DeploymentArtifact getArtifactFromStore(String applicationId, String serviceId) throws TException, DeploymentException;

    /**
     * Returns the DeploymentArtifact given the app's ID. The implementation is specific for
     * each deployer handler implementation.
     *
     * @param applicationId - the application identifier to use to lookup the artifact to grab from the store
     * @param serviceId     - The service identifier to use to find the artifact to grab from the store
     * @param version       - The specific version of the artifact to grab from the store.
     * @return - The artifact that was deployed from the store
     * @throws TException
     * @throws DeploymentException
     */
    public abstract DeploymentArtifact getArtifactFromStore(String applicationId, String serviceId, String version) throws TException, DeploymentException;


    private static final Logger log = LoggerFactory.getLogger(EzBakeDeployerHandler.class);

    protected final Properties configuration;
    protected final EzDeployerConfiguration deployerConfiguration;

    /**
     * Valid fieldNames that has been indexed
     */
    public enum FieldName {
        SecurityId("security-id"),
        ApplicationId("application-id"),
        User("user"),
        Status("status");

        final String name;

        FieldName(String name) {
            this.name = name;
        }

        public static FieldName getByName(String name) {
            for (FieldName n : values()) {
                if (n.name.equalsIgnoreCase(name) || n.toString().equalsIgnoreCase(name)) {
                    return n;
                }
            }
            throw new IllegalArgumentException("Field by the name \"" + name + "\" does not exist.");
        }
    }

    protected EzDeployerStore(Properties configuration, EzDeployerConfiguration deployerConfiguration) {
        this.configuration = configuration;
        this.deployerConfiguration = deployerConfiguration;
    }


    protected long createVersionNumber() {
        return System.currentTimeMillis();
    }

    protected <T> T getOrThrow(Optional<T> item) throws DeploymentException {
        if (!item.isPresent()) {
            DeploymentException e = new DeploymentException("Can not found application:version provided");
            log.error("Error getting metadata for artifact", e);
            throw e;
        }
        return item.get();
    }
}
