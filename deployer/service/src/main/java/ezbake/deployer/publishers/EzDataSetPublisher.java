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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.publishers.database.DatabaseSetup;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * This a publisher for datasets. It handles creating databases, users, etcs for each individual database and then
 * delegates to whatever Thrift publisher is registered for actual publishing
 */
public class EzDataSetPublisher implements EzPublisher {

    private final EzPublisher publisher;
    private final Set<DatabaseSetup> possibleSetups;
    private final Properties configuration;

    @Inject
    public EzDataSetPublisher(@EzPublisherMapping.Thrift EzPublisher actualPublisher, Set<DatabaseSetup> possibleSetups,
                              Properties configuration) {
        this.publisher = actualPublisher;
        this.possibleSetups = possibleSetups;
        this.configuration = configuration;
    }

    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        if (!artifact.getMetadata().getManifest().getDatabaseInfo().isSetDatabaseType()) {
            throw new DeploymentException("Expecting a database type for a dataset");
        }
        for (DatabaseSetup dbSetup : possibleSetups) {
            if (dbSetup.canSetupDatabase(artifact)) {
                List<ArtifactDataEntry> propertiesFiles = dbSetup.setupDatabase(artifact, configuration, callerToken);
                ArtifactHelpers.addFilesToArtifact(artifact, propertiesFiles);
                break;
            }
        }
        publisher.publish(artifact, callerToken);
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        //We're not going to mess with the database on unpublish, just delegate down to the publisher
        publisher.unpublish(artifact, callerToken);
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        //We don't know what database we need to valiate yet, just delegate down to the publisher
        publisher.validate();
    }

    @VisibleForTesting
    public int possibleSetupsCount() {
        return possibleSetups.size();
    }
}
