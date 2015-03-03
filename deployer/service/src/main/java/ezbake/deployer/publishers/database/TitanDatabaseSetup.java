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

package ezbake.deployer.publishers.database;

import com.google.common.collect.Lists;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;

import javax.inject.Inject;
import java.util.List;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/1/14
 * Time: 12:51 PM
 */
public class TitanDatabaseSetup implements DatabaseSetup {
    public static final String DATABASE_NAME = "Titan";

    private AccumuloDatabaseSetup accumuloSetup;
    private ElasticsearchDatabaseSetup elasticSetup;

    @Inject
    public TitanDatabaseSetup(AccumuloDatabaseSetup accumuloSetup, ElasticsearchDatabaseSetup elasticSetup) {
        this.accumuloSetup = accumuloSetup;
        this.elasticSetup = elasticSetup;
    }

    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact, Properties configuration, EzSecurityToken callerToken) throws DeploymentException {
        List<ArtifactDataEntry> entries = Lists.newArrayList();
        entries.addAll(accumuloSetup.setupDatabase(artifact, configuration, callerToken));
        entries.addAll(elasticSetup.setupDatabase(artifact, configuration, callerToken));
        return entries;
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase(DATABASE_NAME);
    }
}
