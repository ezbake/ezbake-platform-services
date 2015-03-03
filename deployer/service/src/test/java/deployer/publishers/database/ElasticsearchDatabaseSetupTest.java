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

package deployer.publishers.database;

import deployer.TestUtils;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.publishers.database.ElasticsearchDatabaseSetup;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class ElasticsearchDatabaseSetupTest {

    @Test
    public void testElasticsearchSetup() throws Exception {

        ElasticsearchDatabaseSetup setup = new ElasticsearchDatabaseSetup();
        DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet); // not really needed
        artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId("application_id");
        artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType("Elasticsearch");

        Properties configuration = new Properties();
        configuration.setProperty(EzBakePropertyConstants.ELASTICSEARCH_HOST, "localhost");
        configuration.setProperty(EzBakePropertyConstants.ELASTICSEARCH_PORT, Integer.toString(123));
        configuration.setProperty(EzBakePropertyConstants.ELASTICSEARCH_CLUSTER_NAME, "elasticsearchCluster");
        configuration.setProperty(EzBakePropertyConstants.ELASTICSEARCH_FORCE_REFRESH_ON_PUT, Boolean.toString(true));

        List<ArtifactDataEntry> entry = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());

        assertNotNull(entry);
    }
}
