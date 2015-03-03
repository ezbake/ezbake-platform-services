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
import ezbake.deployer.publishers.database.MonetDBDatabaseSetup;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Test class for MonetDBDatabaseSetup
 */
public class MonetDBDatabaseSetupTest {
    @Test
    public void testMonetDBSetup() throws Exception {
        MonetDBDatabaseSetup setup = new MonetDBDatabaseSetup();
        DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet); // not really needed
        artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId("application_id");
        artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType("MonetDB");

        Properties configuration = new Properties();
        configuration.setProperty(EzBakePropertyConstants.MONETDB_HOSTNAME, "localhost");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_PORT, "12345");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_USERNAME, "username");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_PASSWORD, "supersecret");

        List<ArtifactDataEntry> files = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());

        assertEquals(2, files.size());
        for (ArtifactDataEntry entry : files) {
            Properties properties = new Properties();
            properties.load(new ByteArrayInputStream(entry.getData()));
            assertEquals(2, properties.size());
        }
    }

    @Test
    public void testSwivlNotReady() throws Exception {
        MonetDBDatabaseSetup setup = new MonetDBDatabaseSetup();
        DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet); // not really needed
        artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId("application_id");
        artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType("MonetDB");

        Properties configuration = new Properties();
        configuration.setProperty(EzBakePropertyConstants.MONETDB_HOSTNAME, "localhost");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_PORT, "12345");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_USERNAME, "username");
        configuration.setProperty(EzBakePropertyConstants.MONETDB_PASSWORD, "supersecret");
        configuration.setProperty("swivl.not.ready.for.2.0", "true");

        List<ArtifactDataEntry> files = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());

        assertEquals(1, files.size());
        ArtifactDataEntry entry = files.get(0);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(entry.getData()));
        assertEquals(4, properties.size());
    }
}
