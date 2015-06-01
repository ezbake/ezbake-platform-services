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
import ezbake.deployer.publishers.database.AccumuloDatabaseSetup;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.SecurityOperationsImpl;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Credentials;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;


public class AccumuloDatabaseSetupTest {
    // commented out @Test as this test needs live instance of accumulo.
    //@Test
    public void testAccumuloSetup() throws Exception {
        final String expectedUserName = "application_id_user";
        final String expectedNamespace = "application_id";
        final String expectedTableName = "test_table";
        final String rootUserName = "root";
        final String rootPassword = "secret";

        DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet);
        artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId("application_id");
        artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType("Accumulo");
        Properties configuration = new Properties();
        configuration.setProperty(EzBakePropertyConstants.ACCUMULO_INSTANCE_NAME, "dev_instance");
        configuration.setProperty(EzBakePropertyConstants.ACCUMULO_ZOOKEEPERS, "localhost:2181");
        configuration.setProperty(EzBakePropertyConstants.ACCUMULO_NAMESPACE, "application-id");
        configuration.setProperty(EzBakePropertyConstants.ACCUMULO_USERNAME, rootUserName);
        configuration.setProperty(EzBakePropertyConstants.ACCUMULO_PASSWORD, rootPassword);
        configuration.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");

        ThriftClientPool pool = new ThriftClientPool(configuration);
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(configuration);
        AccumuloDatabaseSetup setup = new AccumuloDatabaseSetup(pool, securityClient);
        List<ArtifactDataEntry> entries = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());

        // connect to accumulo, create table, insert random data, test if fails
        Properties properties = new Properties();
        for (ArtifactDataEntry entry : entries) {
            properties.load(new ByteArrayInputStream(entry.getData()));
        }

        AccumuloHelper accumuloConfig = new AccumuloHelper(properties);
        Assert.assertEquals(rootPassword, accumuloConfig.getAccumuloPassword());
        Assert.assertEquals(rootUserName, accumuloConfig.getAccumuloUsername());
        Instance instance = new ZooKeeperInstance(accumuloConfig.getAccumuloInstance(), accumuloConfig.getAccumuloZookeepers());

        Credentials credentials = new Credentials(expectedUserName, new PasswordToken(accumuloConfig.getAccumuloPassword()));
        TableOperationsImpl tableOpsImpl = new TableOperationsImpl(instance, credentials);
        tableOpsImpl.create(String.format("%s.%s", expectedNamespace, expectedTableName));
        try {
            Assert.assertTrue(tableOpsImpl.exists(expectedTableName));
        } finally {
            pool.close();
            securityClient.close();
            try { // attempt cleanup

                // given that namespace contains only one table, both table and namespace will be deleted
                tableOpsImpl.delete(String.format("%s.%s", expectedNamespace, expectedTableName));

                credentials = new Credentials(rootUserName, new PasswordToken(rootPassword));
                SecurityOperationsImpl secOpsImpl = new SecurityOperationsImpl(instance, credentials);
                secOpsImpl.dropLocalUser(expectedUserName);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
