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

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import deployer.TestUtils;
import ezbake.common.properties.EzProperties;
import ezbake.common.security.NoOpTextCryptoProvider;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.publishers.database.MongoDBDatabaseSetup;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test setuping up a new database and user in MongoDB
 */
public class MongoDBDatabaseSetupTest {

    //@Test
    public void testMongoDBSetup() throws Exception {
        MongoDBDatabaseSetup setup = new MongoDBDatabaseSetup();
        DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet);
        artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId("app2");
        artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType("MongoDB");
        EzProperties configuration = new EzProperties();
        configuration.setTextCryptoProvider(new NoOpTextCryptoProvider());
        configuration.setProperty(EzBakePropertyConstants.MONGODB_DB_NAME, "admin");
        configuration.setProperty(EzBakePropertyConstants.MONGODB_HOST_NAME, "localhost");
        configuration.setProperty(EzBakePropertyConstants.MONGODB_USE_SSL, "false");
        configuration.setProperty(EzBakePropertyConstants.MONGODB_USER_NAME, "admin");
        configuration.setProperty(EzBakePropertyConstants.MONGODB_PASSWORD, "password", true);

        List<ArtifactDataEntry> entries = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());

        //No try to connect to mongo with the new configuration values
        Properties properties = new Properties();
        for (ArtifactDataEntry entry : entries) {
            properties.load(new ByteArrayInputStream(entry.getData()));
        }
        MongoConfigurationHelper mongoConfig = new MongoConfigurationHelper(properties,
                new NoOpTextCryptoProvider());
        MongoClient client = getMongoClient(mongoConfig);
        DBCollection collection = client.getDB(mongoConfig.getMongoDBDatabaseName()).createCollection("test", null);
        assertNotNull("Should be good to go on our own database", collection);
        try {
            collection = client.getDB("admin").createCollection("test", null);
            fail("Shouldn't be able to do anything on the admin database");
        } catch (Exception ex) {
            //pass
        }
        client.close();
    }

    private MongoClient getMongoClient(MongoConfigurationHelper mongoDBConfiguration) throws Exception {
        String connectionString = String.format("mongodb://%s:%s@%s/%s?ssl=%b",
                mongoDBConfiguration.getMongoDBUserName(), mongoDBConfiguration.getMongoDBPassword(),
                mongoDBConfiguration.getMongoDBHostName(), mongoDBConfiguration.getMongoDBDatabaseName(), mongoDBConfiguration.useMongoDBSSL());
        return new MongoClient(new MongoClientURI(connectionString));
    }
}
