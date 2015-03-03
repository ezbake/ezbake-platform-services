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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;
import ezbakehelpers.mongoutils.MongoHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Setup a MongoDB user and database for an application
 */
public class MongoDBDatabaseSetup implements DatabaseSetup {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBDatabaseSetup.class);
    private final SecureRandom random = new SecureRandom();
    private static final List<String> DBRole = Lists.newArrayList("dbOwner");
    private static final String MONGODB_PROPERTIES_FILE_NAME = "mongodb.properties";
    private static final String ENCRYPTED_MONGODB_PROPERTIES_FILE_NAME = "encrypted_mongodb.properties";


    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact, Properties configuration, EzSecurityToken callerToken) throws DeploymentException {
        MongoHelper mongoHelper = new MongoHelper(configuration);
        MongoConfigurationHelper mongoConfiguration = mongoHelper.getMongoConfigurationHelper();

        //Setup new mongo properties
        String databaseName = ArtifactHelpers.getNamespace(artifact);
        String userName = databaseName + "_user";
        String password = new BigInteger(130, random).toString(32);

        //Connect to Mongo DB
        Mongo client = getMongoClient(mongoHelper);
        try {
            //If user exists, re-generate the password and reset the password
            DBObject cmd = new BasicDBObject("updateUser", userName);
            cmd.put("pwd", password);
            cmd.put("roles", DBRole);
            CommandResult result = client.getDB(databaseName).command(cmd);
            if (!result.ok()) {
                logger.warn("Failed to update mongo user. {}.  Attempting to add", result.getErrorMessage());
                //If user doesn't exist, create new Mongo DB User/Password/Database unique for this application
                cmd = new BasicDBObject("createUser", userName);
                cmd.put("pwd", password);
                cmd.put("roles", DBRole);
                result = client.getDB(databaseName).command(cmd);
                result.throwOnError();
            }
        } finally {
            client.close();
        }

        //Create a mongo.properties file with mongo host, username, password(hashed), and database
        Map<String, String> valuesMap = Maps.newHashMap();
        valuesMap.put(EzBakePropertyConstants.MONGODB_DB_NAME, databaseName);
        valuesMap.put(EzBakePropertyConstants.MONGODB_HOST_NAME, mongoConfiguration.getMongoDBHostName());
        valuesMap.put(EzBakePropertyConstants.MONGODB_USE_SSL, Boolean.toString(mongoConfiguration.useMongoDBSSL()));

        Map<String, String> encryptedValuesMap = Maps.newHashMap();
        encryptedValuesMap.put(EzBakePropertyConstants.MONGODB_USER_NAME, userName);
        encryptedValuesMap.put(EzBakePropertyConstants.MONGODB_PASSWORD, password);
        String connectionString = String.format("mongodb://%s:%s@%s/%s?ssl=%b", userName, password,
                mongoConfiguration.getMongoDBHostName(), databaseName, mongoConfiguration.useMongoDBSSL());
        encryptedValuesMap.put(EzBakePropertyConstants.MONGODB_CONNECTION_STRING, connectionString);

        String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);
        String encryptedProps = Joiner.on('\n').withKeyValueSeparator("=").join(encryptedValuesMap);
        List<ArtifactDataEntry> entries = Lists.newArrayList();
        entries.add(Utilities.createConfCertDataEntry(MONGODB_PROPERTIES_FILE_NAME, properties.getBytes()));
        entries.add(Utilities.createConfCertDataEntry(ENCRYPTED_MONGODB_PROPERTIES_FILE_NAME, encryptedProps.getBytes()));
        return entries;
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase("MongoDB");
    }

    private Mongo getMongoClient(MongoHelper mongoDBHelper) throws DeploymentException {
        try {
            return mongoDBHelper.getMongo();
        } catch (UnknownHostException ex) {
            logger.error("Failed to connect to mongo", ex);
            throw new DeploymentException("Unknown Host when connecting to Mongo: " + ex.getMessage());
        }
    }
}
