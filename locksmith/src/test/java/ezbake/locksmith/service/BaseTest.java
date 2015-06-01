/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.locksmith.service;

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.locksmith.db.MongoDBService;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

import java.util.Properties;

public class BaseTest {
    
    private static Logger log = LoggerFactory.getLogger(BaseTest.class);
    protected static MongodProcess mongod;
    
    protected static int port = 27017;
    protected static Properties ezConfiguration;
    
    public static void init() throws Exception {
        ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        ezConfiguration.setProperty(EzBakePropertyConstants.MONGODB_HOST_NAME, "localhost");
        ezConfiguration.setProperty(EzBakePropertyConstants.MONGODB_PORT, Integer.toString(port));

        MongodStarter starter = MongodStarter.getDefaultInstance();
        
        IMongodConfig mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION).net(new Net(port, Network.localhostIsIPv6())).build();
        
        mongod = starter.prepare(mongodConfig).start();

        MongoConfigurationHelper mc = new MongoConfigurationHelper(ezConfiguration, new SystemConfigurationHelper(ezConfiguration).getTextCryptoProvider());
        MongoClient mongoClient = new MongoClient("localhost", port);
        DB db = mongoClient.getDB(mc.getMongoDBDatabaseName());
        
        db.addUser(mc.getMongoDBUserName(), mc.getMongoDBPassword().toCharArray());
    }
    
    @AfterClass
    public static void reufe() {
        if (mongod != null) {
            mongod.stop();
        }

    }
}
