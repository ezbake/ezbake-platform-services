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

package ezbake.profile.service;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.local.redis.LocalRedis;
import ezbake.profile.*;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.security.ua.UAModule;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;
import org.apache.thrift.TException;
import org.junit.*;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

@Ignore
public class EzProfileTest {
	private static Logger log = LoggerFactory.getLogger(EzProfileTest.class);

	private static Properties ezConfig;
	private static ThriftServerPool serverPool;
	private static ThriftClientPool clientPool;

    public static final String USER_PRINCIPAL = "tester";
	public static final String SECURITY_ID = "SecurityClientTest";


    public static final int MONGO_PORT = randomPort(22456, 23958);

    public static int randomPort(int start, int end) {
        Random portChooser = new Random();
        return portChooser.nextInt((end - start) + 1) + start;
    }

    static LocalRedis redisServer;
    static MongodProcess mongod;

	@BeforeClass
	public static void init() throws Exception {

        redisServer = new LocalRedis();

        ezConfig = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        ezConfig.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(redisServer.getPort()));
        ezConfig.setProperty(EzBakePropertyConstants.REDIS_HOST, "localhost");
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_HOST_NAME, "localhost");
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_PORT, Integer.toString(MONGO_PORT));
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_DB_NAME, "ezprofile");
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_USER_NAME, "ezprofile");
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_PASSWORD, "ezprofile");
        ezConfig.setProperty(EzBakePropertyConstants.MONGODB_USE_SSL, Boolean.FALSE.toString());
        ezConfig.setProperty(UAModule.CACHE_TYPE, "redis");
        ezConfig.setProperty(FileUAService.USERS_FILENAME, EzProfileTest.class.getResource("/"+FileUAService.userFile).getFile());


        MongodStarter starter = MongodStarter.getDefaultInstance();
        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net("localhost", MONGO_PORT, Network.localhostIsIPv6())).build();

        mongod = starter.prepare(mongodConfig).start();

        MongoConfigurationHelper mc = new MongoConfigurationHelper(ezConfig, new SystemConfigurationHelper(ezConfig).getTextCryptoProvider());
        MongoClient mongoClient = new MongoClient("localhost", MONGO_PORT);
        DB db = mongoClient.getDB(mc.getMongoDBDatabaseName());
        db.addUser(mc.getMongoDBUserName(), mc.getMongoDBPassword().toCharArray());

		serverPool = new ThriftServerPool(ezConfig, 9999);
		serverPool.startCommonService(new EzProfileHandler(), ezprofileConstants.SERVICE_NAME, SECURITY_ID);

		clientPool = new ThriftClientPool(ezConfig);
	}

    @AfterClass
    public static void clean() throws InterruptedException, IOException {
        if(serverPool != null) {
            log.info("Closing server pool");
            serverPool.shutdown();
        }
        if (redisServer != null) {
            redisServer.close();
        }
        if (mongod != null) {
            mongod.stop();
        }
    }

    @Before
    public void setUp() throws UnknownHostException {
        Jedis jedis = new Jedis("localhost", redisServer.getPort());
        jedis.flushAll();
        jedis.close();

        Mongo mongo = new MongoClient("localhost", MONGO_PORT);
        List<String> dbs = mongo.getDatabaseNames();
        for (String db : dbs) {
            mongo.dropDatabase(db);
        }
        mongo.close();
    }
	
	@Test
	public void testUserSearch() throws TException {
		EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
        try {
            SearchResult up = client.searchProfileByName(ezToken, "Daryl", "Dixon");
            Assert.assertEquals(SearchStatus.OK, up.getStatusCode());
            Assert.assertEquals(1, up.getProfilesSize());

            for (Map.Entry<String, UserProfile> profile : up.getProfiles().entrySet()) {
                Assert.assertEquals("Daryl", profile.getValue().getFirstName());
                Assert.assertEquals("Dixon", profile.getValue().getLastName());
                Assert.assertEquals("WalkingDead", profile.getValue().getOrganization());
                Assert.assertEquals("WalkerHuntersAffiliated", profile.getValue().getAffiliations().get(0));
            }
        } finally {
            clientPool.returnToPool(client);
        }
	}
	
	@Test
	public void testSearch() throws TException {
		EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
		String first = "*";
		String last = "*";

        EzProfile.Client client = null;
		try {
			client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
			SearchResult result = client.searchDnByName(ezToken, first, last);

            Assert.assertEquals(SearchStatus.OK, result.getStatusCode());
            Assert.assertEquals(2, result.getPrincipalsSize());
		} finally {
			clientPool.returnToPool(client);
		}
	}
	
	@Test
	public void testQuerySearch() throws MalformedQueryException, TException {
		String query = "Ric* Grim*";

        EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = null;
		try {
			client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
			SearchResult result = client.searchDnByQuery(ezToken, query);

            Assert.assertEquals(SearchStatus.OK, result.getStatusCode());
            Assert.assertEquals(1, result.getPrincipalsSize());
		} finally {
			clientPool.returnToPool(client);
		}
	}
	
	@Test
	public void testUserProfileQuery() throws TException, MalformedQueryException {
		String query = "*aryl* Dix*";

        EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = null;
		try {
			client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
			SearchResult result = client.searchProfileByQuery(ezToken, query);

            Assert.assertEquals(SearchStatus.OK, result.getStatusCode());
            Assert.assertEquals(1, result.getProfilesSize());
		} finally {
			clientPool.returnToPool(client);
		}
	}
	
	@Test
	public void testProfileNameSearch() throws TException {
		String first = "Dar*";
		String last = "Dix*";

        EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = null;
		try {
			client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
			SearchResult result = client.searchProfileByQuery(ezToken, first +" " + last);

            Assert.assertEquals(SearchStatus.OK, result.getStatusCode());
            Assert.assertEquals(1, result.getProfilesSize());
		} finally {
			clientPool.returnToPool(client);
		}
	}

    @Test
    public void testUserInfo() throws TException {
        String id = "ddixon.84844";

        EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = null;
		try {
			client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
			UserInfo result = client.userProfile(ezToken, id);

            Assert.assertEquals(id, result.getPrincipal());
		} finally {
			clientPool.returnToPool(client);
		}
    }

    @Test
    public void testUserProfile() throws TException {
        String id = "ddixon.84844";

        EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken(USER_PRINCIPAL);
        EzProfile.Client client = null;
        try {
            client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
            UserProfile result = client.getUserProfile(ezToken, id);

            Assert.assertEquals(id, result.getPrincipal());
        } finally {
            clientPool.returnToPool(client);
        }
    }
}
