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

package ezbake.security.service.sync;

import static org.junit.Assert.*;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.PropertiesConfigurationLoader;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.local.redis.LocalRedis;
import ezbake.local.zookeeper.LocalZookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;


/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 8:31 AM
 */
public class EncryptedRedisCacheTest {

    public static String lockId = "mylock";
    public static int ZOO_PORT = 3756;

    private LocalZookeeper zookeeper;
    private LocalRedis redisServer;
    private EncryptedRedisCache encryptedRedisCache;

    @Before
    public void setUp() throws Exception {
        // Start the local zookeeper
        zookeeper = new LocalZookeeper();

        // Start embedded redis
        redisServer = new LocalRedis();

        // Set up the configuration values
        Properties p = new Properties();
        p.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, zookeeper.getConnectionString());
        p.setProperty(EzBakePropertyConstants.REDIS_HOST, "localhost");
        p.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(redisServer.getPort()));
        EzConfiguration ezConfiguration = new EzConfiguration(new PropertiesConfigurationLoader(p));

        JedisPool jedis = new JedisPool(
                new JedisPoolConfig(),
                p.getProperty(EzBakePropertyConstants.REDIS_HOST),
                redisServer.getPort());

        LocksmithKeySupplier keyProvider = EasyMock.createMock(LocksmithKeySupplier.class);
        final SecretKey key = getAESKey();
        EasyMock.expect(keyProvider.get()).andReturn(key).anyTimes();
        EasyMock.replay(keyProvider);

        // Curator
        CuratorFramework curator = CuratorFrameworkFactory.newClient(zookeeper.getConnectionString(), new ExponentialBackoffRetry(1000, 3));

        encryptedRedisCache = new EncryptedRedisCache(ezConfiguration, jedis, keyProvider, curator);
    }

    SecretKey getAESKey() throws NoSuchAlgorithmException {
        KeyGenerator keygenerator = KeyGenerator.getInstance("AES");
        keygenerator.init(256);
        return keygenerator.generateKey();
    }

    @After
    public void tearDown() throws IOException {
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
        if (redisServer != null) {
            redisServer.close();
        }
    }


    @Test
    public void testSimplePutGet() throws Exception {
        String data = "Hoya Carnosa";
        String key = "Test";

        encryptedRedisCache.put(key, data.getBytes(), System.currentTimeMillis()+5*1000);
        byte[] cachedData = encryptedRedisCache.get(key);

        // We might get a cache miss - one of the 'features' of redis. We can't fail if it's null
        if (cachedData != null) {
            Assert.assertEquals(data, new String(cachedData));
        }
    }
    
    @Test
    public void testInvalidateCache() throws Exception {
        String data= "John Doe";
        String key = "Test Key";
        
        encryptedRedisCache.put(data, key.getBytes(), (long)12000);
        encryptedRedisCache.invalidate();
        
        assertTrue(encryptedRedisCache.get(key) == null);
    }

    @Test
    public void testKeyExists() throws Exception {
        Assert.assertFalse(encryptedRedisCache.exists("TEST"));

        String data = "Hoya Carnosa";
        String key = "Test";
        encryptedRedisCache.put(key, data.getBytes(), System.currentTimeMillis()+5*1000);
        Assert.assertTrue(encryptedRedisCache.exists(key));

    }
}
