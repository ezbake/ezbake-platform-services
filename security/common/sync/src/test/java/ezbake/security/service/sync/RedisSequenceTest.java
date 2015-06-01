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

import java.io.IOException;

import ezbake.local.redis.LocalRedis;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.local.zookeeper.LocalZookeeper;
import redis.clients.jedis.Jedis;

public class RedisSequenceTest {

    private static Logger log = LoggerFactory.getLogger(RedisSequenceTest.class);
    
    public static String zookeeperLockPath = "/ezSecurity/cache/lock";
    public static String zookeeperSeqPath = "/ezSecurity/cache/seq";
    
    public static String redisSeqKey = "redisSeqKey";
    
    private static CuratorFramework curator;
    private static Jedis redis;
    private static InterProcessReadWriteLock lock;
    private static LocalZookeeper localZk;
    private static LocalRedis redisServer;
    
    @BeforeClass
    public static void init() throws Exception { 
        localZk = new LocalZookeeper();
        redisServer = new LocalRedis();
        redis = new Jedis("localhost", redisServer.getPort());
        
        curator = CuratorFrameworkFactory.newClient(localZk.getConnectionString(), new ExponentialBackoffRetry(1000, 3));
        curator.start();
        lock = new InterProcessReadWriteLock(curator, zookeeperLockPath);
        
    }
    
    @Before
    public void before() throws Exception {
        localZk = new LocalZookeeper();
        curator = CuratorFrameworkFactory.newClient(localZk.getConnectionString(), new ExponentialBackoffRetry(1000, 3));
        curator.start();
        lock = new InterProcessReadWriteLock(curator, zookeeperLockPath);
    }
    
    @After
    public void after() {
        curator.close();
    }

    @AfterClass
    public static void stopClass() throws IOException {
        if (redisServer != null) {
            redisServer.close();
        }
    }

    @Test
    public void testInit() throws Exception {
        RedisSequence redisSeq = new RedisSequence(curator, redis, lock, zookeeperSeqPath, redisSeqKey);
        
        long zkSeq = Long.valueOf(new String(curator.getData().forPath(zookeeperSeqPath))).longValue();
        
        log.debug("Zookeeper Initial Sequence: {}", zkSeq);
        
        assertTrue(curator.getData().forPath(zookeeperSeqPath) != null);
        assertTrue(redisSeq.getSequence() == zkSeq);
    }
    
    @Test
    public void testIncrement() throws Exception {
        RedisSequence redisSeq = new RedisSequence(curator, redis, lock, zookeeperSeqPath, redisSeqKey);
        
        long seq = redisSeq.getSequence();
        boolean incSeq = redisSeq.incrementSequence();
        
        assertTrue(incSeq);
        assertTrue(seq+1 == redisSeq.getSequence());
    }
    
    @Test
    public void testRestore() throws Exception {
        RedisSequence redisSeq = new RedisSequence(curator, redis, lock, zookeeperSeqPath, redisSeqKey);
        long seq = redisSeq.getSequence();
        
        String data = Long.valueOf(123456789).toString();
        
        redis.set(redisSeqKey.getBytes(), data.getBytes());
        
        long testSeq = redisSeq.getSequence();
        
        assertTrue(seq == testSeq);
    }
    
    @Test
    public void testRestoreOnIncrement() throws Exception {
        RedisSequence redisSeq = new RedisSequence(curator, redis, lock, zookeeperSeqPath, redisSeqKey);
        
        long seq = redisSeq.getSequence();
        
        String data = Long.valueOf(123456).toString();
        
        redis.set(redisSeqKey.getBytes(), data.getBytes());
        
        boolean incremented = redisSeq.incrementSequence();
        
        assertTrue(incremented);
        assertTrue(seq+1 == redisSeq.getSequence());
    }
}
