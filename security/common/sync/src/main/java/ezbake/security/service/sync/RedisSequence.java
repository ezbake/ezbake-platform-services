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

import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class RedisSequence {
    private Logger log = LoggerFactory.getLogger(RedisSequence.class);
    private CuratorFramework curator;
    private Jedis redis;
    private InterProcessReadWriteLock lock;
    private String zookeeperPathName;
    private String redisKeyName;
    
    private static Random random = new Random();
    
    public RedisSequence(CuratorFramework curator, Jedis redis, InterProcessReadWriteLock lock, String zookeeperPathName, String redisKeyName) throws Exception {
        this.curator = curator;
        this.redis = redis;
        this.lock = lock;
        this.zookeeperPathName= zookeeperPathName;
        this.redisKeyName= redisKeyName;
        
        long initSeq = random.nextLong();
        byte[] seqData = Long.toString(initSeq).getBytes();
        
        try {
            lock.writeLock().acquire();
            
            curator.create().forPath(zookeeperPathName, seqData);
        }
        catch(Exception e) {
            log.debug("Error: {}", e);
        }
        finally {
            lock.writeLock().release();
        }
        
        log.debug("Redis Key Name {} {}", redisKeyName.getBytes(), redisKeyName);
        
        redis.set(redisKeyName.getBytes(), seqData);
        log.debug("Cached Value {}", redis.get(redisKeyName.getBytes()));
    }
    
    private boolean sequenceValid(long zkSeq) throws Exception{
        boolean valid = false;
        
        byte[] data = redis.get(redisKeyName.getBytes());
        log.debug("Data {}", data);
        
        String strSeq = new String(data);
        long seq = Long.valueOf(strSeq); 
        valid = seq == zkSeq;

        return valid;
    }
    
    private long getZookeeperSequence() throws Exception {
        long zkSeq = 0;
        try {
            lock.readLock().acquire();
            byte[] data = curator.getData().forPath(this.zookeeperPathName);
            zkSeq = Long.valueOf(new String(data));
        }
        finally {
            lock.readLock().release();
        }
        
        return zkSeq;
    }
    
    public boolean incrementSequence() throws Exception {
        boolean incremented = false;
        
        long zkSeq = getZookeeperSequence();
        
        try {
            lock.writeLock().acquire();
            
            restoreIfInvalid(zkSeq);
 
            Long redisSeq = redis.incr(this.redisKeyName.getBytes());
            curator.setData().forPath(zookeeperPathName, redisSeq.toString().getBytes());
            incremented = true;
        }
        finally {
            lock.writeLock().release();
        }
        
        return incremented;
    }
    
    public long getSequence() throws Exception {
        long zkSeq = getZookeeperSequence();
        restoreIfInvalid(zkSeq);

        return zkSeq;
    }
    
    private void restoreIfInvalid(long zkSeq) throws Exception {
        if(!sequenceValid(zkSeq)) {
          //if invalid sequence, then restore sequence from zookeeper backup
            redis.set(redisKeyName.getBytes(), new Long(zkSeq).toString().getBytes());
        }
    }
    
    public String getRedisKeyName() {
        return this.redisKeyName;
    }
    
    public String getZookeeperPathName() {
        return this.zookeeperPathName;
    }
}
