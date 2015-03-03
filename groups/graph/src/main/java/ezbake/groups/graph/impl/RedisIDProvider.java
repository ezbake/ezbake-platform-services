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

package ezbake.groups.graph.impl;

import com.google.inject.Inject;
import ezbake.groups.graph.api.GroupIDProvider;
import ezbake.groups.graph.api.GroupIDPublisher;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 6/23/14
 * Time: 11:40 AM
 */
public class RedisIDProvider implements GroupIDProvider {
    private static final Logger logger = LoggerFactory.getLogger(RedisIDProvider.class);

    public static final String PROCESS_LOCK = "/ezbake/groups/id/lock";
    public static final String ID_VALID = "/ezbake/groups/id/valid";

    public static final String ID_REDIS_KEY = "ezbake.protect.ezgroups.graph.group.id";

    private JedisPool jedisPool;
    private InterProcessReadWriteLock lock;
    private CuratorFramework curator;
    private GroupIDPublisher idGetter;

    @Inject
    public RedisIDProvider(JedisPool redis, CuratorFramework curator, GroupIDPublisher idGetter) {
        this.idGetter = idGetter;

        jedisPool = redis;

        this.curator = curator;
        if (curator.getState() == CuratorFrameworkState.LATENT) {
            curator.start();
        }
        lock = new InterProcessReadWriteLock(curator, PROCESS_LOCK);

        try {
            setCurrentID();
        } catch (Exception e) {
            throw new RuntimeException("Unable to set the current group Id, fix errors and restart", e);
        }
    }

    /**
     * Get the current ID. In normal circumstances, this ID has already been allocated, and should only be used as a
     * reference
     *
     * @return the current ID
     */
    @Override
    public long currentID() throws Exception {
        acquireLock(lock.readLock(), 250, TimeUnit.MILLISECONDS);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return Long.parseLong(jedis.get(ID_REDIS_KEY));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            lock.readLock().release();
        }
    }

    @Override
    public void setCurrentID() throws Exception {
        acquireLock(lock.writeLock(), 250, TimeUnit.MILLISECONDS);
        Jedis jedis = null;
        try {
            // Only update the value if it is invalid
            if (isIdValid()) {
                return;
            }

            long id = idGetter.getCurrentId();
            jedis = jedisPool.getResource();
            jedis.set(ID_REDIS_KEY, String.valueOf(id));

            // Set the valid flag
            setIdValid(true);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            lock.writeLock().release();
        }
    }

    /**
     * Get the next available ID
     *
     * @return the value of the ID
     */
    @Override
    public long nextID() throws Exception {
        acquireLock(lock.writeLock(), 250, TimeUnit.MILLISECONDS);
        long id;
        try {
            Jedis jedis = jedisPool.getResource();
            try {
                boolean present = jedis.exists(ID_REDIS_KEY);
                if (!present) {
                    logger.info("Key not present in redis. Resetting the current ID");
                    setIdValid(false);
                    setCurrentID();
                }
                id = jedis.incr(ID_REDIS_KEY);
            } catch (Exception e) {
                // try once more. This might happen if there's a non integer value for the key
                logger.info("Key not available in redis. Resetting the current ID");
                setIdValid(false);
                setCurrentID();
                id = jedis.incr(ID_REDIS_KEY);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        } finally {
            lock.writeLock().release();
        }
        return id;
    }

    private void setIdValid(boolean valid) throws Exception {
        byte[] data = valid ? "1".getBytes() : "0".getBytes();
        try {
            curator.inTransaction()
                    .delete().forPath(ID_VALID)
                    .and()
                    .create().forPath(ID_VALID, data)
                    .and().commit();
        } catch (KeeperException.NoNodeException e) {
            // create the node
            curator.create().forPath(ID_VALID, data);
        } catch (KeeperException.NodeExistsException e) {
            // delete and try again
            curator.inTransaction()
                    .delete().forPath(ID_VALID)
                    .and()
                    .create().forPath(ID_VALID, data)
                    .and().commit();
        }
    }

    private boolean isIdValid() throws Exception {
        boolean valid = false;
        try {
            byte[] data = curator.getData().forPath(ID_VALID);
            if (Arrays.equals(data, "1".getBytes())) {
                valid = true;
            }
        } catch (KeeperException.NoNodeException e) {
            setIdValid(false);
        }
        return valid;
    }

    private void acquireLock(InterProcessMutex lockToAcquire, int timeout, TimeUnit unit) throws Exception {
        if (!lockToAcquire.acquire(timeout, unit)) {
            logger.error("Failed to acquire read lock in {} {}", timeout, unit);
            throw new IllegalStateException("Failed to acquire read lock in " + timeout + " " + unit.toString());
        }
    }
}
