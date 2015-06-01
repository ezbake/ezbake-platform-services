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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import ezbake.configuration.EzConfiguration;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbakehelpers.ezconfigurationhelpers.redis.RedisConfigurationHelper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.crypto.*;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 7/11/14
 * Time: 4:13 PM
 */
public class EncryptedRedisCache implements EzSecurityRedisCache {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRedisCache.class);
    private static final int REDIS_DB_DEFAULT = 4;

    public static final String ZK_LOCK_NODE = "/ezsecurity/cache/lock";
    public static final String KEY_NAMESPACE = "Encrypted Redis Cache Namespace";

    private JedisPool jedisPool;
    private int redisDbIndex;
    private CuratorFramework curator;

    /* This cache is intended to hold only one key, which allows caching but also refresh at some interval */
    private LoadingCache<Integer, SecretKey> locksmithKeySupplier;

    private InterProcessReadWriteLock lock;
    private ConnectionState connectionState;
    private final Object connectionStateLock = new Object();

    @Inject
    @Named(KEY_NAMESPACE)
    private String namespace;

    @Inject
    public EncryptedRedisCache(EzConfiguration ezConfiguration, JedisPool redis, LocksmithKeySupplier keyProvider, CuratorFramework curator) {
        this.jedisPool = redis;
        this.lock = new InterProcessReadWriteLock(curator, ZK_LOCK_NODE);
        this.curator = curator;

        // Connect to curator (if not connected)
        if (curator.getState() != CuratorFrameworkState.STARTED) {
            curator.start();
        }

        // Register a connection state listener
        curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                setConnectionState(connectionState);
            }
        });

        redisDbIndex = new RedisConfigurationHelper(ezConfiguration.getProperties()).getRedisDb(REDIS_DB_DEFAULT);

        locksmithKeySupplier = CacheBuilder.newBuilder()
                .maximumSize(1)
                .refreshAfterWrite(1, TimeUnit.HOURS)
                .build(new LocksmithKeyCacheLoader<Integer>(keyProvider));
    }

    private void setConnectionState(ConnectionState connectionState) {
        synchronized (connectionStateLock) {
            this.connectionState = connectionState;
        }
    }
    private ConnectionState getConnectionState() {
        synchronized (connectionStateLock) {
            return connectionState;
        }
    }

    private SecretKey getEncryptionKey() throws KeyNotFoundException {
        try {
            return locksmithKeySupplier.get(0);
        } catch (ExecutionException e) {
            logger.warn("Failed getting encryption key from locksmith: {}", e.getMessage());
            throw new KeyNotFoundException("Failed getting encryption key from locksmith: " + e.getMessage());
        }
    }

    /**
     * Add a value to the cache. The value will be encrypted with the key provided by the locksmith provider, and set
     * in Redis. The expireAt field will be used to tell redis when to discard the value
     *
     * @param key key to be used for storing this value
     * @param value a raw value to be encypted and stored with the associated key
     * @param expireAt a unix timestamp, in UTC milliseconds, when the value should expire
     * @throws Exception
     */
    @Override
    public void put(String key, byte[] value, long expireAt) throws Exception {
        put(key, value, expireAt, 250, TimeUnit.MILLISECONDS);
    }

    /**
     * Add a value to the cache. The value will be encrypted with the key provided by the locksmith provider, and set
     * in Redis. The expireAt field will be used to tell redis when to discard the value
     *
     * @param key key to be used for storing this value
     * @param value a raw value to be encypted and stored with the associated key
     * @param expireAt a unix timestamp, in UTC milliseconds, when the value should expire
     * @param timeout a timeout for acquiring the write lock, an exception will be thrown if we cannot acquire the
     *                lock in that time
     * @param timeUnit the timeunit to be used for the associated timeout
     * @throws Exception
     */
    @Override
    public void put(String key, byte[] value, long expireAt, long timeout, TimeUnit timeUnit) throws Exception {
        logger.info("Acquiring write lock for accessing the redis cache");
        if (!lock.writeLock().acquire(timeout, timeUnit)) {
            logger.info("Failed to acquire write lock after {} {} (lock acquired in this proces? {})", timeout,
                    timeUnit, lock.writeLock().isAcquiredInThisProcess());
            logger.debug("Things: {}", curator.getChildren().forPath(ZK_LOCK_NODE));
            throw new IllegalStateException("Client could not acquire lock on put");
        }
        try {
            // Encrypt the passed bytes
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, getEncryptionKey());

            value = cipher.doFinal(value);
            logger.info("Writing {} to redis. will expire at {}", key, expireAt);

            // Convert the timeout from milliseconds to seconds
            long expireAtInSeconds = expireAt / 1000l;

            // Obfuscate the key
            String obfuscatedKey = namespaceAndObfuscateKey(key);

            Jedis jedis = jedisPool.getResource();
            try {
                jedis.select(redisDbIndex);
                Pipeline setPipe = jedis.pipelined();
                setPipe.set(obfuscatedKey.getBytes(), value);
                setPipe.expireAt(obfuscatedKey, expireAtInSeconds);
                setPipe.sync();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        } catch (NoSuchPaddingException|NoSuchAlgorithmException|IllegalBlockSizeException|KeyNotFoundException|
                BadPaddingException|InvalidKeyException encryptException) {
            logger.error("Caught an exception in the encryption/put block", encryptException.getMessage());
            throw new Exception("Unable to encrypt: "+encryptException.getMessage(), encryptException);
        } finally {
            logger.info("Releasing the write lock");
            lock.writeLock().release();
        }

    }

    @Override
    public byte[] get(String key) throws Exception {
        return get(key, 250, TimeUnit.MILLISECONDS);
    }

    @Override
    public byte[] get(String key, long timeout, TimeUnit timeUnit) throws Exception {
        byte[] value;

        logger.info("Acquiring read lock for accessing the redis cache");
        if (!lock.readLock().acquire(timeout, timeUnit)) {
            logger.info("Failed to acquire write lock after {} {} (lock acquired in this process? {})", timeout,
                    timeUnit, lock.writeLock().isAcquiredInThisProcess());
            throw new IllegalStateException("Client could not acquire lock on get");
        }
        // Wrapping everyting in this try finally block so we always release the lock
        try {
            Jedis jedis;
            try {
                jedis = jedisPool.getResource();
            } catch (JedisConnectionException e) {
                throw new Exception("Unable to get a connection to redis", e);
            }

            try {
                logger.debug("Performing cache lookup for key: {}", key);
                // Obfuscate the key
                String obfuscatedKey = namespaceAndObfuscateKey(key);

                // Get the value out of redis
                jedis.select(redisDbIndex);
                value = jedis.get(obfuscatedKey.getBytes());
                if (value != null) {
                    // also get the expires
                    long ttl = jedis.ttl(obfuscatedKey.getBytes());
                    logger.info("Read {} from redis. Key will expires in {} seconds ", key, ttl);

                    // Decrypt the value
                    Cipher cipher = Cipher.getInstance("AES");
                    cipher.init(Cipher.DECRYPT_MODE, getEncryptionKey());

                    value = cipher.doFinal(value);
                } else {
                    logger.debug("Cache miss for key: {}", key);
                }
            } catch (Exception decryptException) {
                logger.error("Error decrypting value for key: {}. Clearing from cache", key, decryptException);
                jedis.select(redisDbIndex);
                jedis.del(key);
                throw new Exception("Unable to decrypt the returned value!", decryptException);
            } finally {
                jedisPool.returnResource(jedis);
            }
        } finally {
            logger.info("Releasing the read lock");
            lock.readLock().release();
        }

        return value;
    }

    @Override
    public boolean exists(String key) throws Exception {
        return exists(key, 250, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean exists(String key, long timeout, TimeUnit timeUnit) throws Exception {
        logger.info("Acquiring read lock for accessing the redis cache");
        if (!lock.readLock().acquire(timeout, timeUnit)) {
            logger.info("Failed to acquire write lock after {} {} (lock acquired in this process? {})", timeout,
                    timeUnit, lock.writeLock().isAcquiredInThisProcess());
            throw new IllegalStateException("Client could not acquire read lock");
        }
        boolean exists;
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.select(redisDbIndex);
            exists = jedis.exists(namespaceAndObfuscateKey(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            logger.info("Releasing the read lock");
            lock.readLock().release();
        }
        return exists;
    }

    private String namespaceAndObfuscateKey(final String key) {
        String resultingKey = namespace + "." + key;
        resultingKey = DigestUtils.md5Hex(resultingKey);
        return resultingKey;
    }

    @Override
    public boolean invalidate() {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.select(redisDbIndex);
            jedis.flushDB();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (curator != null) {
            curator.close();
        }
        if (jedisPool != null) {
            jedisPool.destroy();
        }
    }
}
