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

 package ezbake.groups.service.caching;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import ezbake.crypto.PKeyCryptoException;
import ezbake.groups.common.InvalidCacheKeyException;
import ezbake.groups.common.Queryable;
import ezbake.groups.service.GroupsServiceModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisCacheLayer implements CacheLayer<Set<Long>> {
    private static final Logger logger = LoggerFactory.getLogger(RedisCacheLayer.class);
    public static final String SIGNATURE_KEY_PREFIX = "SIGNATURE";

    private JedisPool jedisPool;
    protected SigningChecksumProvider checksumCompute;
    private boolean shouldLogTimers;

    public RedisCacheLayer(JedisPool jedisPool, SigningChecksumProvider signingChecksumProvider) {
        this(jedisPool, signingChecksumProvider, false);
    }

    @Inject
    public RedisCacheLayer(JedisPool jedisPool, SigningChecksumProvider checksumCompute,
                      @Named(GroupsServiceModule.LOG_TIMER_STATS_NAME) Boolean shouldLogTimers) {
        this.jedisPool = jedisPool;
        this.checksumCompute = checksumCompute;
        this.shouldLogTimers = shouldLogTimers;
    }

    /**
     * Clean up the jedis pool
     */
    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
    }

    /**
     * Sets all keys returned from the wildcard key to contain the value -1
     * @param wildKey search parameter for redis.keys
     */
    public void invalidateAll(String wildKey) {
        setAllKeysToValue(wildKey, CacheStatusCodes.DISABLED.getValue());
    }

    /**
     * Set all keys returned from the wildcard key to contain the "needs update" value
     * @param wildKey search parameter for redis.keys
     */
    public void markAllForUpdate(String wildKey) {
        setAllKeysToValue(wildKey, CacheStatusCodes.NEEDS_UPDATE.getValue());
    }

    /**
     * Updates all values that currently exist in the cache
     *
     * This will get keys from the cache and recompute the values for each key
     *
     * @param query query to run for the updates
     * @throws Exception
     */
    public void updateAll(Queryable<Set<Long>> query) throws Exception {
        String keyPattern = query.getWildCardKey();

        Stopwatch timer = getStopwatch();
        Jedis jedis = jedisPool.getResource();
        try {
            Set<String> keys = jedis.keys(keyPattern);
            // Try to run the base query if the key wasn't in the set
            if (!keys.contains(query.getKey())) {
                forceUpdate(jedis, query);
            }
            // Force an update for all the keys currently in redis
            for (String key : keys) {
                try {
                    query.updateInstanceByKey(key);
                    forceUpdate(jedis, query);
                } catch (InvalidCacheKeyException e) {
                    logger.info("Failed to update query for key: {}. Will delete", key);
                    jedis.del(key);
                }
            }
        } finally {
            jedisPool.returnResource(jedis);
            logStopwatch(timer, "Cache updateAll. %s", keyPattern);
        }
    }

    /**
     * Get the value represented by the query
     *
     * If the value exists in the cache, great. Otherwise, run the query, set the value, and return.
     *
     * When the cache entry is 'invalid' (-1), return the empty set
     *
     * @param query query to run
     * @return the value associated with the query
     * @throws Exception
     */
    public Set<Long> get(Queryable<Set<Long>> query) throws Exception {
        String key = query.getKey();

        // Attempt to read from cache
        Stopwatch timer = getStopwatch();

        Jedis jedis = jedisPool.getResource();
        try {
            Transaction multi = jedis.multi();
            Response<Boolean> exists = multi.exists(key);
            Response<Set<String>> members = multi.smembers(key);
            Response<byte[]> signature = multi.get(getSignatureKey(key));
            multi.exec();

            if (!exists.get()) {
                return forceUpdate(jedis, query);
            }

            try {
                Set<Long> values = query.getFromCachable(members.get());
                if (!checksumCompute.verifyChecksumSignature(values, key, signature.get())) {
                    throw new Exception(
                            "Invalid checksum encountered for key: " + key + " Offending values: " + values +
                                    " signature " + (signature.get() == null));
                }

                if (doesCacheEntryNeedUpdate(members.get())) {
                    // We're going to set the value
                    return forceUpdate(jedis, query);
                } else if (isCacheEntryInvalid(members.get())) {
                    return query.getInvalidResult();
                }

                return values;
            } catch (Exception e) {
                logger.warn("Failed getting from query: {}", e.getMessage());
                return forceUpdate(jedis, query);
            }
        } finally {
            jedisPool.returnResource(jedis);
            logStopwatch(timer, "Cache query %s", key);
        }
    }

    /**
     * Run an update for the given query
     *
     * This uses optimistic locking to ensure that the update operation is successful. It will make up to
     * OPTIMISTIC_MAX_TRIES_TO_SET attempts, and will run the query again after each failed update
     *
     * @param jedis redis client
     * @param query query to run
     * @return the collection that was returned by the query
     * @throws Exception
     */
    private Set<Long> forceUpdate(Jedis jedis, Queryable<Set<Long>> query) throws Exception {
        String key = query.getKey();

        Stopwatch timer = getStopwatch();

        Set<Long> value = null;
        int tries = 0;
        while (tries < OPTIMISTIC_MAX_TRIES_TO_SET) {
            // Watch key for modifications while querying database
            jedis.watch(key);

            try {
                value = query.runQuery();
            } catch (Exception e) {
                logger.error("Cache failed to run query to populate values", e);
                jedis.unwatch();
                throw e;
            }

            Collection<String> saddAuths = query.transformToCachable(value);

            // Atomic delete and update of set type
            Transaction multi = jedis.multi();
            multi.del(key);
            multi.sadd(key, saddAuths.toArray(new String[saddAuths.size()]));
            multi.set(getSignatureKey(key), checksumCompute.getChecksumSignature(value, key));

            // If successful... return auths
            if (multi.exec() != null) {
                break;
            }
            logStopwatch(timer, "Failed Optimistic Lock. Attempt: %d", tries);
            timer.reset();
            tries += 1;
        }
        return value;
    }

    /**
     * Get keys back using the given wildcard for redis KEYS command
     * @param wildKey search parameter for redis KEYS
     * @param value value to set all keys to
     */
    private void setAllKeysToValue(String wildKey, String value) {
        Stopwatch timer = getStopwatch();
        Jedis jedis = jedisPool.getResource();
        try {
            Set<String> keys = jedis.keys(wildKey);
            Transaction multi = jedis.multi();
            for (String key : keys) {
                multi.del(key);
                multi.sadd(key, value);
                try {
                    multi.set(getSignatureKey(key), checksumCompute.getChecksumSignature(value, key));
                } catch (PKeyCryptoException e) {
                    logger.warn("Failed to set signature for: {} -> {}", key, value);
                }
            }
            multi.exec();
        } finally {
            jedisPool.returnResource(jedis);
            logStopwatch(timer, "Cache invalidateAll. %s", wildKey);
        }
    }

    /**
     * Determine if a particular cache entry was empty. This method depends on 2 values from redis:
     * - 1, does the key exist
     * - 2, do the members contain the invalid cache key entry
     *
     * @param members members associated with the key
     * @return true if the entry is in the invalid state
     */
    private boolean isCacheEntryInvalid(Set<String> members) {
        return members.contains(CacheStatusCodes.DISABLED.getValue());
    }

    /**
     *
     * Determine if a particular cache entry was in need of update. This method depends on 2 values from redis:
     * - 1, does the key exist
     * - 2, do the members contain the invalid cache key entry
     *
     * @param members members associated with the key
     * @return true if the entry is in the needs update state
     */
    private boolean doesCacheEntryNeedUpdate(Set<String> members) {
        return members.contains(CacheStatusCodes.NEEDS_UPDATE.getValue());
    }

    private byte[] getSignatureKey(String key) {
        return Joiner.on(Queryable.KEY_SEPARATOR).join(SIGNATURE_KEY_PREFIX, key).getBytes();
    }

    private Stopwatch getStopwatch() {
        Stopwatch watch = Stopwatch.createUnstarted();
        if (shouldLogTimers) {
            watch.start();
        }
        return watch;
    }

    private void logStopwatch(Stopwatch timer, String message, Object... args) {
        if (shouldLogTimers) {
            message = String.format(message, args);
            logger.info("TIMER: {} ----------> {}ms", message, timer.elapsed(TimeUnit.MILLISECONDS));
        }
    }

}
