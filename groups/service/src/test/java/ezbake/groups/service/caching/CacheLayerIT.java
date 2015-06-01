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

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.groups.common.Queryable;
import ezbake.groups.service.GroupsServiceModule;
import ezbake.local.redis.LocalRedis;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

public class CacheLayerIT {

    static LocalRedis redis;
    JedisPool jedisPool;
    RedisCacheLayer cache;

    @BeforeClass
    public static void startRedis() throws IOException {
        redis = new LocalRedis();

    }

    @Before
    public void clearRedis() {
        jedisPool = new JedisPool("localhost", redis.getPort());
        Jedis redisClient = jedisPool.getResource();
        try {
            redisClient.flushAll();
        } finally {
            jedisPool.returnResource(redisClient);
        }
        cache = new RedisCacheLayer(jedisPool, new SigningChecksumProvider(new SetChecksumProvider(), new RSAKeyCrypto()));
    }

    @After
    public void cleanUpTest() {
        cache.close();
    }

    @AfterClass
    public static void cleanUpRedis() throws IOException {
        if (redis != null) {
            redis.close();
        }
    }

    @Test
    public void testGetWithUpdate() throws Exception {
        String id = "testId";
        Set<Long> values = Sets.newHashSet(1l, 2l);

        Queryable<Set<Long>> query = mockQuery(id, values, 1);
        Set<Long> cachedValue = cache.get(query);
        assertEquals(values, cachedValue);
    }

    @Test
    public void testGetCacheHit() throws Exception {
        String id = "testId";
        Set<Long> values = Sets.newHashSet(1l, 2l);

        Queryable<Set<Long>> query = mockQuery(id, values, 1, 2);

        // runs the query
        Set<Long> cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

        // hits cache
        cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

        // corrupted read from redis
        cachedValue = cache.get(query);
        assertEquals(values, cachedValue);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetCacheAddedTo() throws Exception {
        String id = "testId";
        Set<Long> values = Sets.newHashSet(1l, 2l);

        long tamperedNumber = 7l;
        Set<Long> tamperedValues = Sets.newHashSet(values);
        tamperedValues.add(tamperedNumber);

        Queryable<Set<Long>> query = createMock(Queryable.class);
        expect(query.getKey()).andReturn(id).anyTimes();

        // these run twice because of the invalid cached values
        expect(query.runQuery()).andReturn(values).times(2);
        expect(query.transformToCachable(values)).andReturn(transformLongsToStrings(values)).times(2);

        // This will return the tampered values
        expect(query.getFromCachable(transformLongsToStrings(tamperedValues))).andReturn(tamperedValues).times(1);
        replay(query);

        // runs the query
        Set<Long> cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

        Jedis jedis = jedisPool.getResource();
        try {
            jedis.sadd(query.getKey(), Long.toString(tamperedNumber, 10));
        } finally {
            jedisPool.returnResource(jedis);
        }

        // hits cache
        cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetCacheRemovedFrom() throws Exception {
        String id = "testId";
        Set<Long> values = Sets.newHashSet(1l, 2l);

        Set<Long> tamperedValues = Sets.newHashSet(values);
        tamperedValues.remove(2l);

        Queryable<Set<Long>> query = createMock(Queryable.class);
        expect(query.getKey()).andReturn(id).anyTimes();

        // these run twice because of the invalid cached values
        expect(query.runQuery()).andReturn(values).times(2);
        expect(query.transformToCachable(values)).andReturn(transformLongsToStrings(values)).times(2);

        // This will return the tampered values
        expect(query.getFromCachable(transformLongsToStrings(tamperedValues))).andReturn(tamperedValues).times(1);
        replay(query);

        // runs the query
        Set<Long> cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

        Jedis jedis = jedisPool.getResource();
        try {
            jedis.srem(query.getKey(), Long.toString(2l, 10));
        } finally {
            jedisPool.returnResource(jedis);
        }

        // hits cache
        cachedValue = cache.get(query);
        assertEquals(values, cachedValue);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetCacheNoSignature() throws Exception {
        String id = "testId";
        Set<Long> values = Sets.newHashSet(1l, 2l);

        Jedis jedis = jedisPool.getResource();
        try {
            jedis.sadd(id, transformLongsToStrings(values).toArray(new String[]{}));
        } finally {
            jedisPool.returnResource(jedis);
        }


        Queryable<Set<Long>> query = createMock(Queryable.class);
        expect(query.getKey()).andReturn(id).anyTimes();

        // This will return the tampered values
        expect(query.getFromCachable(transformLongsToStrings(values))).andReturn(values).once();
        expect(query.runQuery()).andReturn(values).once();
        expect(query.transformToCachable(values)).andReturn(transformLongsToStrings(values)).once();
        expect(query.getFromCachable(transformLongsToStrings(values))).andReturn(values).once();
        replay(query);

        // runs the query
        Set<Long> cachedValue = cache.get(query);
        assertEquals(values, cachedValue);

        // Make sure values have been set
        cachedValue = cache.get(query);
        assertEquals(values, cachedValue);


        jedis = jedisPool.getResource();
        try {
            Assert.assertTrue(jedis.exists(RedisCacheLayer.SIGNATURE_KEY_PREFIX+Queryable.KEY_SEPARATOR+id));
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Tests that invalidating a cache entry sets the value to -1 instead of just deleting the key
     * @throws Exception
     */
    @Test
    public void testInvalidate() throws Exception {
        String baseId = "testId";
        Map<String, Set<Long>> userIdsAndAuths = Maps.newHashMap();
        userIdsAndAuths.put(baseId, Sets.newHashSet(1l, 2l));
        userIdsAndAuths.put("testId1", Sets.newHashSet(3l, 4l, 5l));
        userIdsAndAuths.put("testId2", Sets.newHashSet(7l, 8l));

        // Set a few values for base key
        for (String userId : userIdsAndAuths.keySet()) {
            cache.get(mockQuery(userId, userIdsAndAuths.get(userId), 1));
        }

        cache.invalidateAll(baseId+"*");
        Jedis redisClient = jedisPool.getResource();
        try {
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId"));
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId1"));
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId2"));
        } finally {
            jedisPool.returnResource(redisClient);
        }
    }

    @Test
    public void testInvalidateAndGet() throws Exception {
        cache = new RedisCacheLayer(jedisPool, new MockSigningChecksum());

        String baseId = "testId";
        Map<String, Set<Long>> userIdsAndAuths = Maps.newHashMap();
        userIdsAndAuths.put(baseId, Sets.newHashSet(1l, 2l));
        userIdsAndAuths.put("testId1", Sets.newHashSet(3l, 4l, 5l));
        userIdsAndAuths.put("testId2", Sets.newHashSet(7l, 8l));

        // Set a few values for base key
        for (String userId : userIdsAndAuths.keySet()) {
            cache.get(mockQuery(userId, userIdsAndAuths.get(userId), 1));
        }

        cache.invalidateAll(baseId+"*");
        Jedis redisClient = jedisPool.getResource();
        // Make sure caches are invalid
        try {
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId"));
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId1"));
            assertEquals(Sets.newHashSet("-1"), redisClient.smembers("testId2"));
        } finally {
            jedisPool.returnResource(redisClient);
        }

        for (String userId : userIdsAndAuths.keySet()) {
            assertEquals(
                    Collections.<String>emptySet(),
                    cache.get(mockQuery(userId, Collections.<Long>emptySet(), 1, 1, true)));
        }
    }

    @Test
    public void testUpdateAll() throws Exception {
        String baseId = "testId";
        Map<String, Set<Long>> userIdsAndAuths = Maps.newHashMap();
        userIdsAndAuths.put(baseId, Sets.newHashSet(1l, 2l));
        userIdsAndAuths.put("testId1", Sets.newHashSet(3l, 4l, 5l));
        userIdsAndAuths.put("testId2", Sets.newHashSet(7l, 8l));

        // Imitate invalidated caches
        for (String userId : userIdsAndAuths.keySet()) {
            Jedis redisClient = jedisPool.getResource();
            // Make sure caches are invalid
            try {
                redisClient.sadd(userId, "-1");
            } finally {
                jedisPool.returnResource(redisClient);
            }
        }

        cache.updateAll(mockUpdateQuery(baseId, userIdsAndAuths));
        for (String userId : userIdsAndAuths.keySet()) {
            assertEquals(
                    userIdsAndAuths.get(userId),
                    cache.get(mockQuery(userId, userIdsAndAuths.get(userId), 0, 1)));
        }
    }

    /**
     * Test that update all will run the base query even if no keys exist
     */
    @Test
    public void testUpdateAllNoKeys() throws Exception {
        String baseId = "testId";
        Map<String, Set<Long>> userIdsAndAuths = Maps.newHashMap();
        userIdsAndAuths.put(baseId, Sets.newHashSet(1l, 2l));
        userIdsAndAuths.put("testId1", Sets.newHashSet(3l, 4l, 5l));
        userIdsAndAuths.put("testId2", Sets.newHashSet(7l, 8l));

        // Imitate invalidated caches
        for (String userId : userIdsAndAuths.keySet()) {
            // Don't update the first one just yet
            if (userId.equals(baseId)) {
                continue;
            }

            Jedis redisClient = jedisPool.getResource();
            // Make sure caches are invalid
            try {
                redisClient.sadd(userId, "-1");
            } finally {
                jedisPool.returnResource(redisClient);
            }
        }

        cache.updateAll(mockUpdateQuery(baseId, userIdsAndAuths));
        for (String userId : userIdsAndAuths.keySet()) {
            assertEquals(
                    userIdsAndAuths.get(userId),
                    cache.get(mockQuery(userId, userIdsAndAuths.get(userId), 0, 1)));
        }

    }


    Queryable<Set<Long>> mockQuery(String id, Set<Long> values, int cacheMisses) throws Exception {
        return mockQuery(id, values, cacheMisses, 0);
    }

    Queryable<Set<Long>> mockQuery(String id, Set<Long> values, int cacheMisses, int cacheHits) throws Exception {
        return mockQuery(id, values, cacheMisses, cacheHits, false);
    }

    @SuppressWarnings("unchecked")
    Queryable<Set<Long>> mockQuery(String id, Set<Long> values, int cacheMisses, int cacheHits,
                                     boolean cacheInvalid) throws Exception {
        Queryable<Set<Long>> query = createMock(Queryable.class);
        expect(query.getKey()).andReturn(id).anyTimes();
        if (cacheMisses > 0) {
            expect(query.runQuery()).andReturn(values).times(cacheMisses);
            expect(query.transformToCachable(values)).andReturn(transformLongsToStrings(values)).times(cacheMisses);
        }
        if (cacheHits > 0) {
            if (cacheInvalid) {
                expect(query.getFromCachable(Sets.newHashSet(CacheLayer.CacheStatusCodes.DISABLED.getValue())))
                        .andReturn(Sets.newHashSet(-1l)).times(cacheHits);
                expect(query.getInvalidResult()).andReturn(Collections.<Long>emptySet()).times(cacheHits);
            } else {
                expect(query.getFromCachable(transformLongsToStrings(values))).andReturn(values).times(cacheHits);
            }
        }
        replay(query);
        return query;
    }

    Collection<String> transformLongsToStrings(Set<Long> longs) {
         return Sets.newHashSet(Collections2.transform(longs, new Function<Long, String>() {
             @Nullable
             @Override
             public String apply(Long aLong) {
                 return Long.toString(aLong, 10);
             }
         }));
    }

    @SuppressWarnings("unchecked")
    Queryable<Set<Long>> mockUpdateQuery(String baseKey, Map<String, Set<Long>> keysAndValues) throws Exception {
        Queryable<Set<Long>> query = createMock(Queryable.class);
        expect(query.getWildCardKey()).andReturn(baseKey+"*").once();
        expect(query.getKey()).andReturn(baseKey).once();
        for (String userId : keysAndValues.keySet()) {
            Set<Long> values = keysAndValues.get(userId);
            query.updateInstanceByKey(userId);
            expectLastCall();
            expect(query.getKey()).andReturn(userId).once();
            expect(query.runQuery()).andReturn(keysAndValues.get(userId)).once();
            expect(query.transformToCachable(values)).andReturn(transformLongsToStrings(values)).once();
        }
        replay(query);
        return query;
    }

    private static class MockSigningChecksum extends SigningChecksumProvider {

        public MockSigningChecksum() {
            super(null, null);
        }

        @Override
        public byte[] getChecksumSignature(String data, String key) {
            return new byte[0];
        }

        @Override
        public byte[] getChecksumSignature(Set<Long> data, String key) {
            return new byte[0];
        }

        @Override
        public boolean verifyChecksumSignature(Set<Long> data, String key, byte[] signature) {
            return true;
        }
    }
}
