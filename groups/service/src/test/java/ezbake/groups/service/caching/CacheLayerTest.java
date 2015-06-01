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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.crypto.PKeyCryptoException;
import ezbake.groups.common.InvalidCacheKeyException;
import ezbake.groups.common.Queryable;
import ezbake.groups.service.query.AuthorizationQuery;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import javax.annotation.Nullable;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.List;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class CacheLayerTest {

    /**
     * Make sure close is closing the jedis pool
     */
    @Test
    public void testClose() throws IOException {
        JedisPool jedisPool = createMock(JedisPool.class);
        jedisPool.destroy();
        expectLastCall();
        replay(jedisPool);

        CacheLayer cache = new RedisCacheLayer(jedisPool, null);
        cache.close();
    }

    /**
     * Make sure that invalidateAll is calling del on all the keys in the set
     */
    @Test
    public void testInvalidateAll() throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, PKeyCryptoException {
        String invalidateKey = "abc*";
        Set<String> keys = Sets.newHashSet("abcde", "abcdef", "abciii");

        JedisPool jedisPool = createMock(JedisPool.class);
        Jedis jedis = createMock(Jedis.class);
        Transaction multi = createMock(Transaction.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);

        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.keys(invalidateKey)).andReturn(keys).once();
        expect(jedis.multi()).andReturn(multi).once();
        for (String key : keys) {
            expect(multi.del(key)).andReturn(null).once();
            expect(multi.sadd(key, CacheLayer.CacheStatusCodes.DISABLED.getValue())).andReturn(null).once();
            expect(checksumProvider.getChecksumSignature(eq(CacheLayer.CacheStatusCodes.DISABLED.getValue()), eq(key)))
                    .andReturn("SIG".getBytes()).once();
            expect(multi.set(
                    (byte[])anyObject(),  //eq((RedisCacheLayer.SIGNATURE_KEY_PREFIX+Queryable.KEY_SEPARATOR+key).getBytes()),
                    (byte[])anyObject())) //eq(mockedSignature)))
                    .andReturn(null).once();
        }
        expect(multi.exec()).andReturn(null).once();
        jedisPool.returnResource(jedis);
        expectLastCall().once();
        replay(jedisPool, jedis, multi, checksumProvider);

        CacheLayer cache = new RedisCacheLayer(jedisPool, checksumProvider);
        cache.invalidateAll(invalidateKey);

        verify(jedisPool, jedis, multi, checksumProvider);
    }

    @Test
    public void testInvalidateAllTamper() throws Exception {
        String invalidateKey = "abc*";
        String key1 = "abcde";
        Set<String> keys = Sets.newHashSet(key1, "abcdef", "abciii");
        Set<String> disabledSet = Sets.newHashSet(CacheLayer.CacheStatusCodes.DISABLED.getValue());
        byte[] mockedSignature = "SIGNATURE".getBytes();

        JedisPool jedisPool = createMock(JedisPool.class);
        Jedis jedis = createMock(Jedis.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        Transaction multi = createMock(Transaction.class);
        Queryable<Set<Long>> query = createMock(Queryable.class);

        // Jedis calls
        expect(jedisPool.getResource()).andReturn(jedis).times(2);
        jedisPool.returnResource(jedis);
        expectLastCall().times(2);

        expect(jedis.multi()).andReturn(multi).times(2);
        expect(multi.exec()).andReturn(null).times(2);

        // Request to invalidate all
        expect(jedis.keys(invalidateKey)).andReturn(keys).once();
        for (String key : keys) {
            expect(multi.del(key)).andReturn(null).once();
            expect(multi.sadd(key, CacheLayer.CacheStatusCodes.DISABLED.getValue())).andReturn(null).once();
            expect(checksumProvider.getChecksumSignature(CacheLayer.CacheStatusCodes.DISABLED.getValue(), key))
                    .andReturn(mockedSignature).once();
            expect(multi.set(
                    (byte[])anyObject(),  //eq((RedisCacheLayer.SIGNATURE_KEY_PREFIX+Queryable.KEY_SEPARATOR+key).getBytes()),
                    (byte[])anyObject())) //eq(mockedSignature)))
                    .andReturn(null).once();
        }

        // Request to query one of the keys
        expect(query.getKey()).andReturn("abcde").anyTimes();
        Response<Boolean> existsResponse = createMock(Response.class);
        Response<Set<String>> smembersResponse = createMock(Response.class);
        Response<byte[]> signatureResponse = createMock(Response.class);

        expect(multi.exists(key1)).andReturn(existsResponse).once();
        expect(multi.smembers(key1)).andReturn(smembersResponse).once();
        expect(multi.get((byte[]) anyObject())).andReturn(signatureResponse).once();
        expect(existsResponse.get()).andReturn(Boolean.TRUE).times(1);
        expect(smembersResponse.get()).andReturn(disabledSet).times(3);
        expect(signatureResponse.get()).andReturn("SIGNATURE".getBytes()).times(1);
        expect(query.getFromCachable(disabledSet)).andReturn(Sets.newHashSet(-1l)).once();
        expect(checksumProvider.verifyChecksumSignature(
                (Set<Long>)anyObject(),
                eq(key1),
                (byte[])anyObject())).andReturn(Boolean.TRUE).once();
        expect(query.getInvalidResult()).andReturn(Sets.newHashSet(-1l)).once();

        replay(jedisPool, jedis, checksumProvider, multi, query, existsResponse, smembersResponse, signatureResponse);

        CacheLayer cache = new RedisCacheLayer(jedisPool, checksumProvider);
        cache.invalidateAll(invalidateKey);
        cache.get(query);

        verify(jedisPool, jedis, checksumProvider, multi, query, existsResponse, smembersResponse, signatureResponse);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateAll() throws Exception {
        String key = "def*";
        String invalidKey = "boo";
        Set<String> keys = Sets.newHashSet("deff", "defhig", invalidKey);

        JedisPool jedisPool = createMock(JedisPool.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        Jedis jedis = createMock(Jedis.class);
        Queryable<Set<Long>> query = createMock(Queryable.class);

        expect(query.getWildCardKey()).andReturn(key).once();
        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.keys(key)).andReturn(keys).once();
        expect(query.getKey()).andReturn("def").once();
        mockForceUpdate("def", Sets.newHashSet(1l, 2l, 3l), query, jedis, checksumProvider, 1);
        for (String k : keys) {
            if (k.equals(invalidKey)) {
                query.updateInstanceByKey(k);
                expectLastCall().andThrow(new InvalidCacheKeyException(k, "test error"));
                expect(jedis.del(k)).andReturn(null).once();
            } else {
                query.updateInstanceByKey(k);
                expectLastCall();
                mockForceUpdate(k, Sets.newHashSet(1l, 2l, 3l), query, jedis, checksumProvider, 1);
            }
        }
        jedisPool.returnResource(jedis);
        expectLastCall().once();
        replay(jedisPool, checksumProvider, jedis, query);

        CacheLayer cache = new RedisCacheLayer(jedisPool, checksumProvider);
        cache.updateAll(query);
    }

    /**
     * Make sure get doesn't run the query when cache hits
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGetHit() throws Exception {
        String key = "key";
        Set<Long> members = Sets.newHashSet(1l, 2l, 3l);

        JedisPool jedisPool = createMock(JedisPool.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        Jedis jedis = createMock(Jedis.class);
        Transaction multi = createMock(Transaction.class);
        Response<Boolean> exists = createMock(Response.class);
        Response<Set<String>> membersResponse = createMock(Response.class);
        Response<byte[]> signatureResponse = createMock(Response.class);
        Queryable<Set<Long>> query = createMock(Queryable.class);

        expect(query.getKey()).andReturn(key).once();
        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.multi()).andReturn(multi).once();
        expect(multi.exists(key)).andReturn(exists).once();
        expect(multi.smembers(key)).andReturn(membersResponse).once();
        expect(multi.get((byte[]) anyObject())).andReturn(signatureResponse).once();
        expect(multi.exec()).andReturn(null).once();
        expect(exists.get()).andReturn(Boolean.TRUE).times(3);
        expect(membersResponse.get()).andReturn(transformLongsToStrings(members)).times(3);
        expect(signatureResponse.get()).andReturn("HELLO".getBytes()).times(1);
        expect(checksumProvider.verifyChecksumSignature(
                (Set<Long>)anyObject(),
                anyString(),
                (byte[])anyObject()))
                .andReturn(Boolean.TRUE).anyTimes();
        expect(query.getFromCachable(transformLongsToStrings(members))).andReturn(members).once();
        jedisPool.returnResource(jedis);
        expectLastCall().once();

        replay(jedisPool, jedis, checksumProvider, multi, exists, membersResponse, signatureResponse, query);

        CacheLayer<Set<Long>> cache = new RedisCacheLayer(jedisPool, checksumProvider);
        Set<Long> result = cache.get(query);
        Assert.assertEquals(members, result);
    }

    /**
     * Make sure get runs query when cache misses
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGetMiss() throws Exception {
        String key = "key";
        Set<Long> members = Sets.newHashSet(1l, 2l, 3l);

        JedisPool jedisPool = createMock(JedisPool.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        Jedis jedis = createMock(Jedis.class);
        Transaction multi = createMock(Transaction.class);
        Response<Boolean> exists = createMock(Response.class);
        Response<Set<String>> membersResponse = createMock(Response.class);
        Response<byte[]> signatureResponse = createMock(Response.class);
        Queryable<Set<Long>> query = createMock(Queryable.class);

        expect(query.getKey()).andReturn(key).once();
        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.multi()).andReturn(multi).once();
        expect(multi.exists(key)).andReturn(exists).once();
        expect(multi.smembers(key)).andReturn(membersResponse).once();
        expect(multi.get((byte[])anyObject())).andReturn(signatureResponse).once();
        expect(multi.exec()).andReturn(null).once();
        expect(exists.get()).andReturn(Boolean.FALSE).once();

        mockForceUpdate(key, members, query, jedis, checksumProvider, 1);

        jedisPool.returnResource(jedis);
        expectLastCall().once();
        replay(jedisPool, checksumProvider, jedis, multi, exists, membersResponse, query);

        CacheLayer<Set<Long>> cache = new RedisCacheLayer(jedisPool, checksumProvider);
        Set<Long> result = cache.get(query);
        Assert.assertEquals(members, result);
    }

    /**
     * Make sure get runs query appropriately when optimistic locking has a failure
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGetMissCollision() throws Exception {
        String key = "key";
        Set<Long> members = Sets.newHashSet(1l, 2l, 3l);

        JedisPool jedisPool = createMock(JedisPool.class);
        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        Jedis jedis = createMock(Jedis.class);
        Transaction multi = createMock(Transaction.class);
        Response<Boolean> exists = createMock(Response.class);
        Response<Set<String>> membersResponse = createMock(Response.class);
        Response<byte[]> signatureResponse = createMock(Response.class);
        Queryable<Set<Long>> query = createMock(Queryable.class);

        expect(query.getKey()).andReturn(key).once();
        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.multi()).andReturn(multi).once();
        expect(multi.exists(key)).andReturn(exists).once();
        expect(multi.smembers(key)).andReturn(membersResponse).once();
        expect(multi.get((byte[])anyObject())).andReturn(signatureResponse).once();
        expect(multi.exec()).andReturn(null).once();
        expect(exists.get()).andReturn(Boolean.FALSE).once();

        mockForceUpdate(key, members, query, jedis, checksumProvider, 2);

        jedisPool.returnResource(jedis);
        expectLastCall().once();
        replay(jedisPool, checksumProvider, jedis, multi, exists, membersResponse, query);

        CacheLayer<Set<Long>> cache = new RedisCacheLayer(jedisPool, checksumProvider);
        Set<Long> result = cache.get(query);
        Assert.assertEquals(members, result);
    }

    /**
     * Make sure query fails throw appropriately
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGetMissQueryFail() throws Exception {
        String key = "key";
        String errorMessage = "Testing Error";

        SigningChecksumProvider checksumProvider = createMock(SigningChecksumProvider.class);
        JedisPool jedisPool = createMock(JedisPool.class);
        Jedis jedis = createMock(Jedis.class);
        Transaction multi = createMock(Transaction.class);
        Response<Boolean> exists = createMock(Response.class);
        Response<Set<String>> membersResponse = createMock(Response.class);
        Queryable<Set<String>> query = createMock(Queryable.class);

        expect(query.getKey()).andReturn(key).once();
        expect(jedisPool.getResource()).andReturn(jedis).once();
        expect(jedis.multi()).andReturn(multi).once();
        expect(multi.exists(key)).andReturn(exists).once();
        expect(multi.smembers(key)).andReturn(membersResponse).once();
        expect(multi.get((byte[])anyObject())).andReturn(createMock(Response.class)).once();
        expect(multi.exec()).andReturn(null).once();
        expect(exists.get()).andReturn(Boolean.FALSE).once();

        expect(query.getKey()).andReturn(key).once();
        expect(jedis.watch(key)).andReturn(null).once();
        expect(query.runQuery()).andThrow(new Exception(errorMessage));
        expect(jedis.unwatch()).andReturn(null).once();

        jedisPool.returnResource(jedis);
        expectLastCall().once();
        replay(checksumProvider, jedisPool, jedis, multi, exists, membersResponse, query);

        CacheLayer cache = new RedisCacheLayer(jedisPool, checksumProvider);
        try {
            cache.get(query);
        } catch (Exception e) {
            Assert.assertEquals(errorMessage, e.getMessage());
        }
    }

    /**
     * @param key
     * @param members must have 3 elements...
     * @param query
     * @param jedis
     * @throws Exception
     */
    public static void mockForceUpdate(String key, Set<Long> members, Queryable<Set<Long>> query, Jedis jedis, SigningChecksumProvider signingChecksumProvider, int times) throws Exception {
        expect(query.getKey()).andReturn(key).once();
        expect(jedis.watch(key)).andReturn(null).times(times);
        expect(query.runQuery()).andReturn(members).times(times);
        expect(query.transformToCachable(members)).andReturn(transformLongsToStrings(members)).times(times);
        Transaction multi2 = createMock(Transaction.class);
        expect(jedis.multi()).andReturn(multi2).times(times);
        expect(multi2.del(key)).andReturn(null).times(times);
        // Order gets backwards...
        List<Long> resultList = Lists.newArrayList(members);
        //Collections.reverse(resultList);
        expect(multi2.sadd(
                        eq(key),
                        eq(Long.toString(resultList.get(2), 10)),
                        eq(Long.toString(resultList.get(1), 10)),
                        eq(Long.toString(resultList.get(0), 10)))
        ).andReturn(null).times(times);

        expect(multi2.set(
                (byte[])anyObject(), (byte[])anyObject())).andReturn(createMock(Response.class)).times(times);
        expect(signingChecksumProvider.getChecksumSignature(members, key)).andReturn("HELLO".getBytes()).once();

        expect(multi2.exec()).andReturn(Lists.newArrayList()).times(times);
        replay(multi2);
    }

    static Set<String> transformLongsToStrings(Set<Long> longs) {
        return Sets.newHashSet(Collections2.transform(longs, new Function<Long, String>() {
            @Nullable
            @Override
            public String apply(Long aLong) {
                return Long.toString(aLong, 10);
            }
        }));
    }
}
