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

import ezbake.groups.common.Queryable;

import java.io.Closeable;

/**
 * CacheLayer is a wrapper around the redis operations needed to atomically set and update
 * values stored in redis
 *
 * Cache entries are said to be 'invalid' when the set contains the value -1. An invalid cache
 * should return the empty set for get requests. Invalid keys can be updated, after which the
 * set will be valid again.
 */
public interface CacheLayer<T> extends Closeable {
    public static final int OPTIMISTIC_MAX_TRIES_TO_SET = 5;

    /**
     * Cache status codes
     *
     * These values get added in the set for a particular key when the entry goes into
     * a particular state. All values >=0 indicates a health cache entry. If there are any
     * members < 0, then these codes need to be checked
     */
    public enum CacheStatusCodes {
        DISABLED("-1"),
        NEEDS_UPDATE("-2")
        ;

        final String value;
        CacheStatusCodes(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * Sets all keys returned from the wildcard key to contain the value -1
     * @param wildKey search parameter for redis.keys
     */
    public void invalidateAll(String wildKey);

    /**
     * Set all keys returned from the wildcard key to contain the "needs update" value
     * @param wildKey search parameter for redis.keys
     */
    public void markAllForUpdate(String wildKey);

    /**
     * Updates all values that currently exist in the cache
     *
     * This will get keys from the cache and recompute the values for each key
     *
     * @param query query to run for the updates
     * @throws Exception
     */
    public void updateAll(Queryable<T> query) throws Exception;

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
    public T get(Queryable<T> query) throws Exception;
}

