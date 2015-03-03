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

package ezbake.services.provenance.idgenerator;

import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.services.provenance.thrift.ProvenanceServiceImpl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Properties;

public class RedisIdProvider implements IdProvider {
    private String idGeneratorKey;
    private static JedisPool pool;

    public RedisIdProvider(final Properties properties) {
        EzProperties ezProperties = new EzProperties(properties, true);
        String host = ezProperties.getProperty(EzBakePropertyConstants.REDIS_HOST, "localhost");
        int port = ezProperties.getInteger(EzBakePropertyConstants.REDIS_PORT, 6379);
        this.idGeneratorKey = ezProperties.getProperty(ProvenanceServiceImpl.ID_GENERATOR_KEY, "provenance-id-generator");
        pool = new JedisPool(host, port);
    }

    @Override
    public void shutdown() {
        if (null != pool) {
            pool.destroy();
        }
    }

    @Override
    public long getNextId(ID_GENERATOR_TYPE type) throws IdGeneratorException {
        if (pool == null) {
            throw new IdGeneratorException("Redis not connected");
        }

        Jedis jedis = null;

        try {
            jedis = pool.getResource();

            switch (type) {
                case DocumentType:
                case AgeOffRule:
                case PurgeEvent:
                    return jedis.hincrBy(this.idGeneratorKey, type.toString(), 1);
                default:
                    throw new IdGeneratorException("Not supported ID Generator Type: " + type);
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    @Override
    public long getNextNId(ID_GENERATOR_TYPE type, long delta) throws IdGeneratorException {
        if (pool == null) {
            throw new IdGeneratorException("Redis not connected");
        }

        Jedis jedis = null;

        try {
            jedis = pool.getResource();

            switch (type) {
                case DocumentType:
                case AgeOffRule:
                case PurgeEvent:
                    return jedis.hincrBy(this.idGeneratorKey, type.toString(), delta);
                default:
                    throw new IdGeneratorException("Not supported ID Generator Type: " + type);
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    @Override
    public long getCurrentValue(ID_GENERATOR_TYPE type) throws IdGeneratorException {
        if (pool == null) {
            throw new IdGeneratorException("Redis not connected");
        }

        Jedis jedis = null;

        try {
            jedis = pool.getResource();

            switch (type) {
                case DocumentType:
                case AgeOffRule:
                case PurgeEvent:
                    return Long.parseLong(jedis.hget(this.idGeneratorKey, type.toString()));
                default:
                    throw new IdGeneratorException("Not supported ID Generator Type: " + type);
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    @Override
    public void setCurrentValue(ID_GENERATOR_TYPE type, long value) throws IdGeneratorException {
        if (pool == null) {
            throw new IdGeneratorException("Redis not connected");
        }

        Jedis jedis = null;
        try {
            jedis = pool.getResource();

            switch (type) {
                case DocumentType:
                case AgeOffRule:
                case PurgeEvent:
                    jedis.hset(this.idGeneratorKey, type.toString(), Long.toString(value));
                    break;
                default:
                    throw new IdGeneratorException("Not supported ID Generator Type: " + type);
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

}
