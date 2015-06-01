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

package ezbake.profile.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.service.sync.EncryptedRedisCache;
import ezbake.security.service.sync.LocksmithKeySupplier;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.redis.RedisConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 8/10/14
 * Time: 3:55 PM
 */
public class EzProfileModule extends AbstractModule {
    public static final String LOCKSMITH_KEY_ID = "ezbake.profile.cache.ua.encryption_key";
    public static final String REDIS_CACHE_NAMESPACE = "ezbake.profile.cache.ua";

    private Properties ezProperties;
    public EzProfileModule(Properties p) {
        this.ezProperties = p;
    }

    @Override
    protected void configure() {
        bind(Properties.class).toInstance(ezProperties);
        bind(EzSecurityTokenProvider.class).to(EzProfileAppTokenProvider.class);

        bindConstant().annotatedWith(Names.named(LocksmithKeySupplier.KEY_ID)).to(LOCKSMITH_KEY_ID);
        bindConstant().annotatedWith(Names.named(EncryptedRedisCache.KEY_NAMESPACE)).to(REDIS_CACHE_NAMESPACE);
    }

    @Provides
    public EzbakeSecurityClient provideSecurityClient() {
        return new EzbakeSecurityClient(ezProperties);
    }

    @Provides
    @Singleton
    public JedisPool jedisPoolProvider() {
        RedisConfigurationHelper rc = new RedisConfigurationHelper(ezProperties);
        return new JedisPool(new JedisPoolConfig(), rc.getRedisHost(), rc.getRedisPort());
    }

    @Provides
    @Singleton
    public ThriftClientPool clientPoolProvider() {
        return new ThriftClientPool(ezProperties);
    }

    @Provides
    CuratorFramework provideCurator() {
        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(ezProperties);
        return CuratorFrameworkFactory.builder()
                .connectString(zc.getZookeeperConnectionString())
                .retryPolicy(new RetryNTimes(5, 500))
                .build();
    }
}
