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

package ezbake.groups.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.thinkaurelius.titan.core.TitanGraph;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.groups.graph.api.GroupIDProvider;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbakehelpers.ezconfigurationhelpers.redis.RedisConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 6/16/14
 * Time: 12:09 PM
 */
public class EzGroupsTestModule extends AbstractModule {
    private static final int REDIS_PORT_DEFAULT = 6379;

    Properties p;
    public EzGroupsTestModule(Properties p) {
        this.p = p;
    }

    @Override
    protected void configure() {
        bind(TitanGraph.class).toProvider(TitanGraphProvider.class);
        bind(GroupIDProvider.class).to(NaiveIDProvider.class);
    }

    @Provides
    public Properties getEzProperties() {
        return p;
    }

    @Provides
    public JedisPool provideJedis() {
        if (p.getProperty(EzBakePropertyConstants.REDIS_PORT) == null || p.getProperty(EzBakePropertyConstants.REDIS_PORT).isEmpty()) {
            p.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(REDIS_PORT_DEFAULT));
        }
        RedisConfigurationHelper rc = new RedisConfigurationHelper(p);
        return new JedisPool(new JedisPoolConfig(),rc.getRedisHost(), rc.getRedisPort());
    }

    @Provides
    public CuratorFramework provideCurator() {
        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(p);
        return CuratorFrameworkFactory.builder()
                .connectString(zc.getZookeeperConnectionString())
                .retryPolicy(new RetryNTimes(5, 1000))
                .build();
    }
}
