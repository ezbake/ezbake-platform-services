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

package ezbake.security.service.modules;

import com.google.common.base.Splitter;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.crypto.utils.EzSSL;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.persistence.AppPersistenceModule;
import ezbake.security.service.ServiceTokenProvider;
import ezbake.security.service.registration.ClientLookup;
import ezbake.security.service.registration.EzbakeRegistrationService;
import ezbake.security.service.sync.EncryptedRedisCache;
import ezbake.security.service.sync.LocksmithKeySupplier;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.redis.RedisConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/11/14
 * Time: 9:28 AM
 */
public class EzSecurityModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(EzSecurityModule.class);

    public static final String EZ_SECURITY_PLUGIN_MODULES = "ezbake.security.guice.modules";
    public static final String EZ_SECURITY_CACHE_KEY_NAMESPACE = "ezbake.security.service.sync.cache";

    public static final String ENCRYPTION_KEY_ID = "REDIS_CACHING_KEY";

    private EzProperties properties;
    private List<String> modules = new ArrayList<>();
    public EzSecurityModule() {
        this(null);
    }

    public EzSecurityModule(Properties properties) {
        this.properties = new EzProperties(properties, true);

        String modulesIn = properties.getProperty(EZ_SECURITY_PLUGIN_MODULES, "");
        modules = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(modulesIn);
    }

    @Override
    protected void configure() {
        install(new AppPersistenceModule(properties));

        bindConstant().annotatedWith(Names.named("Locksmith Key ID")).to(ENCRYPTION_KEY_ID);
        bindConstant().annotatedWith(Names.named(EncryptedRedisCache.KEY_NAMESPACE)).to(EZ_SECURITY_CACHE_KEY_NAMESPACE);

        bind(EzbakeRegistrationService.class).to(ClientLookup.class);
        bind(EzSecurityTokenProvider.class).to(ServiceTokenProvider.class);

        // Dynamically add modules from the configuration
        LOGGER.info("EzSecurityModule attempting to load modules: {}", modules);
        for (String moduleURL : modules) {
            try {
                Class<? extends Module> c = Class.forName(moduleURL).asSubclass(Module.class);

                Module pluginModule;
                try {
                    pluginModule = c.getDeclaredConstructor(Properties.class).newInstance(properties);
                } catch (NoSuchMethodException e) {
                    pluginModule = c.newInstance();
                }
                install(pluginModule);

                LOGGER.info("Module loaded successfully: {}", c.getSimpleName());
            } catch (Exception e) {
                LOGGER.error("Unable to load guice module from class: {}", moduleURL, e);
            }
        }
    }

    @Provides
    @Named("server crypto")
    PKeyCrypto providerServiceCrypto() {
        try {
            return EzSSL.getCrypto(properties);
        } catch (IOException e) {
            // if in mock mode, generate a random crypto key
            if (properties.getBoolean(EzBakePropertyConstants.EZBAKE_SECURITY_SERVICE_MOCK_SERVER, false)) {
                return new RSAKeyCrypto();
            } else {
                throw new RuntimeException("Unable to load EzSecurity service crypto keys");
            }
        }
    }

    @Provides
    @Singleton
    ThriftClientPool clientPoolProvider() {
        return new ThriftClientPool(properties);
    }

    @Provides
    Properties provideEzConfiguration() {
        return properties;
    }

    @Provides
    public CuratorFramework provideCurator() {
        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(properties);
        return CuratorFrameworkFactory.builder()
                .connectString(zc.getZookeeperConnectionString())
                .retryPolicy(new RetryNTimes(5, 1000))
                .build();
    }

    @Provides
    public ServiceDiscoveryClient provideServiceDiscoveryClient() {
        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(properties);
        return new ServiceDiscoveryClient(zc.getZookeeperConnectionString());
    }

    @Provides
    Jedis provideJedisClient() {
        RedisConfigurationHelper rc = new RedisConfigurationHelper(properties);
        String redisHost = rc.getRedisHost();
        int redisPort = rc.getRedisPort();
        return new Jedis(redisHost, redisPort);
    }

    @Provides
    JedisPool provideJedisPool() {
        RedisConfigurationHelper rc = new RedisConfigurationHelper(properties);
        String redisHost = rc.getRedisHost();
        int redisPort = rc.getRedisPort();
        return new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    }



}
