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
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import ezbake.common.properties.EzProperties;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.crypto.utils.EzSSL;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.service.caching.CacheLayer;
import ezbake.groups.service.caching.RedisCacheLayer;
import ezbakehelpers.ezconfigurationhelpers.ssl.SslConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;
import java.util.Set;

public class GroupsServiceModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(GroupsServiceModule.class);
    public static final String LOG_TIMER_STATS_PROPERTY = "ezbake.groups.service.log.timer";
    public static final String LOG_TIMER_STATS_NAME = "SHOULD_LOG_TIMER";
    public static final String CRYPTO_NAME = "CRYPTO";

    private EzProperties configuration;
    public GroupsServiceModule(Properties configuration) {
        this.configuration = new EzProperties(configuration, true);
    }

    @Override
    protected void configure() {
        bind(BaseGroupsService.class).to(CachingEzGroupsService.class);
        bind(GroupsGraph.class).to(EzGroupsGraphImpl.class);
        bind(new TypeLiteral<CacheLayer<Set<Long>>>(){}).to(RedisCacheLayer.class);

        // set up some bound properties
        bindConstant()
                .annotatedWith(Names.named(LOG_TIMER_STATS_NAME))
                .to(configuration.getBoolean(LOG_TIMER_STATS_PROPERTY, false));
    }

    @Singleton
    @Provides
    @Named(CRYPTO_NAME)
    PKeyCrypto providerServiceCrypto() {
        SslConfigurationHelper sslHelper = new SslConfigurationHelper(configuration);
        try {
            return new RSAKeyCrypto(
                    IOUtils.toString(EzSSL.getISFromFileOrClasspath(configuration, sslHelper.getPrivateKeyFile())),
                    true);
        } catch (InvalidKeySpecException | NoSuchAlgorithmException | IOException e) {
            ThriftConfigurationHelper thriftConfigurationHelper = new ThriftConfigurationHelper(configuration);
            if (!thriftConfigurationHelper.useSSL()) {
                logger.warn(
                        "Failed to load crypto keys. Generating random keys. Not using SSL, so this might be ok");
                return new RSAKeyCrypto();
            } else {
                throw new RuntimeException("Unable to load service crypto keys");
            }
        }
    }
}
