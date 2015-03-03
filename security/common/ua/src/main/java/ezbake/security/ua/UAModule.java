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

package ezbake.security.ua;

import com.google.inject.AbstractModule;
import ezbake.configuration.EzConfiguration;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.api.ua.UserSearchService;
import ezbake.security.impl.ua.FileUASearch;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.service.sync.EncryptedRedisCache;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.service.sync.NoopRedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 2:55 PM
 */
public class UAModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(UAModule.class);
    public static final String UA_SERVICE_IMPL = "ezbake.security.api.ua.userImpl";
    public static final String UA_SEARCH_IMPL = "ezbake.security.api.ua.searchImpl";
    public static final String CACHE_TYPE = "ezbake.security.api.ua.cacheType";

    Properties properties;
    public UAModule(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        // Bind the UA Service impl
        String userImpl = properties.getProperty(UA_SERVICE_IMPL, FileUAService.class.getCanonicalName());
        try {
            Class<? extends UserAttributeService> implClass = Class.forName(userImpl)
                    .asSubclass(UserAttributeService.class);
            bind(UserAttributeService.class).to(implClass);
            LOGGER.info("Bound {} to {}", userImpl, UA_SERVICE_IMPL);
        } catch (Exception ignored) {
            LOGGER.warn("Unable to bind {} to {}", UA_SERVICE_IMPL, userImpl, ignored);
        }

        // Bind the Search impl
        String searchImpl = properties.getProperty(UA_SEARCH_IMPL, FileUASearch.class.getCanonicalName());
        try {
            Class<? extends UserSearchService> searchClass = Class.forName(searchImpl)
                    .asSubclass(UserSearchService.class);
            bind(UserSearchService.class).to(searchClass);
            LOGGER.info("Bound {} to {}", searchImpl, UA_SEARCH_IMPL);
        } catch (Exception ignored) {
            LOGGER.warn("Unable to bind {} to {}", UA_SEARCH_IMPL, searchImpl, ignored);
        }

        String cacheType = properties.getProperty(CACHE_TYPE);
        if ("redis".equalsIgnoreCase(cacheType)) {
            bind(EzSecurityRedisCache.class).to(EncryptedRedisCache.class);
        } else {
            bind(EzSecurityRedisCache.class).to(NoopRedisCache.class);
        }
    }
}
