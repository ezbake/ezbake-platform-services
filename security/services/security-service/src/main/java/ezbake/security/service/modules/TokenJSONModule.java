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

import com.google.inject.AbstractModule;
import ezbake.security.common.core.TokenJSONProvider;
import ezbake.security.service.ThriftTokenJSONProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/11/14
 * Time: 10:13 AM
 */
public class TokenJSONModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenJSONModule.class);
    public static final String TOKEN_JSON_IMPL="ezbake.security.service.tokenjson";
    public static final String TOKEN_JSON_DEFAULT_IMPL = ThriftTokenJSONProvider.class.getCanonicalName();

    private String impl;

    public TokenJSONModule() {
        this(new Properties());
    }

    public TokenJSONModule(Properties p) {
        impl = p.getProperty(TOKEN_JSON_IMPL, TOKEN_JSON_DEFAULT_IMPL);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void configure() {
        try {
            bind(TokenJSONProvider.class).to((Class<? extends TokenJSONProvider>) Class.forName(impl));
        } catch (Exception e) {
            LOGGER.error("Unable to load TokenJSONModule instance from class: {}", impl, e);
        }
    }
}
