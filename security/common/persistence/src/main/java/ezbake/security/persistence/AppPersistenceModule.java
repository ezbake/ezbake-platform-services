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

package ezbake.security.persistence;

import com.google.inject.AbstractModule;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.persistence.impl.FileRegManager;
import ezbake.security.persistence.impl.JsonFileRegManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 12/18/14
 * Time: 4:28 PM
 */
public class AppPersistenceModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(AppPersistenceModule.class);

    private Properties configuration;
    public AppPersistenceModule(Properties configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        String impl = configuration.getProperty(EzBakePropertyConstants.EZBAKE_APP_REGISTRATION_IMPL);
        if (impl != null) {
            try {
                Class<? extends RegistrationManager> clazz = Class.forName(impl)
                        .asSubclass(RegistrationManager.class);
                bind(RegistrationManager.class).to(clazz);
            } catch (Exception e) {
                logger.error("Unable to load TokenJSONModule instance from class: {}", impl, e);
            }
        } else {
            logger.info("Initializing AppPersistenceModule handler with default registration service: {}",
                    JsonFileRegManager.class.getCanonicalName());
            bind(RegistrationManager.class).to(JsonFileRegManager.class);
        }
    }
}
