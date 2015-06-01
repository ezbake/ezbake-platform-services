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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.CreationException;
import com.google.inject.spi.Message;
import ezbake.configuration.EzConfiguration;
import ezbake.security.api.ua.UserAttributeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 2:59 PM
 */
public class UserAttributeServiceProvider implements Provider<UserAttributeService> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserAttributeService.class);
    public static final String UA_SERVICE_IMPL = "ezbake.security.api.ua.userImpl";

    private EzConfiguration ezConfiguration;
    @Inject
    public UserAttributeServiceProvider(EzConfiguration ezConfiguration) {
        this.ezConfiguration = ezConfiguration;
    }

    @Override
    @SuppressWarnings("unchecked")
    public UserAttributeService get() {
        UserAttributeService service = null;
        String impl = ezConfiguration.getProperties().getProperty(UA_SERVICE_IMPL);
        List<Message> messages = new ArrayList<>();
        if (impl != null) {
            LOGGER.info("Instantiating UserAttributeService using impl: {}", impl);
            try {
                Class uaClass = Class.forName(impl);
                service = (UserAttributeService) uaClass
                        .getDeclaredConstructor(EzConfiguration.class)
                        .newInstance(ezConfiguration);
            } catch (Exception e) {
                LOGGER.error("Error instantiating the UA Service. Will return null", e);
                messages.add(new Message("Error instantiating the UA service", e.getMessage()));
            }
        } else {
            messages.add(new Message("Property: \""+UA_SERVICE_IMPL+"\" must be set in the EzConfiguration"));
        }

        if (!messages.isEmpty()) {
            throw new CreationException(messages);
        }

        return service;
    }
}
