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

package ezbake.security.api.ua;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.security.service.sync.EzSecurityRedisCache;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 4/25/14
 * Time: 3:29 PM
 */
public interface UserAttributeService {
    public static final String UAServiceImpl = "ezbake.security.api.ua.userImpl";


    /**
     * Check with the backing attribute service if a user exists. Only return false if it specifically responded that
     * the user does not
     * @param principal the user's id with the service
     * @return true unless the service indicated the user did not exist
     */
    public boolean assertUserStrictFailure(String principal);

    public boolean assertUser(String principal);
    public User getUser(String principal) throws UserNotFoundException;
    public User getUserProfile(String principal) throws UserNotFoundException;
    public Map<String, List<String>> getUserGroups(String principal) throws UserNotFoundException;

    public EzSecurityRedisCache getCache();
    
    public static class Factory {
        private static final Logger log = LoggerFactory.getLogger(Factory.class);

        public static UserAttributeService getInstance(Properties ezConfiguration) {
            String impl = ezConfiguration.getProperty(UAServiceImpl);

            if (impl == null || impl.isEmpty()) {
                throw new InstantiationError("UserAttributeService Factory didn't find a valid value for " +
                        UAServiceImpl);
            }

            UserAttributeService instance;
            try {
                log.info("UserAttributeService impl: {}", impl);
                instance = (UserAttributeService) Class.forName(impl).getDeclaredConstructor(
                        Properties.class).newInstance(ezConfiguration);
            } catch (Exception e) {
                log.error("Error initializing UserAttributeService impl({}) with EzConfiguration constructor",
                        impl, e);
                throw new RuntimeException("Unable to initialize the user attribute service impl", e);
            }

            return instance;
        }

    }
}
