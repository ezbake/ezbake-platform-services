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

package ezbake.security.service.policy;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.api.ua.User;
import ezbake.security.persistence.model.AppPersistenceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: jhastings
 * Date: 5/12/14
 * Time: 11:17 AM
 */
public abstract class AuthorizationPolicy {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationPolicy.class);
    public static final String POLICY_IMPL = "ezbake.security.service.policy.impl";
    public static final String DEFAULT_IMPL = "ezbake.security.service.policy.SimplePolicy";

    static Map<String, AuthorizationPolicy> instances = new HashMap<String, AuthorizationPolicy>();

    protected Properties ezConfiguration;

    protected AuthorizationPolicy(Properties ezConfiguration) {
        this.ezConfiguration = ezConfiguration;
    }

    public static AuthorizationPolicy getInstance(Properties ezConfiguration) {
        String impl = ezConfiguration.getProperty(POLICY_IMPL, DEFAULT_IMPL);

        AuthorizationPolicy instance = instances.get(impl);
        if (instance == null) {
            try {
                log.info("UserAuthorizationPolicy impl: {}", impl);
                instance = (AuthorizationPolicy) Class.forName(impl).getDeclaredConstructor(
                        Properties.class).newInstance(ezConfiguration);
                instances.put(impl, instance);
            } catch (Exception e) {
                log.error("Error initializing UserAuthorizationPolicy impl({}) with EzConfiguration constructor",
                        impl, e);
                throw new RuntimeException("Unable to initialize the user authorization policy impl", e);
            }
        }

        return instance;

    }

    public abstract void populateTokenForUser(EzSecurityToken token, User user);
    public abstract Set<String> authorizationsForUser(User user);
    public abstract Set<String> authorizationsForApp(AppPersistenceModel app);
    public abstract Set<String> externalCommunityAuthorizationsForUser(User u);

}
