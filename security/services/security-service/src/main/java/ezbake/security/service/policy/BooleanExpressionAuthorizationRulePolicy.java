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

package ezbake.security.service.policy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.api.ua.User;
import ezbake.security.persistence.model.AppPersistenceModel;

/**
 * Authorization policy that grants additional authorizations based on rules
 * and existing authorizations. Since this policy only makes sense in the
 * context of a user application already having a base set of authorizations,
 * it instantiates a base policy from which is reads authorizations before
 * granting its own.
 */
public class BooleanExpressionAuthorizationRulePolicy extends AuthorizationPolicy {

    /**
     * Property for path to rule file that can be read by
     * {@link ezbake.security.service.policy.FlatFileBooleanExpressionAuthorizationRuleReader}.
     */
    public static final String RULE_FILE_PROPERTY_KEY = "ezbake.security.service.policy.authorizationRule.file";

    /** Property for base implementation */
    public static final String BASE_IMPL_PROPERTY_KEY = "ezbake.security.service.policy.authorizationRule.baseImpl";

    /** Default filename for authorization rules */
    public static final String DEFAULT_RULE_FILE = "ezbake/security/service/policy/authorizations.txt";

    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(BooleanExpressionAuthorizationRuleSet.class);

    /** Rules */
    private final BooleanExpressionAuthorizationRuleSet rules;

    /** Base policy */
    private final AuthorizationPolicy basePolicy;

    /**
     * Construct a new authorization policy.
     *
     * Reads rules from file specified by properties. Attempts to find the file
     * on the filesystem first. If absent, will try to load the file from
     * classpath instead.
     *
     * @param properties EzBake configuration
     */
    public BooleanExpressionAuthorizationRulePolicy(Properties properties) {
        super(properties);

        Properties clone = (Properties) properties.clone();
        clone.setProperty(AuthorizationPolicy.POLICY_IMPL,
                properties.getProperty(BASE_IMPL_PROPERTY_KEY, AuthorizationPolicy.DEFAULT_IMPL));
        basePolicy = AuthorizationPolicy.getInstance(clone);

        String ruleFileName = properties.getProperty(RULE_FILE_PROPERTY_KEY, DEFAULT_RULE_FILE);
        File ruleFile = new File(ruleFileName);
        Reader reader = null;
        if (ruleFile.exists()) {
            try {
                reader = new FileReader(ruleFile);
            } catch (FileNotFoundException e) {
                // This shouldn't happen since we just checked that the file exists
                logger.error("Couldn't find rule file", e);
            }
        } else {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(ruleFileName);
            reader = new InputStreamReader(is);
        }

        try {
            rules = new FlatFileBooleanExpressionAuthorizationRuleReader(reader).readRules();
        } catch (IOException e) {
            logger.error("Failed to load authorization rules", e);

            throw new RuntimeException("Unable to initialize authorization rule policy", e);
        }
    }

    @Override
    public void populateTokenForUser(EzSecurityToken token, User user) {
        basePolicy.populateTokenForUser(token, user);
    }

    @Override
    public Set<String> authorizationsForUser(User user) {
        Set<String> baseAuthorizations = basePolicy.authorizationsForUser(user);
        logger.debug("Base user authorizations: {}", baseAuthorizations);

        Set<String> grantedAuthorizations = rules.grantAuthorizations(baseAuthorizations);
        logger.debug("Granted user authorizations: {}", grantedAuthorizations);

        return grantedAuthorizations;
    }

    @Override
    public Set<String> authorizationsForApp(AppPersistenceModel app) {
        Set<String> baseAuthorizations = basePolicy.authorizationsForApp(app);
        logger.debug("Base app authorizations: {}", baseAuthorizations);

        Set<String> grantedAuthorizations = rules.grantAuthorizations(baseAuthorizations);
        logger.debug("Granted app authorizations: {}", grantedAuthorizations);

        return grantedAuthorizations;
    }

    @Override
    public Set<String> externalCommunityAuthorizationsForUser(User user) {
        return basePolicy.externalCommunityAuthorizationsForUser(user);
    }
}
