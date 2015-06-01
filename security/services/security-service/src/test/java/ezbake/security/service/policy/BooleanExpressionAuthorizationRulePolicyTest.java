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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;

import ezbake.security.api.ua.Authorizations;
import ezbake.security.api.ua.User;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.base.thrift.EzSecurityToken;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BooleanExpressionAuthorizationRulePolicyTest {

    private static final String CUSTOM_AUTHORIZATION_PATH = "ezbake/security/service/policy/custom-authorizations.txt";

    @Test
    public void testLoadFromFilesystem() throws URISyntaxException, IOException {
        URL resourceUrl = this.getClass().getClassLoader().getResource(CUSTOM_AUTHORIZATION_PATH);
        assertNotNull(resourceUrl);
        Path resourcePath = Paths.get(resourceUrl.toURI());

        Path tmpPath = Files.createTempFile("test", ".txt");
        File tmpFile = tmpPath.toFile();
        tmpFile.deleteOnExit();
        FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        Files.copy(resourcePath, fileOutputStream);

        Properties properties = new Properties();
        properties.setProperty(BooleanExpressionAuthorizationRulePolicy.RULE_FILE_PROPERTY_KEY,
                tmpFile.getAbsolutePath());

        BooleanExpressionAuthorizationRulePolicy policy = new BooleanExpressionAuthorizationRulePolicy(properties);

        // The custom policy should grant "B", "C", "D", and "E", but not grant "F". See the policy file for details.
        assertEquals(Sets.newHashSet("level", "org", "A", "B", "C", "D", "E", "USA"),
                policy.authorizationsForUser(createTestUser()));
        assertEquals(Sets.newHashSet("level", "A", "B", "C", "D", "E", "USA"),
                policy.authorizationsForApp(createTestApp()));

        fileOutputStream.close();
        assertTrue(tmpFile.delete());
    }

    @Test
    public void testLoadFromClasspath() {
        Properties properties = new Properties();
        properties.setProperty(BooleanExpressionAuthorizationRulePolicy.RULE_FILE_PROPERTY_KEY,
                CUSTOM_AUTHORIZATION_PATH);

        BooleanExpressionAuthorizationRulePolicy policy = new BooleanExpressionAuthorizationRulePolicy(properties);

        // The custom policy should grant "B", "C", "D", and "E", but not grant "F". See the policy file for details.
        assertEquals(Sets.newHashSet("level", "org", "A", "B", "C", "D", "E", "USA"),
                policy.authorizationsForUser(createTestUser()));
        assertEquals(Sets.newHashSet("level", "A", "B", "C", "D", "E", "USA"),
                policy.authorizationsForApp(createTestApp()));
    }

    @Test
    public void testDefaults() {
        BooleanExpressionAuthorizationRulePolicy policy =
                new BooleanExpressionAuthorizationRulePolicy(new Properties());

        // The supplied default rules should be empty. The returned authorizations should be what the user started with.
        // SimplePolicy (the default) grants authorizations for any formal accesses, level, organization, and
        // citizenship.
        assertEquals(Sets.newHashSet("level", "org", "A", "USA"), policy.authorizationsForUser(createTestUser()));
        assertEquals(Sets.newHashSet("level", "A", "USA"), policy.authorizationsForApp(createTestApp()));
    }

    @Test
    public void testCustomBasePolicy() {
        Properties properties = new Properties();
        properties.setProperty(BooleanExpressionAuthorizationRulePolicy.BASE_IMPL_PROPERTY_KEY,
                CustomBasePolicy.class.getName());

        BooleanExpressionAuthorizationRulePolicy policy =
                new BooleanExpressionAuthorizationRulePolicy(properties);

        // The supplied default rules should be empty. The returned authorizations should be what the user started with.
        assertEquals(Sets.newHashSet("custom-user", "A", "USA"), policy.authorizationsForUser(createTestUser()));
        assertEquals(Sets.newHashSet("custom-app", "A", "USA"), policy.authorizationsForApp(createTestApp()));
    }

    public static class CustomBasePolicy extends AuthorizationPolicy {

        protected CustomBasePolicy(Properties properties) {
            super(properties);
        }

        @Override
        public void populateTokenForUser(EzSecurityToken token, User user) {
            // Don't care
        }

        @Override
        public Set<String> authorizationsForUser(User user) {
            return Sets.newHashSet("custom-user", "A", "USA");
        }

        @Override
        public Set<String> authorizationsForApp(AppPersistenceModel app) {
            return Sets.newHashSet("custom-app", "A", "USA");
        }

        @Override
        public Set<String> externalCommunityAuthorizationsForUser(User u) {
            // Don't care
            return null;
        }
    }

    /**
     * Create a new app model that has base authorizations "level", "org", "A",
     * and "USA" when using {@link SimplePolicy}.
     *
     * @return new user
     */
    private static User createTestUser() {
        User user = new User();
        Authorizations authorizations = user.getAuthorizations();
        authorizations.setAuths(Sets.newHashSet("A"));
        authorizations.setLevel("level");
        authorizations.setOrganization("org");
        authorizations.setCitizenship("USA");

        return user;
    }

    /**
     * Create a new app model that has base authorizations "level", "A", and
     * "USA" when using {@link SimplePolicy}.
     *
     * @return new app model
     */
    private static AppPersistenceModel createTestApp() {
        AppPersistenceModel app = new AppPersistenceModel();
        app.setFormalAuthorizations(Lists.newArrayList("A", "USA"));
        app.setAuthorizationLevel("level");

        return app;
    }
}
