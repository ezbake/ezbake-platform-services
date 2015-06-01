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

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BooleanExpressionAuthorizationRuleSetTest {

    private BooleanExpressionAuthorizationRuleSet ruleSet;

    @Before
    public void setUp() {
        ruleSet = new BooleanExpressionAuthorizationRuleSet();
    }

    @Test
    public void testAndRule() {
        ruleSet.addRule("C", "A&B");
        ruleSet.addRule("D", "A&X");

        assertEquals(Sets.newHashSet("A", "B", "C"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
    }

    @Test
    public void testOrRule() {
        ruleSet.addRule("C", "A|B");
        ruleSet.addRule("D", "B|X");

        assertEquals(Sets.newHashSet("A", "B", "C", "D"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
    }

    @Test
    public void testBlankRule() {
        ruleSet.addRule("C", "");

        assertEquals(Sets.newHashSet("A", "B", "C"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
    }

    @Test
    public void testOrderedRules() {
        ruleSet.addRule("C", "A");
        ruleSet.addRule("D", "B&C");

        assertEquals(Sets.newHashSet("A", "B", "C", "D"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
    }

    @Test
    public void testParentheses() {
        ruleSet.addRule("C", "((((A&B))))");
        ruleSet.addRule("D", "((((A|B))))");
        ruleSet.addRule("E", "((((A))))");

        assertEquals(Sets.newHashSet("A", "B", "C", "D", "E"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRules() {
        // See PermissionUtils.isVisibilityExpression() for a complete set of invalid rules
        ruleSet.addRule("X", "A&");
    }
}
