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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import ezbake.security.permissions.PermissionUtils;

/**
 * An authorization-granting rule set that grants authorizations based on
 * whether a set of existing authorizations satisfy boolean expressions.
 *
 * Rules are evaluated in insertion-order and use all previously granted
 * authorizations when evaluating a given expression. That is, if one
 * expression grants authorization "foo", then a subsequent rule may use
 * "foo" as part of its boolean expression.
 */
public class BooleanExpressionAuthorizationRuleSet implements AuthorizationRuleSet {

    private final LinkedHashMap<String, String> rules = new LinkedHashMap<>();

    /**
     * Add a rule to the set of authorization-granting rules.
     *
     * @param authorization authorization to conditionally grant
     * @param expression a permission-utils compatible boolean expression
     */
    public void addRule(String authorization, String expression) {
        if (!PermissionUtils.isVisibilityExpression(expression)) {
            throw new IllegalArgumentException(String.format("Invalid visibility expression: %s", expression));
        }

        rules.put(authorization, expression);
    }

    @Override
    public Set<String> grantAuthorizations(final Set<String> baseAuthorizations) {
        Set<String> allAuthorizations = new HashSet<>(baseAuthorizations);
        for (Map.Entry<String, String> rule : rules.entrySet()) {
            if (PermissionUtils.validateVisibilityExpression(allAuthorizations, rule.getValue())) {
                allAuthorizations.add(rule.getKey());
            }
        }

        return allAuthorizations;
    }
}
