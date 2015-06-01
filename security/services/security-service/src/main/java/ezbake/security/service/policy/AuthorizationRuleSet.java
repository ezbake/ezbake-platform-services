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

import java.util.Set;

/**
 * Authorization rules allow a principal to start with a base set of
 * authorizations, then have extra authorizations granted based on a set of
 * rules.
 */
public interface AuthorizationRuleSet {

    /**
     * Grant extra authorizations based on a set of rules and a set of base
     * authorizations.
     *
     * @param baseAuthorizations base authorizations
     * @return authorizations with any extra granted authorizations
     */
    Set<String> grantAuthorizations(Set<String> baseAuthorizations);
}
