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

package ezbake.security.service.enforcement;

import ezbake.security.persistence.model.AppPersistenceModel;

/**
 * User: jhastings
 * Date: 11/21/13
 * Time: 6:52 AM
 */
public class AppInteraction {
    public static boolean allowInterAppComms(AppPersistenceModel from, AppPersistenceModel to) {
        boolean allowed = false;

        if (from != null && to != null) {
            String fromClass = from.getAuthorizationLevel();
            String toClass = to.getAuthorizationLevel();
            if (fromClass != null && toClass != null) {
                try {
                    if (fromClass.compareTo(toClass) > 0) {
                        allowed = true;
                    }
                } catch (IllegalArgumentException e) {
                    // Must have been unsupported classification
                }
            }
        } else {
            allowed = true;
        }

        return allowed;
    }

}
