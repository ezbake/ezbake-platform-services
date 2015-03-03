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

package ezbake.protect.ezca;

import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.persistence.model.CAPersistenceModel;

import java.util.Map;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 10:25 AM
 */
public class Factory {
    public static Class entryType(final Map<String, String> entry) {
        Class c = null;
        for (String s : entry.keySet()) {
            if (s.contains("name")) {
                c = AppPersistenceModel.class;
                break;
            } else if (s.contains("serial")) {
                c = CAPersistenceModel.class;
                break;
            }
        }
        return c;
    }
}
