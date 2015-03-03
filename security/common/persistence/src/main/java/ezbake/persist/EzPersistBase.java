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

package ezbake.persist;

import java.util.Map;

import ezbake.security.persistence.model.AppPersistCryptoException;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 11:02 AM
 */
public abstract class EzPersistBase {
    public abstract EzPersistBase populateEzPersist(Map<String, String> rows) throws AppPersistCryptoException;
    public abstract Map<String, Object> ezPersistRows();

    public static EzPersistBase fromEzPersist(Map<String, String> rows, Class clazz) throws IllegalAccessException, InstantiationException, AppPersistCryptoException {
        if (clazz == null) {
            throw new InstantiationException("class is null");
        }
        EzPersistBase ep = (EzPersistBase) clazz.newInstance();
        return ep.populateEzPersist(rows);
    }
}
