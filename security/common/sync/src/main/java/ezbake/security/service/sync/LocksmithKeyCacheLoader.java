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

package ezbake.security.service.sync;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheLoader;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;

import javax.crypto.SecretKey;

/**
 * User: jhastings
 * Date: 9/3/14
 * Time: 2:45 PM
 */
public class LocksmithKeyCacheLoader<K extends Object> extends CacheLoader<K, SecretKey> {

    Supplier<SecretKey> keySupplier;
    public LocksmithKeyCacheLoader(Supplier<SecretKey> keySupplier) {
        this.keySupplier = keySupplier;
    }

    @Override
    public SecretKey load(K key) throws Exception {
        SecretKey secretKey = keySupplier.get();
        if (secretKey == null) {
            throw new KeyNotFoundException("Locksmith key supplier returned null for key:" + key);
        }
        return secretKey;
    }

}
