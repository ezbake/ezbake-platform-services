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
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.security.lock.smith.thrift.KeyType;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import ezbake.thrift.ThriftClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 10:14 PM
 */
@Singleton
public class LocksmithKeySupplier implements Supplier<SecretKey> {
    private static final Logger logger = LoggerFactory.getLogger(LocksmithKeySupplier.class);
    public static final String LOCKSMITH_SERVICE = EzLocksmithConstants.SERVICE_NAME;
    public static final String KEY_ID = "Locksmith Key ID";

    final ThriftClientPool clientPool;
    EzSecurityTokenProvider tokenProvider;
    Supplier<String> locksmithIdSupplier;

    @Inject
    @Named(KEY_ID)
    protected String id;

    @Inject
    public LocksmithKeySupplier(EzSecurityTokenProvider tokenProvider, ThriftClientPool clientPool, String id) {
        this.tokenProvider = tokenProvider;
        this.clientPool = clientPool;
        this.id = id;
        this.locksmithIdSupplier = Suppliers.memoizeWithExpiration(new Supplier<String>() {
            @Override
            public String get() {
                return LocksmithKeySupplier.this.clientPool.getSecurityId(LOCKSMITH_SERVICE);
            }
        }, 60, TimeUnit.SECONDS);
    }

    @Override
    public SecretKey get() {
        SecretKey key;

        String locksmithSecurityId = locksmithIdSupplier.get();
        if (locksmithSecurityId == null) {
            logger.warn("Failed to get a security id for {}. Not attempting to fetch {} key", LOCKSMITH_SERVICE, id);
            return null;
        }

        EzLocksmith.Client client = null;
        try {
            logger.info("Retrieving key: {} from {}", id, LOCKSMITH_SERVICE);
            client = clientPool.getClient(LOCKSMITH_SERVICE, EzLocksmith.Client.class);
            String encodedKey = client.retrieveKey(tokenProvider.get(locksmithSecurityId), id, KeyType.AES);
            logger.info("{} returned key: {}", LOCKSMITH_SERVICE, id);
            key = new SecretKeySpec(Base64.decodeBase64(encodedKey), "AES");
        } catch (TException e) {
            logger.error("Unable to retrieve key ID: {} from locksmith service {}", id, e.getMessage());
            key = null;
        } finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
        }

        return key;
    }

}
