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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.name.Names;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.security.lock.smith.thrift.KeyType;
import ezbake.security.test.MockEzSecurityToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import ezbake.thrift.ThriftClientPool;

import javax.crypto.KeyGenerator;
import java.security.NoSuchAlgorithmException;

/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 10:26 PM
 */
public class LocksmithKeySupplierTest {
    public static final String lockId = "LOCK_ID";

    LocksmithKeySupplier keyProvider;
    @Before
    public void setUp() throws TException, EzConfigurationLoaderException, NoSuchAlgorithmException {
        // Set up mocks
        EzLocksmith.Client lockClient = EasyMock.createMock(EzLocksmith.Client.class);
        EasyMock.expect(lockClient.retrieveKey(
                EasyMock.<EzSecurityToken>anyObject(),
                EasyMock.eq(lockId),
                EasyMock.eq(KeyType.AES)
        )).andReturn(getAESKey());

        final ThriftClientPool clientPool = EasyMock.createMock(ThriftClientPool.class);
        EasyMock.expect(clientPool.getSecurityId(LocksmithKeySupplier.LOCKSMITH_SERVICE)).andReturn("12345");
        EasyMock.expect(clientPool.getClient(LocksmithKeySupplier.LOCKSMITH_SERVICE, EzLocksmith.Client.class)).andReturn(lockClient);
        clientPool.returnToPool(EasyMock.isA(EzLocksmith.Client.class));
        EasyMock.expectLastCall();

        EasyMock.replay(lockClient, clientPool);

        final EzSecurityTokenProvider tokenProvider = EasyMock.createMock(EzSecurityTokenProvider.class);
        EasyMock.expect(tokenProvider.get(
                EasyMock.anyString()))
                .andReturn(MockEzSecurityToken.getMockAppToken("01"))
                .anyTimes();
        keyProvider = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(EzSecurityTokenProvider.class).toInstance(tokenProvider);
                bind(ThriftClientPool.class).toInstance(clientPool);
                bindConstant().annotatedWith(Names.named("Locksmith Key ID")).to(lockId);
            }
        }).getInstance(LocksmithKeySupplier.class);

    }

    String getAESKey() throws NoSuchAlgorithmException {
        KeyGenerator keygenerator = KeyGenerator.getInstance("AES");
        keygenerator.init(256);
        return Base64.encodeBase64String(keygenerator.generateKey().getEncoded());
    }


    @Test
    public void testGetKey() throws KeyNotFoundException {
        keyProvider.get();
    }
}
