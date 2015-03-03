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

package ezbake.locksmith.db;

import static org.junit.Assert.*;

import java.net.UnknownHostException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.locksmith.service.BaseTest;
import ezbake.security.lock.smith.thrift.KeyExistsException;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.crypto.AESCrypto;

public class LocksmithManagerTest extends BaseTest {
	private static Logger log = LoggerFactory.getLogger(LocksmithManagerTest.class);
	
	private static MongoDBService mongoService;
	private static AbstractLocksmithManager aesManager;
	private static EzConfiguration ezConfig;
	
	@BeforeClass
	public static void init() throws Exception {
	    BaseTest.init();
        ezConfig = new EzConfiguration(new ClasspathConfigurationLoader());
		
        mongoService = new MongoDBService(ezConfiguration);
		aesManager = new AesLocksmithManager("aes_keys", ezConfig.getProperties());
	}
	
	@After
	public void clean() {
		mongoService.dropCollectionFromDb(aesManager.getTableName());
	}
	
	@Test
	public void testGenerateAESKey() throws KeyExistsException {
		aesManager.generateKey("a1b2c3d4e5", "TEST");
		aesManager.generateKey("A1B2C3D4E5", "TEST");
	}
	
	@Test
	public void testGenerateAESKeyDuplicateId() {
		boolean exception = false;
		
		try {
			aesManager.generateKey("a1b2c3d4e5", "TEST");
			aesManager.generateKey("a1b2c3d4e5", "TEST");
		}
		catch(KeyExistsException e) {
			exception = true;
		}
		finally {
			assertTrue(exception);
		}
	}
	
	@Test
	public void testGetAESKey() throws KeyExistsException, KeyNotFoundException {
		String keyId = "The Key Id";
		SecretKey key = null;
		
        aesManager.generateKey(keyId, "TEST");

        byte[] keyData = aesManager.getKey(keyId, "TEST");

        key = new SecretKeySpec(keyData, 0, keyData.length, "AES");

        AESCrypto aesCrypto = new AESCrypto();
        String data = "Data.";

        byte[] cipherData = aesCrypto.encrypt(key, data.getBytes());
        byte[] uncipherData = aesCrypto.decrypt(key, cipherData);

        String str = new String(uncipherData);
        assertTrue("Encrypted String Should Be Same As Decrypted String", str.equals(data));
	}

    @Test(expected=KeyNotFoundException.class)
	public void testGetNotExistentGood() throws KeyNotFoundException {
		String keyId = "The Non Existent Key Id";
		aesManager.getKey(keyId, "TEST");
	}
	
	@Test
	public void testAESRemove() throws KeyExistsException, KeyNotFoundException {
		String keyId = "The Key Id";
		int status = 0;
		boolean keyNotFoundException = false;
		
		log.info("Test AES Remove");
		
        aesManager.generateKey(keyId, "TEST");
        aesManager.removeKey(keyId, "TEST");
        assertTrue(mongoService.collectionCount(aesManager.getTableName()) == 0);
	}
	
	@Test
	public void testAESInsert() throws KeyExistsException, KeyNotFoundException {
		String keyId = "Key Identifiably Identible Identifier";

        AESCrypto aesCrypto = new AESCrypto();
        SecretKey secret = aesCrypto.generateAESKey();

        log.info("AES Encoded Lenght: {}", secret.getEncoded().length);

        aesManager.insertKey(keyId, secret.getEncoded(), "TEST", null);
        byte[] b = aesManager.getKey(keyId, "TEST");
        SecretKey shhh_Secret = new SecretKeySpec(b, 0, b.length, "AES");

        assertTrue(secret.equals(shhh_Secret));
	}

    @Test
    public void testSharedWithGet() throws KeyExistsException, KeyNotFoundException {
        String keyId = "my key";
        String key = "test key";
        String owner = "owner";
        String shared = "shared";

        aesManager.insertKey(keyId, key.getBytes(), owner, shared);

        byte[] owners = aesManager.getKey(keyId, owner);
        byte[] shareds = aesManager.getKey(keyId, shared);

        Assert.assertEquals(key, new String(owners));
        Assert.assertEquals(key, new String(shareds));
    }
	
	@Test
	public void testAESInsertBogusKey() throws KeyExistsException {
		/**
		 * Note: This test passes, because we allow them to insert bogus keys.
		 */
		byte[] bogusKey = {0x42, 0x4F, 0x47, 0x55, 0x53};
		
        log.info("Using Key {}", new String(bogusKey));
        aesManager.insertKey("1234", bogusKey, "TEST", null);
	}

    @Test(expected=KeyNotFoundException.class)
    public void testOnlyVisibleToOwner() throws KeyExistsException, KeyNotFoundException {
        String id = "mykeyid";
        aesManager.generateKey(id, "TEST");

        try {
            aesManager.getKey(id, "TEST");
        } catch (KeyNotFoundException e) {
            Assert.fail("Failed to get generated key");
        }

        aesManager.getKey(id, "ANOTHER_USER");
    }

    @Test(expected=KeyExistsException.class)
    public void keysNamesMustBeUnique() throws KeyExistsException {
        String keyId = "jeffskey";

        aesManager.generateKey(keyId, "Jeff");
        aesManager.generateKey(keyId, "John");
    }
}
