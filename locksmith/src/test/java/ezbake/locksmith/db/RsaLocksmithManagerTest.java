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

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

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
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.RSAKeyCrypto;

public class RsaLocksmithManagerTest extends BaseTest {
	
	private static Logger log =  LoggerFactory.getLogger(RsaLocksmithManagerTest.class);
	private static EzConfiguration ezConfig;
	private static RsaLocksmithManager rsaManager;
	private static MongoDBService mongoService;
	
	@BeforeClass
	public static void init() throws Exception {
        ezConfig = new EzConfiguration(new ClasspathConfigurationLoader());
        BaseTest.init();
        rsaManager =  new RsaLocksmithManager("rsa_keys", ezConfig.getProperties());
		mongoService = new MongoDBService(ezConfiguration);
	}
	
	@After
	public void clean() throws KeyNotFoundException {
		mongoService.dropCollectionFromDb(rsaManager.getTableName());
	}

	@Test
	public void testRsaGenerateAKey() throws KeyExistsException {
		rsaManager.generateKey("123456", "TEST");
	}
	
	@Test(expected=KeyExistsException.class)
	public void testRsaGenerateDuplicateKey() throws KeyExistsException {
		String keyId = "The Great Id";
		rsaManager.generateKey(keyId, "TEST");
		rsaManager.generateKey(keyId, "TEST");
	}
	
	@Test(expected=KeyNotFoundException.class)
	public void testGetNotExistentKeyGood() throws KeyNotFoundException {
		String keyId = "A Key That Doesn't Exist";
		rsaManager.getKey(keyId, "TEST");
	}
	
	@Test
	public void testRsaGetKey() throws KeyExistsException, KeyNotFoundException, NoSuchAlgorithmException, InvalidKeySpecException, PKeyCryptoException {
		String keyId = "123456";
		
		rsaManager.generateKey(keyId, "TEST");
		byte[] keyData = rsaManager.getKey(keyId, "TEST");
		
		RSAKeyCrypto keyCrypto = new RSAKeyCrypto(new String(keyData),true);
		
		byte[] data = "Hello, World!".getBytes();
		
		byte[] cipherData = keyCrypto.encrypt(data);
		byte[] uncipherData = keyCrypto.decrypt(cipherData);
		
		assertTrue(new String(data).compareTo(new String(uncipherData)) == 0);
	}
	
	@Test
	public void testRSARemoveKey() throws KeyExistsException, KeyNotFoundException {
		String keyId = "123456";
		
		rsaManager.generateKey(keyId, "TEST");
		rsaManager.removeKey(keyId, "TEST");

        assertTrue(mongoService.collectionCount(rsaManager.getTableName()) == 0);
	}


    @Test(expected=KeyNotFoundException.class)
    public void testOnlyVisibleToOwner() throws KeyExistsException, KeyNotFoundException {
        String id = "mykeyid";
        rsaManager.generateKey(id, "TEST");

        try {
            rsaManager.getKey(id, "TEST");
        } catch (KeyNotFoundException e) {
            Assert.fail("Failed to get key we just generated");
        }
        rsaManager.getKey(id, "ANOTHER_USER");
    }

    @Test
    public void testGetPublicKeyAsOwner() throws KeyExistsException, InvalidKeySpecException, NoSuchAlgorithmException {
        String keyId = "public key";
        byte[] priv = rsaManager.generateKey(keyId, "Jeff");
        String pub = rsaManager.getPublicKey(keyId);

        RSAKeyCrypto privateKey = new RSAKeyCrypto(new String(priv), true);

        Assert.assertEquals(pub, privateKey.getPublicPEM());
    }

    @Test
    public void testGetPublicThenGetPrivate() throws KeyExistsException, InvalidKeySpecException, NoSuchAlgorithmException {
        String keyId = "public key";
        String pub = rsaManager.getPublicKey(keyId);
        byte[] priv = rsaManager.generateKey(keyId, "Jeff");
        log.debug("pub: {}", pub);
        log.debug("priv: {}", new RSAKeyCrypto(new String(priv), true).getPublicPEM());

        RSAKeyCrypto privateKey = new RSAKeyCrypto(new String(priv), true);
        Assert.assertEquals(pub, privateKey.getPublicPEM());
    }

    @Test(expected=KeyExistsException.class)
    public void testGetPublicThenGetPrivateBecomesOwner() throws KeyExistsException, InvalidKeySpecException, NoSuchAlgorithmException {
        String keyId = "public key";
        try {
            String pub = rsaManager.getPublicKey(keyId);
            byte[] priv = rsaManager.generateKey(keyId, "Jeff");

            RSAKeyCrypto privateKey = new RSAKeyCrypto(new String(priv), true);
            Assert.assertEquals(pub, privateKey.getPublicPEM());
        } catch (Exception e) {
            Assert.fail("No exceptions should be thrown here");
        }

        rsaManager.generateKey(keyId, "John");
    }
}
