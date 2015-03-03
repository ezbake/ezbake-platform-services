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

package ezbake.locksmith.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Random;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.crypto.AESCrypto;
import ezbake.locksmith.db.MongoDBService;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.KeyExistsException;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.security.lock.smith.thrift.KeyType;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

public class IT_EzLocksmithHandlerTest extends BaseTest {
	private static Logger log = LoggerFactory.getLogger(IT_EzLocksmithHandlerTest.class);

	private static ThriftServerPool serverPool;
	private static ThriftClientPool clientPool;

	private static final String SERVICE_NAME = "SERVICE_NAME";

	private static MongoDBService mongo;

	private static String rsaTable;
	private static String aesTable;

	@BeforeClass
	public static void init() throws Exception {
	    BaseTest.init();

        Random portChooser = new Random();
        int port = portChooser.nextInt((23999 - 22999) + 1) + 22999;
        serverPool = new ThriftServerPool(ezConfiguration, port);
		serverPool.startCommonService(new EzLocksmithHandler(), SERVICE_NAME, "gibberish1234");
        clientPool = new ThriftClientPool(ezConfiguration);

		mongo = new MongoDBService(ezConfiguration);
		EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(ezConfiguration);
		rsaTable = String.format("lock_smith_%s_%s_%s", appConfig.getApplicationName(), appConfig.getServiceName(), "rsa_keys");
		aesTable = String.format("lock_smith_%s_%s_%s", appConfig.getApplicationName(), appConfig.getServiceName(), "aes_keys");
	}

	@Before
	public void clean() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("localhost", port);
        mongoClient.dropDatabase("db");
	}

	@Test(expected=KeyExistsException.class)
	public void testAESGenerateKeyDuplicatedId() throws TException, KeyExistsException {
		log.info("Test Generate AES");
		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;
		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.AES, Collections.<String>emptyList());
			client.generateKey(ezToken, keyId, KeyType.AES, Collections.<String>emptyList());
		}
		finally {
			clientPool.returnToPool(client);
		}

	}

	@Test
	(expected=KeyExistsException.class)
	public void testRSAGenerateKeyDuplicatedId() throws TException, KeyExistsException {
		log.info("Test Generate RSA");
		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;
		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.RSA, Collections.<String>emptyList());
			client.generateKey(ezToken, keyId, KeyType.RSA, Collections.<String>emptyList());
		}
		finally {
			clientPool.returnToPool(client);
		}
	}

	@Test(expected=KeyExistsException.class)
	public void testAESUploadKeyDuplicatedId() throws TException, KeyExistsException {
		log.info("Test Upload");
		EzSecurityToken ezToken = getTestEzSecurityToken();

		EzLocksmith.Client client = null;
		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.uploadKey(ezToken, keyId, new String(), KeyType.AES);
			client.uploadKey(ezToken, keyId, new String(), KeyType.AES);
		}
		finally {
			clientPool.returnToPool(client);
		}
	}

	@Test
    (expected=KeyExistsException.class)
	public void testRSAUploadKeyDuplicatedId() throws TException, KeyExistsException {
		log.info("Test RSA Upload Key Duplicated Id");

		EzSecurityToken ezToken = getTestEzSecurityToken();

		EzLocksmith.Client client = null;
		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.uploadKey(ezToken, keyId, new String(), KeyType.RSA);
			client.uploadKey(ezToken, keyId, new String(), KeyType.RSA);
		}
		finally {
			clientPool.returnToPool(client);
		}
	}

    @Test
    public void testRetrieveGeneratesIfNotExists() throws TException {
        String keyId = "RETRIEVE_KEY";

        EzSecurityToken ezToken = getTestEzSecurityToken();
        EzLocksmith.Client client = null;
        try {
            client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
            String keyData = client.retrieveKey(ezToken, keyId, KeyType.AES);

            // Make sure it's a valid AES key
            SecretKey keySpec = new SecretKeySpec(Base64.decodeBase64(keyData), "AES");
            String data = "encrypt this";
            AESCrypto crypto = new AESCrypto();
            byte[] enc = crypto.encrypt(keySpec, data.getBytes());
            assertEquals(data, new String(crypto.decrypt(keySpec, enc)));
        } finally {
            clientPool.returnToPool(client);
        }

    }

	@Test
	public void testAESGetGeneratedData() throws TException, KeyExistsException, KeyNotFoundException, NoSuchAlgorithmException {
		log.info("Test AES Get Generated Data");

		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;

		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.AES, Collections.<String>emptyList());
			String keyData = client.retrieveKey(ezToken, keyId, KeyType.AES);

			assertTrue(keyData != null);

            // Make sure it's a valid AES key
            SecretKey keySpec = new SecretKeySpec(Base64.decodeBase64(keyData), "AES");
            String data = "encrypt this";
            AESCrypto crypto = new AESCrypto();
            byte[] enc = crypto.encrypt(keySpec, data.getBytes());
            assertEquals(data, new String(crypto.decrypt(keySpec, enc)));
        }
		finally {
			clientPool.returnToPool(client);
		}
	}

	@Test
	public void testRSAGetGeneratedData() throws TException, KeyExistsException, KeyNotFoundException {
		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;

		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.RSA, Collections.<String>emptyList());
			String keyData = client.retrieveKey(ezToken, keyId, KeyType.RSA);

			assertTrue(keyData != null);
		}
		finally {
			clientPool.returnToPool(client);
		}
	}

	@Test
	public void testAESRemoveGeneratedData() throws KeyExistsException, TException, KeyNotFoundException {
		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;

		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.AES, Collections.<String>emptyList());

			client.removeKey(ezToken, keyId, KeyType.AES);
			assertTrue(mongo.collectionCount(aesTable) == 0);
		}
		finally {
			clientPool.returnToPool(client);
		}
	}

	@Test
	public void testRSARemoveGeneratedData() throws TException, KeyExistsException, KeyNotFoundException  {
		EzSecurityToken ezToken = getTestEzSecurityToken();
		EzLocksmith.Client client = null;

		String keyId = "keyId";

		try {
			client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
			client.generateKey(ezToken, keyId, KeyType.RSA, Collections.<String>emptyList());

			client.removeKey(ezToken, keyId, KeyType.RSA);

			assertTrue(mongo.collectionCount(rsaTable) == 0);
		}
		finally {
			clientPool.returnToPool(client);
		}
	}


	@Test
	public void testRSAGenerateData() throws TException {
	    EzSecurityToken ezToken = getTestEzSecurityToken();
	    EzLocksmith.Client client = null;

	    String keyId = "keyId";

        client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
        String pk = client.retrievePublicKey(ezToken,keyId, ezToken.getValidity().getIssuedTo());
        assertTrue(pk != null);
	}

    @Test
    public void testRSANonOwnerGeneratesPrivate() throws TException {
        EzSecurityToken ezToken = getTestEzSecurityToken();
        EzLocksmith.Client client = null;
        String keyId = "keyId";

        // Somebody accesses the public key first
        client = clientPool.getClient(SERVICE_NAME, EzLocksmith.Client.class);
        String pk = client.retrievePublicKey(MockEzSecurityToken.getMockUserToken("Not Owner"), keyId, null);
        assertTrue(pk != null);

        // Now attempt to retrieve the
        String priv = client.retrieveKey(MockEzSecurityToken.getMockUserToken("Key Owner"), keyId, KeyType.RSA);
        assertTrue(priv != null);
    }


	public EzSecurityToken getTestEzSecurityToken() {
        return MockEzSecurityToken.getMockUserToken("Key Owner");
    }
}
