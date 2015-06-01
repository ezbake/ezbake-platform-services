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

import java.util.Iterator;
import java.util.Properties;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import ezbake.security.lock.smith.thrift.KeyExistsException;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.crypto.AESCrypto;

public class AesLocksmithManager extends AbstractLocksmithManager {
	private static final Logger log = LoggerFactory.getLogger(AesLocksmithManager.class);
	private AESCrypto aesCrypto;

	public AesLocksmithManager(String tableName, Properties ezConfig) {
		super(tableName, ezConfig);
		aesCrypto = new AESCrypto();
	}

	@Override
	public byte[] generateKey(String keyId, String owner, String... sharedWith) throws KeyExistsException {
		SecretKey secretKey = aesCrypto.generateAESKey();
		byte[] keyData = secretKey.getEncoded();

		return insertKey(keyId, keyData, owner, sharedWith);
	}

	@Override
	public byte[] insertKey(String keyId, byte[] keyData, String owner, String... sharedWith) throws KeyExistsException {
		log.debug("Insert Key {}, {}", keyId, keyData);

		if(keyExists(keyId)) {
			throw new KeyExistsException("There exists a key with id [" + keyId + "] already in the database.");
		}

		BasicDBObject doc = new BasicDBObject(KEY_ID, keyId)
                .append(KEY_DATA, keyData)
                .append(KEY_OWNER, owner)
                .append(ACCESS_LIST, sharedWith);
		mongoService.insertDocumentIntoCollection(getTableName(), doc);
        return keyData;
	}

	@Override
	public void removeKey(String keyId, String owner) throws KeyNotFoundException {

		if(!keyExists(keyId)) {
			throw new KeyNotFoundException("The Key With Specified Id [" + keyId + "] doesn't exist.");
		}

		BasicDBObject doc = new BasicDBObject(KEY_ID, keyId).append(KEY_OWNER, owner);
		mongoService.removeDocumentFromCollection(getTableName(), doc);
	}

	@Override
	public byte[] getKey(String keyId, String user) throws KeyNotFoundException {
		byte[] keyData = null;

		DBObject projection = new BasicDBObject();
		projection.put(KEY_ID, 1);
		projection.put(KEY_DATA, 1);

		DBCursor cursor = mongoService.findInCollection(getTableName(), keyQuery(keyId, user), projection);
		Iterator<DBObject> it = cursor.iterator();

		if(it.hasNext()) {
			DBObject obj = it.next();
			keyData = (byte[])obj.get(KEY_DATA);
		} else {
            throw new KeyNotFoundException("No key found for key ID: "+keyId+", Owner: "+user);
        }

		return keyData;
	}
}
