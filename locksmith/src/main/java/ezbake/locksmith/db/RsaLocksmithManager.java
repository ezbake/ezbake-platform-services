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

import com.mongodb.*;

import ezbake.security.lock.smith.thrift.KeyExistsException;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.crypto.RSAKeyCrypto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RsaLocksmithManager extends AbstractLocksmithManager {
    public static final Logger logger = LoggerFactory.getLogger(RsaLocksmithManager.class);

	public RsaLocksmithManager(String tableName, Properties ezConfig) {
		super(tableName, ezConfig);
	}

	@Override
	public byte[] generateKey(String keyId, String owner, String... sharedwith) throws KeyExistsException {
		byte[] data = new RSAKeyCrypto().getPrivatePEM().getBytes();
		return insertKey(keyId, data, owner, sharedwith);
	}

	@Override
	public byte[] insertKey(String keyId, byte[] keyData, String owner, String... sharedWith) throws KeyExistsException {
        byte[] insertedKey = keyData;

		if(!keyExists(keyId)) {
			BasicDBObject doc = new BasicDBObject(KEY_ID, keyId)
                    .append(KEY_DATA, keyData)
                    .append(KEY_OWNER, owner)
                    .append(ACCESS_LIST, sharedWith);

			mongoService.insertDocumentIntoCollection(getTableName(), doc);
		} else {
            // Try to claim ownership
            DBObject keyQuery = keyQuery(keyId);
            DBObject projection = new BasicDBObject(KEY_OWNER, 1).append(KEY_DATA, 1);

            Iterator<DBObject> it = mongoService.findInCollection(getTableName(), keyQuery, projection).iterator();
            if (it.hasNext()) {
                DBObject obj = it.next();
                String keyOwner = (String) obj.get(KEY_OWNER);
                insertedKey = (byte[]) obj.get(KEY_DATA);
                if (keyOwner == null) {
                    // Take ownership
                    DBObject upd = new BasicDBObjectBuilder().push("$set").add(KEY_OWNER, owner).pop().get();
                    DBCollection coll = mongoService.getMongoDB().getCollection(getTableName());
                    coll.update(keyQuery, upd);
                } else {
                    throw new KeyExistsException("The keyId [" + keyId + "] Already Exists.");
                }
            }
        }
        return insertedKey;
	}

	@Override
	public void removeKey(String keyId, String owner) throws KeyNotFoundException {
		
		if(keyExists(keyId)) {
			BasicDBObject doc = new BasicDBObject(KEY_ID, keyId).append(KEY_OWNER, owner);
			mongoService.removeDocumentFromCollection(getTableName(), doc);
		} else {
            throw new KeyNotFoundException("The KeyId [" + keyId + "] Does Not Exist");
        }
	}

	@Override
	public byte[] getKey(String keyId, String owner) throws KeyNotFoundException {
		byte[] result = null;
		
        BasicDBObject projection = new BasicDBObject();
        projection.put(KEY_ID, 1);
        projection.put(KEY_DATA, 1);

        DBCursor cursor = mongoService.findInCollection(getTableName(), keyQuery(keyId, owner), projection);
        Iterator<DBObject> it = cursor.iterator();

        if(it.hasNext()) {
            DBObject obj = it.next();
            result = (byte[])obj.get(KEY_DATA);
        } else {
            throw new KeyNotFoundException("No key found for key ID: "+keyId+", Owner: "+owner);
        }

		return result;
	}

	public String getPublicKey(String keyId) throws KeyExistsException {
        // Get the key
        DBObject keyQuery = new BasicDBObjectBuilder().add(KEY_ID, keyId).get();
        DBObject projection = new BasicDBObjectBuilder().add(KEY_DATA, 1).get();

        Iterator<DBObject> keyIt = mongoService.findInCollection(getTableName(), keyQuery, projection).iterator();
        byte[] key;
        if (keyIt.hasNext()) {
             key = (byte[]) keyIt.next().get(KEY_DATA);
        } else {
            key = generateKey(keyId, null);
        }

        return RSAKeyCrypto.getPublicFromPrivatePEM(new String(key));
	}
	
}
