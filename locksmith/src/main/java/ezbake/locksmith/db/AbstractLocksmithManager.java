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

import java.io.Closeable;
import java.util.Iterator;
import java.util.Properties;

import com.mongodb.BasicDBList;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import ezbake.security.lock.smith.thrift.KeyExistsException;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;

public abstract class AbstractLocksmithManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(AbstractLocksmithManager.class);

    protected static final String KEY_ID = "keyId";
    protected static final String KEY_DATA = "keyData";
    protected static final String KEY_OWNER = "keyOwner";
    protected static final String ACCESS_LIST = "keyAccess";

    protected String tableName;
    protected MongoDBService mongoService;
    
    public AbstractLocksmithManager(String tableName, Properties ezConfig) {
        final EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(ezConfig);
        String appName = appConfig.getApplicationName();
        String serviceName = appConfig.getServiceName();
        this.tableName = String.format("lock_smith_%s_%s_%s", appName, serviceName, tableName);

        try {
            mongoService = new MongoDBService(ezConfig);
        } catch (ManagerDbAccessException e) {
            log.error("Failed to connect to mongo on startup. Exiting");
            throw new RuntimeException(e);
        }

        log.debug("Using appName {}, serviceName {}", appName, serviceName);
        log.info("Table Name {}", tableName);
        if(!mongoService.collectionExists(this.tableName)) {
            log.info("Collection [{}] Does Not Exist. Trying to create", this.tableName);
            mongoService.createCollection(this.tableName);
        }
    }

    @Override
    public void close() {
        mongoService.getMongoDB().getMongo().close();
    }

    public String getTableName() {
        return tableName;
    }
    
    protected boolean keyExists(String id) {
        DBObject projection = new BasicDBObject();
        projection.put(KEY_ID, 1);

        DBCursor cursor = mongoService.findInCollection(tableName, keyQuery(id), projection);

         Iterator<DBObject> it = cursor.iterator();
        return it.hasNext();
    }

    protected DBObject keyQuery(String id) {
        return keyQuery(id, null);
    }

    protected DBObject keyQuery(String id, String user) {
        DBObject keyMatch = new BasicDBObject(KEY_ID, id);
        if (user != null) {
            DBObject ownerMatch = new BasicDBObject(KEY_OWNER, user);
            DBObject sharedMAtch = new BasicDBObject(ACCESS_LIST, user);

            BasicDBList exp1 = new BasicDBList();
            exp1.add(ownerMatch);
            exp1.add(sharedMAtch);

            keyMatch.put("$or", exp1);
        }
        return keyMatch;
    }
    
    public abstract byte[] generateKey(String keyId, String owner, String... sharedWith) throws KeyExistsException;
    
    public abstract byte[] insertKey(String keyId, byte[] keyData, String owner, String... sharedwith) throws KeyExistsException;
    
    public abstract void removeKey(String keyId, String owner) throws KeyNotFoundException;
    
    public abstract byte[] getKey(String keyId, String owner) throws KeyNotFoundException;
}
