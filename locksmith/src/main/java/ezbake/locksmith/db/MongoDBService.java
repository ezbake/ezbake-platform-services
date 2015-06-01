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

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

import com.mongodb.*;
import ezbakehelpers.mongoutils.MongoHelper;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date 05/21/14
 * @author gdrocella
 * time: 2:07pm
 */
public class MongoDBService {
	private static final Logger log = LoggerFactory.getLogger(MongoDBService.class);

    private MongoHelper mongoHelper;
	private Mongo mongo;
	private DB db;

	public MongoDBService(Properties ezProperties) throws ManagerDbAccessException {
        mongoHelper = new MongoHelper(ezProperties);
        try {
            mongo = mongoHelper.getMongo();
        } catch (UnknownHostException ex) {
            throw new ManagerDbAccessException("Unknown Host when connecting to Mongo: " + ex.getMessage());
        }
		db = mongo.getDB(mongoHelper.getMongoConfigurationHelper().getMongoDBDatabaseName());
	}

    public DB getMongoDB() {
        return db;
    }

	public Set<String> getCollections() {
		return db.getCollectionNames();
	}
	
	
	/**
	 * Inserts a document into a collection
	 * @param collection - collection to insert document into
	 * @param doc - the document to be inserted 
	 * @return - true on success, false otherwise
	 */
	public ObjectId insertDocumentIntoCollection(String collection, DBObject doc) {
		boolean success = true;
		log.info("Insert Document into collection [{}]", collection);
		DBCollection coll = db.getCollection(collection);
		WriteResult result = coll.insert(doc);

		return (ObjectId) result.getUpsertedId();
	}
	
	
	public boolean dropCollectionFromDb(String collection) {
		DBCollection coll = db.getCollection(collection);
		coll.drop();
		
		return true;
	}
	
	public DBCursor findInCollection(String collection, DBObject search, DBObject projection) throws NullPointerException {
		DBCollection coll = db.getCollection(collection);
		
		return coll.find(search, projection);
	}
	
	public void removeDocumentFromCollection(String collection, BasicDBObject obj) {
		DBCollection coll = db.getCollection(collection);
		
		coll.remove(obj);
	}
	
	public boolean collectionExists(String collection) {
		return db.collectionExists(collection);
	}
	
	public void createCollection(String collection) {
		db.createCollection(collection, new BasicDBObject());
	}
	
	public long collectionCount(String collection) {
		DBCollection coll =  db.getCollection(collection);
		return coll.getCount();
	}
}
