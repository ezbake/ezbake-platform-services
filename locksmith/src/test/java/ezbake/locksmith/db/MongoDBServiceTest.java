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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import ezbake.locksmith.service.BaseTest;

import org.apache.commons.io.FileUtils;
import org.bson.types.ObjectId;

public class MongoDBServiceTest extends BaseTest {
	private Logger log = LoggerFactory.getLogger(MongoDBServiceTest.class);
	private static MongoDBService mongoService;

    private static final String data = "ASDFADJSJDJDJDJDKSDLFJDASKFJSAKDFJSDKFJKSADJFDSA";

	@BeforeClass
	public static void init() throws Exception {
	    BaseTest.init();
        mongoService = new MongoDBService(ezConfiguration);
	}
	
	@After
	public void clean() {
		mongoService.dropCollectionFromDb("lock_smith_thrift_keys");
	}
	
	@Test
	public void testInsert() {
		log.info("Collections {}", mongoService.getCollections());
	}

    @Test
    public void testListVsSingle() {
        BasicDBObject insert = new BasicDBObject("test", 1).append("owner", "Jeff");
        mongoService.insertDocumentIntoCollection("mycol", insert);

        BasicDBObject query = new BasicDBObject("test", 1).append("owner", "Jeff");
        BasicDBObject proj = new BasicDBObject("test", 1).append("owner", 1);
        log.debug("Doc: {}", mongoService.findInCollection("mycol", query, proj));

        insert = new BasicDBObject("tester", 1).append("owner", Lists.newArrayList("Jeff", "Eric"));
        mongoService.insertDocumentIntoCollection("mycol", insert);

        query = new BasicDBObject("tester", 1).append("owner", "Eric");
        proj = new BasicDBObject("tester", 1).append("owner", 1);
        log.debug("Doc: {}", mongoService.findInCollection("mycol", query, proj));
    }
	
	@Test
	public void testSimpleDocInsert() throws IOException {
		log.info("Test Simple Doc Insert");
		
		BasicDBObject doc = new BasicDBObject("type", "RSA").append("keyData", data);
		
		mongoService.insertDocumentIntoCollection("lock_smith_thrift_keys", doc);
	}
	
	@Test
	public void testSimpleDocFindAll() throws IOException {
		log.info("Test Simple Doc Delete");

		BasicDBObject doc = new BasicDBObject("type", "RSA").append("keyData", data);

		mongoService.insertDocumentIntoCollection("lock_smith_thrift_keys", doc);

        BasicDBObject doc2 = new BasicDBObject("type", "RSA").append("keyData", data);
		ObjectId objId = mongoService.insertDocumentIntoCollection("lock_smith_thrift_keys", doc2);
		
		BasicDBObject proj = new BasicDBObject();
		BasicDBObject query = new BasicDBObject("_id", objId);
		
		proj.put("keyData", 1);
		proj.put("type", 1);
		
		DBCursor cursor = mongoService.findInCollection("lock_smith_thrift_keys", query, proj);

        for (DBObject currObj : cursor) {
            String typeObj = (String) currObj.get("type");
            assertTrue(typeObj.equals("RSA"));
        }
	}
	
	@Test
	public void testRemoveDoc() throws IOException {
		log.info("Test Remove Doc");
		BasicDBObject doc = new BasicDBObject("type", "RSA").append("keyData", data);
		ObjectId objId = mongoService.insertDocumentIntoCollection("lock_smith_thrift_keys", doc);
		
		BasicDBObject query = new BasicDBObject("_id", objId);
		BasicDBObject proj = new BasicDBObject();
		
		proj.put("keyData", 1);
		proj.put("type", 1);
		
		mongoService.removeDocumentFromCollection("lock_smith_thrift_keys", query);

		DBCursor cursor = mongoService.findInCollection("lock_smith_thrift_keys", query, proj);
        for (DBObject aCursor : cursor) {
            String typeObj = (String) aCursor.get("type");
            assertTrue(typeObj.equals("RSA"));
        }
	}
	
}
