/*   Copyright (C) 2013-2015 Computer Sciences Corporation
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

package ezbake.profile.service.exe;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.profile.EzProfile;
import ezbake.profile.SearchResult;
import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.profile.service.EzProfileHandler;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.security.ua.UAModule;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class ThreadedServiceExecutorTest {
	private static Logger log = LoggerFactory.getLogger(ThreadedServiceExecutorTest.class);
	
	private static ThriftServerPool pool;
	private static ThriftClientPool clientPool;
	private static UserAttributeService userAttributeService;
	public  static final String SERVICE_NAME = "EzProfile";
	public static final String SECURITY_ID = "1234567890";
	
	@BeforeClass
	public static void init() throws Exception {
        final Properties properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

		pool = new ThriftServerPool(properties, 61234);
        pool.startCommonService(new EzProfileHandler(), SERVICE_NAME, SECURITY_ID);

        clientPool = new ThriftClientPool(properties);
        userAttributeService = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Properties.class).toInstance(properties);
            }
        }, new UAModule(properties)).getInstance(UserAttributeService.class);
    }
	
	@AfterClass
	public static void clean() {
		if(pool != null) {
            log.info("Closing server pool");
			pool.shutdown();
		}
	}
	
	@Test
	public void staticListTest() throws TException {
		String[] list = {
                "ddixon.84844",
                "rgrimes"
        };

		Set<String> dnSet = new HashSet<>(Lists.asList(list[0], list));
		ThreadedUserProfileExecutor tupExe = new ThreadedUserProfileExecutor(userAttributeService,  Runtime.getRuntime().availableProcessors());
		long start = System.currentTimeMillis();
		Map<String, User> upMap = tupExe.execute(dnSet);

		long end = System.currentTimeMillis();
		double delta = (end-start)/1000.0;
	}
	
	@Test
	public void test() throws TException {

		Set<String> dnSet = new HashSet<String>();

		EzProfile.Client client = null;
		EzSecurityToken ezToken = MockEzSecurityToken.getMockUserToken("tester");
		
		try {
			client = clientPool.getClient(SERVICE_NAME, EzProfile.Client.class);
			
			SearchResult result = client.searchDnByName(ezToken, "*", "*");
			Set<String> data = result.getPrincipals();
			
			log.info("Got Data {}", data);
			
			for(String dn : data) {
				dnSet.add(dn);
			}
		}
		finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
		}
		
		
		ThreadedUserProfileExecutor tupExe = new ThreadedUserProfileExecutor(userAttributeService,  Runtime.getRuntime().availableProcessors());
		long start = System.currentTimeMillis();
		Map<String, User> upMap = tupExe.execute(dnSet);
		
		long end = System.currentTimeMillis();
		double delta = (end-start)/1000.0;
		log.info("User Profile Count " + upMap.size());
		log.info("Time: " + delta + " seconds");
		
		Set<String> keys = upMap.keySet();
		Iterator<String> it = keys.iterator();
		
		for(Map.Entry<String, User> entry : upMap.entrySet()) {
			User currProfile = entry.getValue();
			assertTrue(currProfile != null);
			assertTrue(currProfile.getFirstName() != null);
		}
	}



}
