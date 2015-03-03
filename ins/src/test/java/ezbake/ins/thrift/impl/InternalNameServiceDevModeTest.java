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

package ezbake.ins.thrift.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.common.ins.INSUtility;
import ezbake.ins.thrift.gen.AppService;
import ezbake.ins.thrift.gen.ApplicationSummary;
import ezbake.ins.thrift.gen.FeedPipeline;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.ins.thrift.gen.JobRegistration;
import ezbake.ins.thrift.gen.WebApplicationLink;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * This class is to test the few methods that are available in dev mode
 */
@Ignore //hate doing this, but just can't get this to run consistently on jenkins
public class InternalNameServiceDevModeTest {
    private static ThriftClientPool pool;
    private static ThriftServerPool servers;
    private static final String TestAppId = "1010010";
    private InternalNameService.Client client;

    @BeforeClass
    public static void init() throws Exception {
        EzConfiguration configuration = new EzConfiguration(new ClasspathConfigurationLoader(
                ClasspathConfigurationLoader.CLASSPATH_DEFAULT_RESOURCE, "/devMode.properties"
        ));
        Properties properties = configuration.getProperties();
        servers = new ThriftServerPool(properties, 15750);
        servers.startCommonService(new InternalNameServiceHandler(), InternalNameServiceConstants.SERVICE_NAME, "ins");

        pool = new ThriftClientPool(properties);

    }

    @AfterClass
    public static void cleanup() {
        //Shutdown our client connections and the services
        pool.close();
        servers.shutdown();
    }


    @Before
    public void beforeTest() throws Exception {
        client = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
    }

    @After
    public void afterTest() throws Exception {
        pool.returnToPool(client);
    }

    @Test
    public void testGetUriPrefix() throws Exception {
        assertEquals(INSUtility.buildUriPrefix(InternalNameServiceHandler.DevModePrefix, "MyCategory"),
                client.getURIPrefix(TestAppId, "MyCategory"));
    }

    @Test
    public void testGetTopic() throws Exception {
        Set<String> expected = Sets.newHashSet("TestFeedTopic1", "TestFeedTopic2");
        assertEquals(expected, client.getTopicsForFeed(TestAppId, "TestFeed"));
    }

    @Test(expected = Exception.class)
    public void testUnsupportedMethod() throws Exception {
        client.getCategories();
    }

    @Test
    public void testGetIntents() throws Exception {
        AppService a1 = new AppService("MyFirstApp", "MyIntentService1");
        AppService a2 = new AppService("MySecondApp", "MyIntentService2");
        Set<AppService> expectedApps = Sets.newHashSet(a1, a2);
        Set<AppService> actualApps = client.appsThatSupportIntent("myIntent");
        assertEquals(expectedApps, actualApps);
    }

    @Test
    public void testGetWebAppsForURI() throws Exception {
        Set<WebApplicationLink> expectedLinks = Sets.newHashSet(
                buildWebApp("www.firstapp.com", "MyFirstApp", false),
                buildWebApp("www.secondapp.com", "MySecondApp", true));
        Set<WebApplicationLink> actualLinks = client.getWebAppsForUri("NEWS://CNN");
        assertEquals(expectedLinks, actualLinks);
    }

    @Test
    public void testGetChloeApps() throws Exception {
        Set<WebApplicationLink> expectedLinks = Sets.newHashSet(
                buildWebApp("www.firstapp.com", "MyFirstApp", false),
                buildWebApp("www.secondapp.com", "MySecondApp", true));
        Set<WebApplicationLink> actualLinks = client.getChloeWebApps();
        assertEquals(expectedLinks, actualLinks);
    }

    @Test
    public void testGetSystemTopics() throws Exception {
        Set<String> expectedTopics = Sets.newHashSet("SSR");
        Set<String> actualTopics = client.getSystemTopics();
        assertEquals(expectedTopics, actualTopics);
    }

    @Test
    public void testGetJobRegistrations() throws Exception {
        JobRegistration job1 = new JobRegistration();
        job1.setFeedName("CNN");
        job1.setJobName("cnnBatch");
        Set<JobRegistration> expectedJobs = Sets.newHashSet(job1);
        Set<JobRegistration> actualJobs = client.getJobRegistrations("12345",
                ThriftTestUtils.generateTestSecurityToken("ins", "ins",
                        Lists.newArrayList("U")));
        assertEquals(expectedJobs, actualJobs);
    }

    @Test
    public void testGetAppByName() throws Exception {
        ApplicationSummary expectedSummary = new ApplicationSummary().setAppName("myTestApp").setId("12345")
                .setExternalUri("/myApp").setPoc("test@test.com").setSponsoringOrganization("MyOrg");
        ApplicationSummary actualSummary = client.getAppByName("myTestApp",
                ThriftTestUtils.generateTestSecurityToken("ins", "ins", Lists.newArrayList("U")));
        assertEquals(expectedSummary, actualSummary);
    }

    @Test
    public void testGetPipelineFeeds() throws Exception {
        FeedPipeline one = new FeedPipeline().setFeedName("myTestFeed").setDescription("A feed to test");
        FeedPipeline two = new FeedPipeline().setFeedName("mySecondFeed").setDescription("A feed to test two");
        Set<FeedPipeline> expected = Sets.newHashSet(one, two);
        Set<FeedPipeline> actual = client.getPipelineFeeds();
        assertEquals(expected, actual);
    }

    private WebApplicationLink buildWebApp(String url, String name, boolean includePrefix) {
        WebApplicationLink link = new WebApplicationLink();
        link.setWebUrl(url);
        link.setAppName(name);
        link.setIncludePrefix(includePrefix);
        return link;
    }

}
