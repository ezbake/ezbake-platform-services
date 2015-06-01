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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.ins.INSUtility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.data.elastic.EzElasticHandler;
import ezbake.ins.thrift.gen.AppAccess;
import ezbake.ins.thrift.gen.AppService;
import ezbake.ins.thrift.gen.Application;
import ezbake.ins.thrift.gen.ApplicationNotFoundException;
import ezbake.ins.thrift.gen.ApplicationSummary;
import ezbake.ins.thrift.gen.BroadcastTopic;
import ezbake.ins.thrift.gen.FeedPipeline;
import ezbake.ins.thrift.gen.FeedType;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.ins.thrift.gen.ListenerPipeline;
import ezbake.ins.thrift.gen.WebApplication;
import ezbake.ins.thrift.gen.WebApplicationLink;
import ezbake.query.intents.IntentType;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import ezbake.thrift.ThriftTestUtils;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;

public class InternalNameServiceTest {
    private static ThriftClientPool pool;
    private static ThriftServerPool servers;
    private static String TestAppId = "1010010";
    private static String appName;
    private InternalNameService.Client client;
    private static EzConfiguration configuration;
    private static EsSetup esSetup;
    private static EzSecurityToken securityToken = ThriftTestUtils.generateTestSecurityToken("ins", "ins",
            Lists.newArrayList("U"));

    @BeforeClass
    public static void init() throws Exception {
        configuration = new EzConfiguration(new ClasspathConfigurationLoader());

        final ElasticsearchConfigurationHelper elasticConfig = new ElasticsearchConfigurationHelper(configuration.getProperties());
        final Settings settings =
                ImmutableSettings.settingsBuilder()
                        .put("cluster.name", elasticConfig.getElasticsearchClusterName())
                                // Use supplied cluster because production would use it
                        .put("network.host", elasticConfig.getElasticsearchHost())
                                // Use supplied host because production would use it
                        .put("transport.tcp.port", elasticConfig.getElasticsearchPort())
                                // Use supplied port because production would use it
                        .put("node.local", false)
                                // ESClient not written for local mode, would need additional param
                        .put("script.native.visibility.type",
                                "ezbake.data.elastic.security.EzSecurityScriptFactory").build();
        // Local Testing
        esSetup = new EsSetup(settings);

        // Start up elastic search server
        appName = new EzBakeApplicationConfigurationHelper(configuration.getProperties()).getApplicationName();
        deleteIndex(appName);

        Thread.sleep(3000);
        resetThrift(true);
    }

    private static void deleteIndex(String name) throws Exception {
        if (esSetup.exists(name)) {
            esSetup.execute(EsSetup.deleteIndex(name));
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Shutdown our client connections and the services
        pool.close();
        servers.shutdown();

        deleteIndex(appName);
        resetThrift(true);
    }

    @Before
    public void AddApps() throws Exception {
        client = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
        client.saveApplication(getTestApplication(), securityToken);
        Application secondApp = getTestApplication("Second App", "555555",
                Sets.newHashSet("CN=Dummy User, OU=People, O=Name Service, C=US"));
        secondApp.webApp.getUrnMap().clear();
        secondApp.webApp.getUrnMap().put("Dummy", new WebApplicationLink().setAppName("Test"));
        secondApp.getCategories().clear();
        client.saveApplication(secondApp, securityToken);
    }

    @After
    public void DeleteApp() throws Exception {
        client.deleteApplication(TestAppId, securityToken);
        client.deleteApplication("555555", securityToken);
        pool.returnToPool(client);
    }

    private Application getTestApplication() {
        return getTestApplication("MyApp", TestAppId, Sets.newHashSet("CN=Test One, OU=People, O=Name Service, C=US",
                "CN=Two Test, OU=People, O=Name Service, C=US"));
    }

    private Application getTestApplication(String appName, String appId, Set<String> users) {
        Application a = new Application();
        a.setAppName(appName);
        a.setPoc("Unit Tester");
        a.setAllowedUsers(users);

        HashMap<String, String> categories = new HashMap<>();
        categories.put("CNN", "NEWS");
        categories.put("Twitter", "SOCIAL");
        a.setCategories(categories);
        a.setId(appId);
        a.setAuthorizations(Sets.newHashSet("U", "FOUO"));

        FeedPipeline feed = new FeedPipeline();
        feed.setFeedName("CNN");
        BroadcastTopic feedTopic = new BroadcastTopic();
        feedTopic.setDescription("News Feed");
        feedTopic.setName("cnn");
        feedTopic.setThriftDefinition("a thrift idl is expected here");
        feed.setBroadcastTopics(Sets.newHashSet(feedTopic));
        feed.setDescription("My CNN Feed");
        feed.setExportingSystem("CNN.com");
        feed.setType("Streaming");
        feed.setDataType("XML");
        a.setFeedPipelines(Sets.newHashSet(feed));

        WebApplication webApp = new WebApplication();
        webApp.setIsChloeEnabled(true);
        Map<String, WebApplicationLink> urnMap = new HashMap<>();
        WebApplicationLink link = new WebApplicationLink();
        link.setWebUrl("https://apps.some.domain.com/cnn?id={id}");
        urnMap.put("NEWS://CNN/", link);
        webApp.setUrnMap(urnMap);
        a.setWebApp(webApp);

        ListenerPipeline listener = new ListenerPipeline();
        BroadcastTopic broadcastTopic = new BroadcastTopic();
        broadcastTopic.setDescription("CNN Normalized");
        broadcastTopic.setName("CNN-Norm");
        broadcastTopic.setThriftDefinition("a thrift idl is expected here");
        listener.setBroadcastTopics(Sets.newHashSet(broadcastTopic));
        listener.setDescription("Indexing pipeline for CNN Data");
        listener.setFeedName("CNN-Index");
        listener.setListeningTopics(Sets.newHashSet("cnn-breaking", "cnn-us"));
        a.setListenerPipelines(Sets.newHashSet(listener));

        Map<String, String> intentServiceMap = new HashMap<>();
        intentServiceMap.put(IntentType.LOCATION.name(), "Service1");
        intentServiceMap.put(IntentType.IMAGE.name(), "Service2");
        a.setIntentServiceMap(intentServiceMap);

        AppAccess appAccess = new AppAccess();
        appAccess.isNotRestricted = false;
        Map<String, String> restrictedApps = new HashMap<>();
        restrictedApps.put(appName, appId);
        appAccess.setRestrictedToApps(restrictedApps);
        a.setAppAccess(appAccess);
        return a;
    }

    @Test
    public void testGetApplication() throws Exception {
        Application app = client.getAppById(TestAppId, securityToken);
        assertNotNull(app);
    }

    @Test
    public void testIsAppNameExists() throws Exception {
        Application newApp = getTestApplication();
        newApp.setId("123-My-ID");
        newApp.setAppName("StarWars");
        client.saveApplication(newApp, securityToken);

        assertTrue(client.getDuplicateAppNames("StarWars", securityToken).size() > 0);
        assertFalse(client.getDuplicateAppNames("StarGate", securityToken).size() > 0);

        client.deleteApplication("123-My-ID", securityToken);
    }

    @Test
    public void testFeedsWereAdded() throws Exception {
        Set<FeedPipeline> feeds = client.getPipelineFeeds();
        assertNotNull(feeds);
        assertEquals(1, feeds.size());
        assertEquals("CNN", feeds.iterator().next().getFeedName());
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void testDeleteApplication() throws Exception {
        Application newApp = getTestApplication();
        newApp.setId("XXXX");
        client.saveApplication(newApp, securityToken);
        assertNotNull(client.getAppById("XXXX", securityToken));
        client.deleteApplication("XXXX", securityToken);
        client.getAppById("XXXX", securityToken);
    }

    @Test
    public void testCategories() throws Exception {
        //Add a couple categories
        client.addCategory("NEWS", securityToken);
        client.addCategory("LINK", securityToken);
        Set<String> expected = Sets.newHashSet("NEWS", "LINK");
        //Test that the add and get calls are functioning
        Set<String> actual = client.getCategories();
        assertEquals(expected, actual);
        //Now test removing a category
        client.removeCategory("LINK", securityToken);
        actual = client.getCategories();
        expected.remove("LINK");
        assertEquals(expected, actual);
    }

    @Test
    public void testMoreThan10() throws Exception {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            client.addCategory(Integer.toString(i), securityToken);
            expected.add(Integer.toString(i));
        }
        try {
            Set<String> actual = client.getCategories();
            assertEquals(expected, actual);
        } finally {
            for (String category : expected) {
                client.removeCategory(category, securityToken);
            }
        }
    }

    @Test
    public void testSystemTopics() throws Exception {
        //Add a couple categories
        client.addSystemTopic("SystemTopic_1", securityToken);
        client.addSystemTopic("SystemTopic_2", securityToken);
        Set<String> expected = Sets.newHashSet("SystemTopic_1", "SystemTopic_2");
        //Test that the add and get calls are functioning
        Set<String> actual = client.getSystemTopics();
        assertEquals(expected, actual);
        //Now test removing a category
        client.removeSystemTopic("SystemTopic_2", securityToken);
        actual = client.getSystemTopics();
        expected.remove("SystemTopic_2");
        assertEquals(expected, actual);
    }

    @Test
    public void testGetURIPrefix() throws Exception {
        Application app = getTestApplication();
        Map.Entry<String, String> categoryEntry = app.getCategories().entrySet().iterator().next();

        String uriPrefix = client.getURIPrefix(app.getId(), categoryEntry.getKey());
        assertEquals(INSUtility.buildUriPrefix(categoryEntry.getValue(), categoryEntry.getKey()), uriPrefix);
    }

    @Test
    public void testGetURIPrefixes() throws Exception {

        // test one app that has expected categories
        Application app = getTestApplication();
        Set<String> expected = Sets.newHashSet();

        for (Map.Entry<String, String> categoryEntry : app.getCategories().entrySet()) {
            expected.add(INSUtility.buildUriPrefix(categoryEntry.getValue(), categoryEntry.getKey()));
        }
        Set<String> actual = client.getURIPrefixes();

        assertEquals(expected, actual);

        // test for missing categories
        app.setCategories(null);
        client.saveApplication(app, securityToken);

        actual = client.getURIPrefixes();
        assertEquals(0, actual.size());
    }


    @Test
    public void testGetChloeWebApps() throws Exception {

        try {
            // create application that is chloe enabled
            Application app = getTestApplication();
            app.webApp.setChloeWebUrl("https://ChloeURL");
            client.saveApplication(app, securityToken);

            String expected = app.getWebApp().getChloeWebUrl();

            // create application that is not chloe enabled
            Application falseApp = getTestApplication();
            falseApp.setId("False App");
            falseApp.setAppName("False App Name");
            WebApplication webApp = new WebApplication();
            webApp.setIsChloeEnabled(false);
            falseApp.setWebApp(webApp);
            client.saveApplication(falseApp, securityToken);

            Set<WebApplicationLink> actual = client.getChloeWebApps();

            // expected 1 match
            assertEquals(1, actual.size());

            // assert validity of url
            assertEquals(expected, actual.iterator().next().getWebUrl());

            // test - two applications with same uri
            // one app has more than one map
            falseApp.webApp.isChloeEnabled = true;
            falseApp.webApp.setChloeWebUrl("https://ChloeURL");
            falseApp.setWebApp(webApp);
            client.saveApplication(falseApp, securityToken);

            actual = client.getChloeWebApps();

            // expected 2 matches
            assertEquals(2, actual.size());

            // assert validity of urls
            for (WebApplicationLink aWebApp : actual) {
                assertEquals(expected, aWebApp.getWebUrl());
            }

        } finally {
            client.deleteApplication("False App", securityToken);
        }
    }

    @Test
    public void testWebAppByPrefix() throws Exception {
        try {
            // create application that has searched uri
            Application app = getTestApplication();
            WebApplication expected = app.getWebApp();

            // create application that does not have searched uri
            Application falseApp = getTestApplication();
            falseApp.setId("False App");
            falseApp.setAppName("False App Name");
            WebApplication webApp = new WebApplication();
            webApp.setIsChloeEnabled(true);
            Map<String, WebApplicationLink> urnMap = new HashMap<>();
            WebApplicationLink link = new WebApplicationLink();
            link.setWebUrl("https://apps.some.domain.com/cnn?id={id}");
            urnMap.put("FALSE_URN://cnn/", link);
            webApp.setUrnMap(urnMap);
            falseApp.setWebApp(webApp);
            client.saveApplication(falseApp, securityToken);

            // searched uri
            Map.Entry<String, WebApplicationLink> urnEntry = expected.getUrnMap().entrySet().iterator().next();
            String uri = urnEntry.getKey() + "/myUniqueId/12323213.xml";

            // test - expected one application with same uri
            Set<WebApplicationLink> actual = client.getWebAppsForUri("NEWS://CNN");

            // expected 1 match
            assertEquals(1, actual.size());

            // assert validity of url
            assertEquals(urnEntry.getValue().getWebUrl(), actual.iterator().next().getWebUrl());

            // test - two applications with same uri
            // one app has more than one map

            link = new WebApplicationLink();
            link.setWebUrl("https://apps.some.domain.com/cnn?id={id}");
            urnMap.put("NEWS://CNN/", link);
            webApp.setUrnMap(urnMap);
            falseApp.setWebApp(webApp);
            client.saveApplication(falseApp, securityToken);

            actual = client.getWebAppsForUri(uri);

            // expected 2 matches
            assertEquals(2, actual.size());

            // assert validity of urls
            for (WebApplicationLink aWebApp : actual) {
                assertEquals(urnEntry.getValue().getWebUrl(), aWebApp.getWebUrl());
            }

        } finally {
            client.deleteApplication("False App", securityToken);
        }
    }

    @Test
    public void testGetMyApps() throws Exception {

        String newAppId = "secondApp";
        try {
            //First let's add another application
            Application newApp = getTestApplication();
            newApp.setId(newAppId);
            newApp.setAllowedUsers(Sets.newHashSet("CN=Two Test, OU=People, O=Name Service, C=US"));
            client.saveApplication(newApp, securityToken);

            //test for exact match - expected 1 match
            securityToken.getTokenPrincipal().setPrincipal("CN=Test One, OU=People, O=Name Service, C=US");
            Set<Application> testResult = client.getMyApps(securityToken);
            assertEquals("Can't find original match", 1, testResult.size());

            //test for exact match - expected 2 matchs
            securityToken.getTokenPrincipal().setPrincipal("CN=Two Test, OU=People, O=Name Service, C=US");
            testResult = client.getMyApps(securityToken);
            assertEquals("Can't find new match", 2, testResult.size());

            // test for exact match - expected 0 matches
            securityToken.getTokenPrincipal().setPrincipal("CN=Test Test, OU=People, O=Name Service, C=US");
            testResult = client.getMyApps(securityToken);
            assertEquals(0, testResult.size());

            // test for exact match - expected 0 matches
            securityToken.getTokenPrincipal().setPrincipal("Test");
            testResult = client.getMyApps(securityToken);
            assertEquals(0, testResult.size());

        } finally {
            //clean it up
            client.deleteApplication(newAppId, securityToken);
        }
    }

    @Test
    public void testGetAppByName() throws Exception {
        ApplicationSummary summary = client.getAppByName(getTestApplication().getAppName(), securityToken);
        assertEquals(getTestApplication().getPoc(), summary.getPoc());
        assertEquals(getTestApplication().getId(), summary.getId());
    }

    @Test
    public void testGetAppByNameWithId() throws Exception {
        ApplicationSummary summary = client.getAppByName(getTestApplication().getId(), securityToken);
        assertEquals(getTestApplication().getPoc(), summary.getPoc());
        assertEquals(getTestApplication().getId(), summary.getId());
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void testGetAppByNameThrows() throws Exception {
        client.getAppByName("notreal", securityToken);
    }

    @Test
    public void testAppsThatSupportIntent() throws Exception {

        String newAppId = "secondApp";
        try {
            // create and save test app
            Application newApp = getTestApplication();
            newApp.setId(newAppId);
            newApp.setAllowedUsers(Sets.newHashSet("CN=Two Test, OU=People, O=Name Service, C=US"));
            Map<String, String> intentMap = Maps.newHashMap();
            intentMap.put(IntentType.ACTIVITY.name(), "Activity");
            newApp.setIntentServiceMap(intentMap);
            client.saveApplication(newApp, securityToken);

            //test for exact match - expected 1 match
            Set<AppService> testResult = client.appsThatSupportIntent(IntentType.ACTIVITY.name());
            assertEquals("Can't find original match", 1, testResult.size());

            // test for 0 matches
            testResult = client.appsThatSupportIntent(IntentType.RELATIONSHIP.name());
            assertEquals("Can't find original match", 0, testResult.size());

            // test for illegal intent name
            testResult = client.appsThatSupportIntent("RANDOM_INTENT");
            assertEquals("Can't find original match", 0, testResult.size());

        } finally {
            //clean it up
            client.deleteApplication(newAppId, securityToken);
        }
    }

    @Test
    public void testImportExport() throws Exception {
        String exportedApp = client.exportApplication(TestAppId, securityToken);
        client.deleteApplication(TestAppId, securityToken);
        //Make sure there's nothing there
        securityToken.getTokenPrincipal().setPrincipal("CN=Test One, OU=People, O=Name Service, C=US");
        Set<Application> unitsApps = client.getMyApps(securityToken);
        assertEquals(0, unitsApps.size());
        //Now import
        client.importApplication(exportedApp, securityToken);
        //Make sure something is there
        Set<Application> unitsAppsNow = client.getMyApps(securityToken);
        assertEquals(1, unitsAppsNow.size());
    }

    @Test
    public void testInvalidImport() throws Exception {
        try {
            client.importApplication("Invalid", securityToken);
            fail("Expecting Exception");
        } catch (TException ex) {
            resetThrift(true);
            client = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
        }
    }

    @Test
    public void testDeleteInvalidAppId() throws Exception {
        assertFalse(client.deleteApplication("Invalid", securityToken));
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void testUriPrefixInvalidAppId() throws Exception {
        client.getURIPrefix("Invalid", "Invalid");
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void testUriPrefixInvalidCategory() throws Exception {
        client.getURIPrefix(TestAppId, "Invalid");
    }

    @Test
    public void testPing() throws Exception {
        assertTrue(client.ping());
    }

    @Test
    public void testAllBroadcastTopicNames() throws Exception {
        Application app1 = getTestApplication();
        Application app2 = getTestApplication();

        app2.setId("0101101"); //id must be unique; so, we change it.
        app2.setAppName("app2"); //name must be unique; so, we change it.
        app2.setListenerPipelines(new HashSet<ListenerPipeline>());
        try {
            client.saveApplication(app1, securityToken);
            client.saveApplication(app2, securityToken);

            Set<String> allBroadcastTopicNames = client.allBroadcastTopicNames(FeedType.ALL);

            Set<String> expectedVal = new HashSet<>();
            Set<BroadcastTopic> topics = new HashSet<>();

            Set<FeedPipeline> feeds = Sets.newHashSet(app1.getFeedPipelines());
            for (FeedPipeline feed : feeds) {
                topics.addAll(feed.getBroadcastTopics());
            }

            Set<ListenerPipeline> listeners = Sets.newHashSet(app1.getListenerPipelines());
            for (ListenerPipeline listener : listeners) {
                topics.addAll(listener.getBroadcastTopics());
            }

            for (BroadcastTopic topic : topics) {
                expectedVal.add(topic.getName());
            }

            assertEquals(allBroadcastTopicNames, expectedVal);
        } finally {
            client.deleteApplication(app2.getId(), securityToken);
        }
    }

    @Test
    public void testGetAllApplicationsSummary() throws Exception {
        String newAppId = "secondApp";

        Map<String, String> expected = new HashMap<>();
        expected.put("Second App", "555555");
        expected.put("MyApp", "1010010");

        try {
            Map<String, String> actual = new HashMap<>();
            Set<ApplicationSummary> summary = client.getAllApplicationsSummary();

            for (ApplicationSummary as : summary) {
                actual.put(as.getAppName(), as.getId());
            }

            assertEquals(expected, actual);

        } finally {
            client.deleteApplication(newAppId, securityToken);
        }
    }

    private static void resetThrift(boolean resetDocumentService) throws Exception {

        if (pool != null) {
            pool.close();
        }

        if (servers != null) {
            servers.shutdown();
        }

        servers = new ThriftServerPool(configuration.getProperties(), 14000);
        if (resetDocumentService) {
            servers.startApplicationService(new EzElasticHandler(), "documentService",
                    InternalNameServiceConstants.SERVICE_NAME, "ins");
        }
        servers.startCommonService(new InternalNameServiceHandler(), InternalNameServiceConstants.SERVICE_NAME, "ins");

        //Still create our client pool like we would for real
        pool = new ThriftClientPool(configuration.getProperties());
    }
}
