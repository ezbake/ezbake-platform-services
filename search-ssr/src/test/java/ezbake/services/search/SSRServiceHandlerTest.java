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

package ezbake.services.search;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ezbake.base.thrift.*;
import ezbake.base.thrift.Date;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.common.TimeUtil;
import ezbake.data.elastic.EzElasticHandler;
import ezbake.data.elastic.thrift.*;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.service.processor.EzSecurityHandler;
import ezbake.security.thrift.EzSecurityServicesConstants;
import ezbake.security.ua.UAModule;
import ezbake.services.centralPurge.thrift.EzCentralPurgeConstants;
import ezbake.services.search.utils.SSRUtils;
import ezbake.thrift.ThriftServerPool;
import ezbake.thrift.ThriftTestUtils;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;
import org.apache.thrift.TException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.*;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.*;

import static ezbake.data.common.TimeUtil.getCurrentThriftDateTime;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

public class SSRServiceHandlerTest {
    private static SSRServiceHandler ssrService;
    private static ThriftServerPool serverPool;
    private static EzSecurityToken securityToken;
    private static final String SERVICE_NAME = "documentDataset";
    private static Node node;
    private static Properties props;

    @BeforeClass
    public static void startUp() {
        try {
            props = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
            ElasticsearchConfigurationHelper elasticConfig = new ElasticsearchConfigurationHelper(props);
            final Settings settings =
                    ImmutableSettings.settingsBuilder()
                            .put("script.disable_dynamic", false)
                            .put("cluster.name", elasticConfig.getElasticsearchClusterName())
                                    // Use supplied cluster because production would use it
                            .put("network.host", elasticConfig.getElasticsearchHost())
                                    // Use supplied host because production would use it
                            .put("transport.tcp.port", elasticConfig.getElasticsearchPort())
                                    // Use supplied port because production would use it
                            .put("script.native.visibility.type",
                                    "ezbake.data.elastic.security.EzSecurityScriptFactory").build();

            node = NodeBuilder.nodeBuilder().local(false).settings(settings).node();
            node.start();
            Thread.sleep(3000);

            props = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

            String securityId = props.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);
            securityToken = ThriftTestUtils.generateTestSecurityToken(
                    securityId, securityId, Arrays.asList("U"));

            System.out.println("principal:"+securityToken.getTokenPrincipal().getPrincipal());
            props.setProperty(EzBakePropertyConstants.EZBAKE_SSL_CIPHERS_KEY, "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA");
            props.setProperty(UAModule.UA_SERVICE_IMPL, FileUAService.class.getCanonicalName());
            Properties securityConfig = new Properties();
            securityConfig.putAll(props);
            securityConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, System.getProperty("user.dir") + File.separator + "search-ssr-service/src/test/resources/pki/server");
            System.out.println("user.dir="+System.getProperty("user.dir"));
            serverPool = new ThriftServerPool(securityConfig, 14000);
            serverPool.startCommonService(new EzSecurityHandler(), EzSecurityServicesConstants.SECURITY_SERVICE_NAME, "12345");

            // Get the application name for the ezelastic service
            String ezelasticAppName = props.getProperty(SSRServiceHandler.EZELASTIC_APPLICATION_NAME_KEY, null);
            serverPool.startApplicationService(new EzElasticHandler(), SERVICE_NAME, ezelasticAppName, securityId);

            ServiceDiscoveryClient discovery = new ServiceDiscoveryClient(props);
            discovery.setSecurityIdForCommonService(EzCentralPurgeConstants.SERVICE_NAME, securityId);
            discovery.close();

            ssrService = new SSRServiceHandler();
            props.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, System.getProperty("user.dir") + File.separator + "search-ssr-service/src/test/resources/pki/client");
            ssrService.setConfigurationProperties(props);
            ssrService.getThriftProcessor();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    @AfterClass
    public static void shutdown() throws Exception {
        node.client().admin().indices().prepareDelete("ssrindexing").get();
        node.stop();
        node.close();
        ssrService.shutdown();
        serverPool.shutdown();
    }

    @After
    public void cleanup() {
        node.client().prepareDeleteByQuery("ssrindexing")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute()
                .actionGet();
    }

    @Test
    public void testGetTypeFromURI() {
        String type = ssrService.getTypeFromUri("SOCIAL://chirp/tag:search:449728248529043456");
        assertEquals("Correct type generated", "SOCIAL:chirp", type);
    }

    @Test
    public void testSearchSsrCheckFormalVisibility() throws Exception {

        String uri = "DEV://chirp/tag:search:475858716546592768";
        String formalVisibility = "U";
        String key = "testkey";
        String value = String.valueOf(new java.util.Date().getTime());

        String securityId = props.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);
        EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken(
                securityId, securityId, Arrays.asList(formalVisibility));

        List<IndexResponse> responses = populateTestData(uri, formalVisibility, null, key, value, token);
        assertEquals("One index response expected", 1, responses.size());
        assertTrue("Index response should be a success", responses.get(0).isSuccess());

        String search = QueryBuilders.matchQuery(key, value).toString();
        Query query = new Query().setSearchString(search).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SSRSearchResult results = ssrService.searchSSR(query, token);

        List<SSR> matchingSsrs = results.getMatchingRecords();
        assertEquals("Expect one matching record.", 1, matchingSsrs.size());

        SSR querySsr = matchingSsrs.get(0);

        assertNotNull("Visibility should not be null", querySsr.getVisibility());
        assertEquals("Formal Markings not equal", formalVisibility, querySsr.getVisibility().getFormalVisibility());
    }

    @Test
    public void testSearchSsrCheckExternalCommunityVisibility() throws Exception {
        String uri = "DEV://chirp/tag:search:475858716546592768";
        String formalVisibility = "U";
        String extCommVisibility = "TEST";
        String key = "testkey";
        String value = String.valueOf(new java.util.Date().getTime());

        String securityId = props.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);
        EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken(
                securityId, securityId, Arrays.asList(formalVisibility));
        TreeSet<String> extCommSet = new TreeSet<>();
        extCommSet.add(extCommVisibility);
        token.getAuthorizations().setExternalCommunityAuthorizations(extCommSet);

        List<IndexResponse> responses = populateTestData(uri, formalVisibility, extCommVisibility, key, value, token);

        assertEquals("One index response expected", 1, responses.size());
        assertTrue("Index response should be a success", responses.get(0).isSuccess());

        //String search = QueryBuilders.queryString(value).toString();
        String search = QueryBuilders.matchQuery(key, value).toString();
        Query query = new Query().setSearchString(search).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SSRSearchResult results = ssrService.searchSSR(query, token);

        List<SSR> matchingSsrs = results.getMatchingRecords();
        assertEquals("Expect one matching record.", 1, matchingSsrs.size());

        SSR querySsr = matchingSsrs.get(0);

        assertNotNull("Visibility should not be null", querySsr.getVisibility());
        assertNotNull("Advanced Markings should not be null", querySsr.getVisibility().getAdvancedMarkings());
        assertEquals("External Community Visibility not equal", extCommVisibility,
                querySsr.getVisibility().getAdvancedMarkings().getExternalCommunityVisibility());
    }

    @Test
    public void testGetEnterpriseMetadata() throws Exception {
        populateTestData();

        // verify query by metadata
        String exemptCode22Query = QueryBuilders.multiMatchQuery("22", "exempt_code").toString();
        Query query = new Query().setSearchString(exemptCode22Query).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SSRSearchResult results = ssrService.searchSSR(query, securityToken);
        assertEquals("Should have one with exempt_code=22", 1, results.getTotalHits());

        String exemptCode14Query = QueryBuilders.multiMatchQuery("14", "exempt_code").toString();
        query = new Query().setSearchString(exemptCode14Query).setPage(new Page().setPageSize((short) 5).setOffset(0));
        results = ssrService.searchSSR(query, securityToken);
        assertEquals("Should have one with exempt_code=14", 1, results.getTotalHits());

        String exemptCodeNonExistentQuery = QueryBuilders.multiMatchQuery("6", "exempt_code").toString();
        query = new Query().setSearchString(exemptCodeNonExistentQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        results = ssrService.searchSSR(query, securityToken);
        assertEquals("Should have no match for exempt_code=6", 0, results.getTotalHits());

        String uspTrueQuery = QueryBuilders.multiMatchQuery("true", "usp").toString();
        query = new Query().setSearchString(uspTrueQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        results = ssrService.searchSSR(query, securityToken);
        assertEquals("Should have 2 with usp=true", 2, results.getTotalHits());

        String uspFalseQuery = QueryBuilders.multiMatchQuery("false", "usp").toString();
        query = new Query().setSearchString(uspFalseQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        results = ssrService.searchSSR(query, securityToken);
        assertEquals("Should have no match for usp=false", 0, results.getTotalHits());
    }

    @Test
    public void testBulkPut() throws TException {
        String key = "theField";
        String value1 = "first value";
        String value2 = "second value";

        Map<SSR, String> docs = Maps.newHashMap();
        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://test/12345");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());
        String json = "{\"" + key + "\": \"" + value1 + "\"}";
        docs.put(ssr1, json);

        SSR ssr2 = new SSR();
        ssr2.setUri("DEV://test/123456789");
        ssr2.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr2.setSnippet("some_snippet");
        ssr2.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 2005)));
        ssr2.setTitle("ssr_title");
        ssr2.setTimeOfIngest(getCurrentThriftDateTime());
        json = "{\"" + key + "\": \"" + value2 + "\"}";
        docs.put(ssr2, json);

        List<IndexResponse> responses = ssrService.putWithDocs(docs, securityToken);
        assertEquals("One index response expected", 2, responses.size());
        assertTrue("Index response 0 should be a success", responses.get(0).isSuccess());
        assertTrue("Index response 1 should be a success", responses.get(1).isSuccess());

        // Query by SSR result date
        String devTestQuery = QueryBuilders.rangeQuery(SSRUtils.SSR_DATE_FIELD).lt("010000Z NOV 05").toString();
        Query query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SSRSearchResult result = ssrService.searchSSR(query, securityToken);
        assertEquals("Two matches returned for date query", 2, result.getTotalHits());

        // Query by free text
        devTestQuery = QueryBuilders.queryString(key + ":second").toString();
        query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        result = ssrService.searchSSR(query, securityToken);
        assertEquals("One match returned for free text query", 1, result.getTotalHits());
        assertEquals("Correct URI returned with SSR", ssr2.getUri(), result.getMatchingRecords().get(0).getUri());

        // Match query
        devTestQuery = QueryBuilders.matchQuery(key, "first").toString();
        query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        result = ssrService.searchSSR(query, securityToken);
        assertEquals("One match returned for free text query", 1, result.getTotalHits());
        assertEquals("Correct URI returned with SSR", ssr1.getUri(), result.getMatchingRecords().get(0).getUri());

        for(SSR ssr: result.getMatchingRecords()){
            System.out.println("SSR timeOfIngest: "+ssr.getTimeOfIngest());
        }
    }

    @Test
    public void testHighlighting() throws TException {
        String key = "theField";
        String value1 = "lorem ipsum something or other";

        Map<SSR, String> docs = Maps.newHashMap();
        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://test/12345");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());
        String json = "{\"" + key + "\": \"" + value1 + "\"}";
        docs.put(ssr1, json);

        List<IndexResponse> responses = ssrService.putWithDocs(docs, securityToken);
        assertEquals("One index response expected", 1, responses.size());
        assertTrue("Index response should be a success", responses.get(0).isSuccess());

        // Match query
        String devTestQuery = QueryBuilders.queryString("ipsum").toString();
        Query query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        query.setHighlighting(new HighlightRequest().setFields(Sets.newHashSet(new HighlightedField("*"))));
        SSRSearchResult result = ssrService.searchSSR(query, securityToken);
        assertEquals("One match returned for free text query", 1, result.getTotalHits());
        assertEquals("Correct URI returned with SSR", ssr1.getUri(), result.getMatchingRecords().get(0).getUri());
        assertTrue("Highlight returned with em tags", result.getHighlights().get(ssr1.getUri()).getResults().get(key).get(0).contains("<em>ipsum</em>"));
    }

    @Test
    public void testPassThroughSearch() throws TException {
        String key = "theField";
        String value1 = "lorem ipsum something or other";

        Map<SSR, String> docs = Maps.newHashMap();
        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://test/12345");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());
        String json = "{\"" + key + "\": \"" + value1 + "\"}";
        docs.put(ssr1, json);

        List<IndexResponse> responses = ssrService.putWithDocs(docs, securityToken);
        assertEquals("One index response expected", 1, responses.size());
        assertTrue("Index response should be a success", responses.get(0).isSuccess());

        // Match query
        String devTestQuery = QueryBuilders.queryString("ipsum").toString();
        Query query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        query.setReturnedFields(Sets.newHashSet(key));
        SearchResult result = ssrService.search(query, securityToken);
        assertEquals("One match returned for free text query", 1, result.getTotalHits());
        assertEquals("Correct ID returned with JSON object", ssr1.getUri(), result.getMatchingDocuments().get(0).get_id());

        // Check that the requested field was returned
        JsonElement jsonElement = new JsonParser().parse(result.getMatchingDocuments().get(0).get_jsonObject());
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        assertTrue("Returned object contains the requested field", jsonObject.has(key));
        assertEquals("Correct value returned for field", value1, jsonObject.get(key).getAsString());
    }

    @Test
    public void testInsertMalformedObject() throws TException {
        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://test/12345");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());
        String json = "{\"somedate\": \"2001/01/01\"}";

        Map<SSR, String> docs = Maps.newHashMap();
        docs.put(ssr1, json);
        ssrService.putWithDocs(docs, securityToken);

        ssr1 = new SSR();
        ssr1.setUri("DEV://test/123456789");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());
        json = "{\"somedate\": \"this is not a date\"}";

        docs = Maps.newHashMap();
        docs.put(ssr1, json);
        List<IndexResponse> responses = ssrService.putWithDocs(docs, securityToken);
        assertEquals("One index response expected", 1, responses.size());
        assertTrue("Index response should be a success", responses.get(0).isSuccess());

        // Make sure that the first document is in there
        String devTestQuery = QueryBuilders.rangeQuery("somedate").lt("2001/01/02").toString();
        Query query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SSRSearchResult result = ssrService.searchSSR(query, securityToken);
        assertEquals("One match returned for correct JSON", 1, result.getTotalHits());
        assertEquals("Result has the correct URI", "DEV://test/12345", result.getMatchingRecords().get(0).getUri());

        // Verify that the malformed document is searchable
        devTestQuery = QueryBuilders.queryString("this is not a date").toString();
        query = new Query().setSearchString(devTestQuery).setPage(new Page().setPageSize((short) 5).setOffset(0));
        result = ssrService.searchSSR(query, securityToken);
        assertEquals("One match returned for malformed JSON", 1, result.getTotalHits());
        assertEquals("Result has the correct URI", "DEV://test/123456789", result.getMatchingRecords().get(0).getUri());
    }

    private void populateTestData() throws Exception {
        HashMap<SSR, String> ssrJsonMap = Maps.newHashMap();
        URL url1 = Resources.getResource("source1.json");
        String source1 = Resources.toString(url1, Charsets.UTF_8);
        URL url2 = Resources.getResource("source2.json");
        String source2 = Resources.toString(url2, Charsets.UTF_8);

        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://chirp/tag:search:475858716546592768");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());

        EnterpriseMetaData metaData = new EnterpriseMetaData();
        Map<String, String> tags = new HashMap<>();
        tags.put("usp", "true");
        tags.put("exempt_code", "22");
        metaData.setTags(tags);
        ssr1.setMetaData(metaData);

        ssrJsonMap.put(ssr1, source1);

        SSR ssr2 = new SSR();
        ssr2.setUri("DEV://chirp/tag:search:475858716546596474");
        ssr2.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr2.setSnippet("some_snippet");
        ssr2.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr2.setTitle("ssr_title");
        ssr2.setTimeOfIngest(getCurrentThriftDateTime());

        EnterpriseMetaData metaData2 = new EnterpriseMetaData();
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("usp", "true");
        tags2.put("exempt_code", "14");
        metaData2.setTags(tags2);
        ssr2.setMetaData(metaData2);

        ssrJsonMap.put(ssr2, source2);

        ssrService.putWithDocs(ssrJsonMap, securityToken);

        Thread.sleep(1000);
    }

    private List<IndexResponse> populateTestData(String uri, String formalVisibility,
                                                 String extCommVisibility, String key, String value, EzSecurityToken token) throws Exception {

        HashMap<SSR, String> ssrJsonMap = Maps.newHashMap();
        String json = "{\"" + key + "\": \"" + value + "\"}";

        Visibility visibility = new Visibility();
        visibility.setFormalVisibility(formalVisibility);
        if (extCommVisibility != null) {
            visibility.setAdvancedMarkings(new AdvancedMarkings().setExternalCommunityVisibility(extCommVisibility));
        }

        SSR ssr = new SSR();
        ssr.setUri(uri);
        ssr.setVisibility(visibility);
        ssr.setSnippet("some_snippet");
        ssr.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr.setTitle("ssr_title");
        ssr.setTimeOfIngest(getCurrentThriftDateTime());

        EnterpriseMetaData metaData = new EnterpriseMetaData();
        Map<String, String> tags = new HashMap<>();
        tags.put("usp", "true");
        tags.put("exempt_code", "22");
        metaData.setTags(tags);
        ssr.setMetaData(metaData);

        ssrJsonMap.put(ssr, json);
        return ssrService.putWithDocs(ssrJsonMap, token);
    }

    @Test
    public void testPurgeStatus() throws TException {
        long purgeId1 = System.currentTimeMillis();
        PurgeState state1 = new PurgeState();
        state1.setPurgeId(purgeId1);
        state1.setPurgeStatus(PurgeStatus.PURGING);
        state1.setTimeStamp(TimeUtil.convertToThriftDateTime(System
                .currentTimeMillis()));
        state1.setPurged(Sets.<Long>newHashSet());
        state1.setNotPurged(Sets.<Long>newHashSet());
        state1.setSuggestedPollPeriod(10000);

        long purgeId2 = System.currentTimeMillis();
        PurgeState state2 = new PurgeState();
        state2.setPurgeId(purgeId2);
        state2.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
        state2.setTimeStamp(TimeUtil.convertToThriftDateTime(System
                .currentTimeMillis()));
        state2.setPurged(Sets.<Long>newHashSet());
        Set<Long> notPurged = Sets.newHashSet();
        notPurged.add(12345l);
        state2.setNotPurged(notPurged);
        state2.setSuggestedPollPeriod(10000);

        ssrService.insertPurgeStatus(state1,
                new Visibility().setFormalVisibility("U"), securityToken);
        ssrService.insertPurgeStatus(state2,
                new Visibility().setFormalVisibility("U"), securityToken);

        PurgeState purgeState = ssrService.purgeStatus(securityToken, purgeId1);
        assertEquals("Should get expected purge1 status", state1.getPurgeStatus(), purgeState.getPurgeStatus());
        purgeState = ssrService.purgeStatus(securityToken, purgeId2);
        assertEquals("Should get expected purge2 status", state2.getPurgeStatus(), purgeState.getPurgeStatus());
        assertEquals("Should get expected purged ids", 0, state2.getPurged().size());
        assertEquals("Should get expected notPurged ids", 1, state2.getNotPurged().size());
        assertEquals("Should get expected suggestedPollPeriod", 10000, state2.getSuggestedPollPeriod());
    }

    @Test
    public void testDelete() throws Exception {
        System.out.println("******** STARTING DELETE TEST");
        populateTestData();

        String uri1 = "DEV://chirp/tag:search:475858716546592768";
        String uri2 = "DEV://chirp/tag:search:475858716546596474";

        Set<String> uris = Sets.newHashSet();
        uris.add(uri1);
        uris.add(uri2);

        String queryString = QueryBuilders.matchQuery("title", "ssr_title").toString();
        Query query = new Query().setSearchString(queryString).setPage(new Page().setPageSize((short) 5).setOffset(0));
        SearchResult results = ssrService.search(query, securityToken);
        assertEquals("Should have 2 matching titles before delete", 2, results.getTotalHits());

        ssrService.bulkDelete(uris, securityToken);

        results = ssrService.search(query, securityToken);
        assertEquals("Should have no title matches after delete", 0, results.getTotalHits());

        System.out.println("******** WE'RE DONE");

    }

    @Test
    public void testBeginPurge_NoIds() throws TException {
        long purgeId = System.currentTimeMillis();
        try {
            ssrService.beginPurge("purgeCallbackService", purgeId, Sets.<Long>newHashSet(), securityToken);

            PurgeState purgeState = ssrService.purgeStatus(securityToken, purgeId);

            assertEquals("Should get expected purge FINISHED_COMPLETE status",
                    PurgeStatus.FINISHED_COMPLETE, purgeState.getPurgeStatus());

            ssrService.beginPurge(
                    "purgeCallbackService", purgeId, null, securityToken);

            purgeState = ssrService.purgeStatus(securityToken, purgeId);

            assertEquals("Should get expected purge FINISHED_COMPLETE status",
                    PurgeStatus.FINISHED_COMPLETE, purgeState.getPurgeStatus());
        } catch (TException te) {
            fail("Should not have run into exception");
        }
    }

    @Test
    public void testBeginPurge_BadSecurityToken() throws TException {
        EzSecurityToken badToken = ThriftTestUtils.generateTestSecurityToken(
                "xyz", "xyz", Arrays.asList("U"));
        try {
            ssrService.beginPurge("purgeCallbackService", System.currentTimeMillis(), Sets.<Long>newHashSet(), badToken);
            fail("Should have run into an exception due to bad token");
        } catch (TException te) {
        }
    }

    @Test
    public void testPercolate() throws Exception {

        populateTestData();

        final PercolateQuery percolator = new PercolateQuery();
        final FilterBuilder correctTypeFilter =FilterBuilders.typeFilter("DEV:chirp");
        final QueryBuilder filteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), correctTypeFilter);
        final Visibility visibility = new Visibility().setFormalVisibility("U");
        percolator.setVisibility(visibility);
        final String queryDoc2 = jsonBuilder().startObject().field("query", filteredQuery).endObject().string();
        percolator.setQueryDocument(queryDoc2);

        // Action: Create and execute an OR match query on title
        IndexResponse percolateResponse = ssrService.putPercolateQuery("first", percolator, securityToken);

        Map<String, PercolatorInboxPeek> peekMainInboxResponse = ssrService.peekMainPercolatorInbox(securityToken);
        assertEquals(1, peekMainInboxResponse.size());
        PercolatorInboxPeek percolatorInboxPeek = peekMainInboxResponse.get(percolateResponse.get_id());
        assertEquals(0, percolatorInboxPeek.getCountOfHits());
        assertEquals("first", percolatorInboxPeek.getName());
        assertEquals(queryDoc2, percolatorInboxPeek.getSearchText());

        Thread.sleep(1000);

        populateTestData();

        HashMap<SSR, String> ssrJsonMap = Maps.newHashMap();
        URL url1 = Resources.getResource("source1.json");
        String source1 = Resources.toString(url1, Charsets.UTF_8);
        URL url2 = Resources.getResource("source2.json");
        String source2 = Resources.toString(url2, Charsets.UTF_8);

        SSR ssr1 = new SSR();
        ssr1.setUri("DEV://chirp/tag:search:475858716546592768");
        ssr1.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr1.setSnippet("some_snippet");
        ssr1.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr1.setTitle("ssr_title");
        ssr1.setTimeOfIngest(getCurrentThriftDateTime());

        EnterpriseMetaData metaData = new EnterpriseMetaData();
        Map<String, String> tags = new HashMap<>();
        tags.put("usp", "true");
        tags.put("exempt_code", "22");
        metaData.setTags(tags);
        ssr1.setMetaData(metaData);

        ssrJsonMap.put(ssr1, source1);
        ssrService.putWithDocs(ssrJsonMap, securityToken);

        PercolatorHitInbox inbox = ssrService.getAndFlushPercolatorInbox(percolateResponse.get_id(), securityToken);
        assertEquals(2, inbox.getHits().getMatchingRecordsSize());

        percolator.setId(percolateResponse.get_id() + "two");

        final IndexResponse percolateResponse2 = ssrService.putPercolateQuery("second", percolator, securityToken);

        SSR ssr2 = new SSR();
        ssr2.setUri("DEV://chirp/tag:search:475858716546596474");
        ssr2.setVisibility(new Visibility().setFormalVisibility("U"));
        ssr2.setSnippet("some_snippet");
        ssr2.setResultDate(new DateTime(new Date((short) 10, (short) 5, (short) 1999)));
        ssr2.setTitle("ssr_title");
        DateTime dateTime = getCurrentThriftDateTime();
        dateTime.setTime(dateTime.getTime().setMinute((short) 0));
        ssr2.setTimeOfIngest(dateTime);

        EnterpriseMetaData metaData2 = new EnterpriseMetaData();
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("usp", "true");
        tags2.put("exempt_code", "14");
        metaData2.setTags(tags2);
        ssr2.setMetaData(metaData2);

        ssrJsonMap.put(ssr2, source2);
        ssrService.putWithDocs(ssrJsonMap, securityToken);

        PercolatorHitInbox inbox2 = ssrService.getAndFlushPercolatorInbox(percolateResponse2.get_id(), securityToken);

        assertEquals(2, inbox2.getHits().getMatchingRecordsSize());
        final FilterBuilder wrongTypeFilter = FilterBuilders.typeFilter("wrong");
        final QueryBuilder wrongFilteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), wrongTypeFilter);
        final String queryDocWrong = jsonBuilder().startObject().field("query", wrongFilteredQuery).endObject().string();
        percolator.setQueryDocument(queryDocWrong);
        percolator.setId(percolateResponse.get_id() + "two");

        final IndexResponse percolateResponse3 = ssrService.putPercolateQuery("third", percolator, securityToken);

        ssrService.putWithDocs(ssrJsonMap, securityToken);

        PercolatorInboxPeek inbox3 = ssrService.peekPercolatorInbox(percolateResponse.get_id(), securityToken);
        assertEquals(2, inbox3.getCountOfHits());

        System.out.println("responseId:" + percolateResponse.get_id() + " " + percolateResponse.get_version() + " " + percolateResponse.get_type());

        Query percQuery = new Query();
        percQuery.setSearchString(QueryBuilders.matchAllQuery().toString());
        percQuery.setType(".percolator");

        final SearchResult resultsQuery = ssrService.queryPercolators(percQuery, securityToken);
        assertEquals(2, resultsQuery.getMatchingDocumentsSize());


        List<PercolateQuery> results = ssrService.percolate(ssrJsonMap, securityToken);
        assertEquals("Should have 2 matching titles before update", 2, results.size());

        String queryJson = QueryBuilders.idsQuery(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD).addIds(percolateResponse3.get_id() + "_inbox").toString();
        Query query = new Query(queryJson);
        final SearchResult docResults = ssrService.search(query, securityToken);

        assertEquals(1, docResults.getMatchingDocumentsSize());
        JSONObject inboxHits = new JSONObject(docResults.getMatchingDocuments().get(0).get_jsonObject());
        JSONArray dochits = (JSONArray) inboxHits.get(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS);
        assertEquals(0, dochits.length());

        queryJson = QueryBuilders.idsQuery(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD).addIds(percolateResponse.get_id() + "_inbox").toString();
        query = new Query(queryJson);
        final SearchResult docResults2 = ssrService.search(query, securityToken);

        assertEquals(1, docResults2.getMatchingDocumentsSize());
        JSONObject inboxHits2 = new JSONObject(docResults2.getMatchingDocuments().get(0).get_jsonObject());
        JSONArray dochits2 = (JSONArray) inboxHits2.get(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS);
        assertEquals(2, dochits2.length());

        queryJson = QueryBuilders.matchAllQuery().toString();
        query.setSearchString(queryJson);
        query.setType(SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD);
        final SearchResult docResultsMain = ssrService.search(query, securityToken);
        assertEquals(1, docResultsMain.getMatchingDocumentsSize());
        JSONObject mainInboxHits = new JSONObject(docResultsMain.getMatchingDocuments().get(0).get_jsonObject());
        JSONArray mainPercIds = (JSONArray) mainInboxHits.get(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS);
        assertEquals(2, mainPercIds.length());

        final String queryDoc = jsonBuilder().startObject().field("query", QueryBuilders.matchAllQuery()).endObject().string();

        final SearchResult resultsQuery2 = ssrService.queryPercolators(percQuery, securityToken);
        System.out.println("resultsQuery2: " + resultsQuery2.getMatchingDocumentsSize() + " " + resultsQuery2.getMatchingDocuments().get(0).toString());

        boolean deleteTrue = ssrService.deletePercolateQuery(percolateResponse.get_id(), securityToken);
        assertEquals(true, deleteTrue);

        final SearchResult resultsQuery3 = ssrService.queryPercolators(percQuery, securityToken);
        System.out.println("resultsQuery: " + resultsQuery3.getMatchingDocumentsSize());//+ " " + resultsQuery3.getMatchingDocuments().get(0).toString());
        assertEquals(1, resultsQuery3.getMatchingDocumentsSize());

        List<PercolateQuery> results3 = ssrService.percolate(ssrJsonMap, securityToken);
        assertEquals("Should have 0 matching titles after update", 0, results3.size());

        Map<String, PercolatorInboxPeek> peekMainInboxResponse2 = ssrService.peekMainPercolatorInbox(securityToken);
        assertEquals(1, peekMainInboxResponse2.size());
        PercolatorInboxPeek percolatorInboxPeek2 = peekMainInboxResponse2.get(percolateResponse2.get_id());
        assertEquals(0, percolatorInboxPeek2.getCountOfHits());
        assertEquals("third", percolatorInboxPeek2.getName());
        assertEquals(queryDocWrong, percolatorInboxPeek2.getSearchText());

        // test update percolate inbox
        // create query first
        PercolateQuery query4 = new PercolateQuery();
        query4.setVisibility(visibility);
        query4.setQueryDocument(queryDoc2);
        final IndexResponse inbox4 = ssrService.putPercolateQuery("origin name", query4, securityToken);

        // ingest data
        ssrService.putWithDocs(ssrJsonMap, securityToken);

        // update name
        final String newName = "name updated";
        IndexResponse updateResponse = ssrService.updatePercolateInbox(newName, inbox4.get_id(), securityToken);
        assertTrue(updateResponse.isSuccess());

        PercolatorInboxPeek inboxPeek4 = ssrService.peekPercolatorInbox(inbox4.get_id(), securityToken);
        assertEquals(newName, inboxPeek4.getName());
        assertEquals("inbox hits should remain after update", 2, inboxPeek4.getCountOfHits());
    }
}
