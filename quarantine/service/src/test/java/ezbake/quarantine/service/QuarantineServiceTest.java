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

package ezbake.quarantine.service;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.test.TestUtils;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.quarantine.service.util.ElasticsearchUtility;
import ezbake.quarantine.service.util.IDGenerationUtility;
import ezbake.quarantine.thrift.*;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;
import org.apache.accumulo.core.client.*;
import org.apache.thrift.TException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.javatuples.Pair;
import org.junit.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class QuarantineServiceTest {
    private static QuarantineService service;
    private static final String SYSTEM_HIGH_VISIBILITY = "A&XYZ&123"; // U FOUO USA S=P UNFIN
    private static final String APP_NAME = "test";
    private static final String ALT_APP_NAME = "other_app";
    private static Node node;

    private static EzSecurityTokenWrapper fouoToken = new EzSecurityTokenWrapper(TestUtils.createTestToken("mockAppSecId", Sets.newHashSet("A", "123", "XYZ"), "mockAppSecId"));
    private static EzSecurityTokenWrapper secretToken = new EzSecurityTokenWrapper(TestUtils.createTestToken("mockAppSecId", Sets.newHashSet("A", "123", "XYZ", "P"), "mockAppSecId"));
    private static EzSecurityTokenWrapper otherIdToken = new EzSecurityTokenWrapper(TestUtils.createTestToken("mockAppSecId", Sets.newHashSet("A", "123", "XYZ"), "other"));
    private static EzSecurityTokenWrapper communitiesToken;

    private static ThriftClientPool mockPool;
    private static Properties props;
    private static final String QUARANTINE_SECURITY_ID = "mockAppSecId";

    @BeforeClass
    public static void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, NamespaceExistsException, InterruptedException, TException {
        communitiesToken = new EzSecurityTokenWrapper(secretToken);
        communitiesToken.getAuthorizations().setExternalCommunityAuthorizations(Sets.newHashSet("TEST")).setPlatformObjectAuthorizations(Sets.newHashSet(1l));
        fouoToken.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1l));
        secretToken.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1l));
        otherIdToken.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1l));
        props = new Properties();
        props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, QUARANTINE_SECURITY_ID);
        props.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true");
        props.setProperty(EzBakeSecurityClientConfigurationHelper.MOCK_TARGET_ID_KEY, QUARANTINE_SECURITY_ID);
        props.setProperty(QuarantineService.SYSTEM_VISIBILITY_PROP, SYSTEM_HIGH_VISIBILITY);
        props.setProperty(EzBakePropertyConstants.ELASTICSEARCH_CLUSTER_NAME, "test");
        props.setProperty(EzBakePropertyConstants.ELASTICSEARCH_FORCE_REFRESH_ON_PUT, "true");
        props.setProperty(EzBakePropertyConstants.ELASTICSEARCH_HOST, "localhost");
        props.setProperty(EzBakePropertyConstants.ELASTICSEARCH_PORT, "9393");

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

        Set<Long> groupsMask = Sets.newHashSet(1l);
        String groupName = EzGroupsConstants.APP_GROUP + EzGroupsConstants.GROUP_NAME_SEP + APP_NAME;
        String otherGroupName = EzGroupsConstants.APP_GROUP + EzGroupsConstants.GROUP_NAME_SEP + ALT_APP_NAME;
        Set<String> appSet = Sets.newHashSet(groupName);
        Set<String> otherAppSet = Sets.newHashSet(otherGroupName);
        EzGroups.Client mockClient = mock(EzGroups.Client.class);
        when(mockClient.getGroupsMask(fouoToken, appSet)).thenReturn(groupsMask);
        when(mockClient.getGroupsMask(communitiesToken, appSet)).thenReturn(groupsMask);
        when(mockClient.getGroupsMask(secretToken, appSet)).thenReturn(groupsMask);
        when(mockClient.getGroupsMask(otherIdToken, appSet)).thenReturn(groupsMask);
        when(mockClient.getGroupsMask(fouoToken, otherAppSet)).thenReturn(Sets.newHashSet(2l));
        mockPool = mock(ThriftClientPool.class);
        when(mockPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class)).thenReturn(mockClient);
        when(mockPool.getSecurityId(EzGroupsConstants.SERVICE_NAME)).thenReturn("groups");
        service = getService();
        Thread.sleep(3000);
    }

    @AfterClass
    public static void destroy() throws ExecutionException, InterruptedException {
        node.client().admin().indices().delete(new DeleteIndexRequest(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)).get();
        node.close();
    }

    @After
    public void cleanup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, InterruptedException, EzSecurityTokenException {
        if (node.client().admin().indices().prepareExists(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX).get().isExists()) {
            node.client().prepareDeleteByQuery(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        } else {
            service = getService();
        }
        Thread.sleep(3000);
    }

    @Test
    public void testSendToQuarantine() throws TableNotFoundException, TException, NoSuchAlgorithmException, IOException, ClassNotFoundException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<QuarantineResult> resultList = service.getQuarantinedObjects(Lists.newArrayList(IDGenerationUtility.getId(qo)), fouoToken);

        assertEquals("Got one result back", 1, resultList.size());
        QuarantineResult result = resultList.get(0);

        // Check the returned object
        assertEquals("Correct pipeline ID", pipelineId, result.getObject().getPipelineId());
        assertEquals("Correct status", ObjectStatus.QUARANTINED, result.getStatus());

        // Check the hashed key
        String id = IDGenerationUtility.getId(qo);
        assertEquals("Correct ID", id, result.getId());
    }

    @Test
    public void testSendToQuarantine_additionalMetadata() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        AdditionalMetadata addlMetadata = new AdditionalMetadata();
        Exception e = new RuntimeException();
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        e.printStackTrace(writer);
        String stacktrace = sw.toString();
        addlMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(stacktrace));
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, addlMetadata, fouoToken);

        // Verify that the QuarantinedEvents list returned contains the metadata
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);

        assertEquals("Only one quarantine result", 1, results.size());
        QuarantineResult result = results.get(0);

        assertEquals("Only one event", 1, result.getEventsSize());
        QuarantineEvent event = result.getEvents().get(0);
        assertEquals("One retrieved additional metadata mapping", 1, event.getAdditionalMetadata().getEntriesSize());
        Map<String, MetadataEntry> entries = event.getAdditionalMetadata().getEntries();
        assertTrue("Additional metadata contains stacktrace key", entries.containsKey("stacktrace"));
        assertEquals("Stacktrace value is correct", stacktrace, entries.get("stacktrace").getValue());
        assertEquals("Metadata entry has the correct (system high) visibility", SYSTEM_HIGH_VISIBILITY, entries.get("stacktrace").getVisibility().getFormalVisibility());
    }

    @Test
    public void testSendToQuarantine_additionalMetadataHighClassification() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        AdditionalMetadata addlMetadata = new AdditionalMetadata();
        Exception e = new RuntimeException();
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        e.printStackTrace(writer);
        String stacktrace = sw.toString();
        addlMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(stacktrace));
        addlMetadata.putToEntries("some classified stuff", new MetadataEntry().setValue("SECRET STUFF").setVisibility(new Visibility().setFormalVisibility("P")));
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, addlMetadata, fouoToken);

        // Verify that the QuarantinedEvents list returned contains the metadata
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);

        assertEquals("Only one quarantine result", 1, results.size());
        QuarantineResult result = results.get(0);

        assertEquals("Only one event", 1, result.getEventsSize());
        QuarantineEvent event = result.getEvents().get(0);

        // Verify that we cannot see the additional metadata
        assertEquals("One retrieved additional metadata mapping", 1, event.getAdditionalMetadata().getEntriesSize());
        Map<String, MetadataEntry> entries = event.getAdditionalMetadata().getEntries();
        assertTrue("Additional metadata contains stacktrace key", entries.containsKey("stacktrace"));
        assertEquals("Stacktrace value is correct", stacktrace, entries.get("stacktrace").getValue());
        assertEquals("Metadata entry has the correct (system high) visibility", SYSTEM_HIGH_VISIBILITY, entries.get("stacktrace").getVisibility().getFormalVisibility());

        // Try again with more auths
        results = service.getQuarantinedObjects(Lists.newArrayList(id), secretToken);

        assertEquals("Only one quarantine result", 1, results.size());
        result = results.get(0);

        assertEquals("Only one event", 1, result.getEventsSize());
        event = result.getEvents().get(0);

        // Verify that we can see the additional metadata
        assertEquals("One retrieved additional metadata mapping", 2, event.getAdditionalMetadata().getEntriesSize());
    }

    @Test
    public void testSendToQuarantine_additionalMetadataHighClassificationWithCommunities() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        AdditionalMetadata addlMetadata = new AdditionalMetadata();
        Exception e = new RuntimeException();
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        e.printStackTrace(writer);
        String stacktrace = sw.toString();
        addlMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(stacktrace));
        addlMetadata.putToEntries("some classified stuff", new MetadataEntry().setValue("SECRET STUFF").setVisibility(new Visibility().setFormalVisibility("P").setAdvancedMarkings(new AdvancedMarkings().setExternalCommunityVisibility("TEST"))));
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, addlMetadata, fouoToken);

        // Verify that the QuarantinedEvents list returned contains the metadata
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);

        assertEquals("Only one quarantine result", 1, results.size());
        QuarantineResult result = results.get(0);

        assertEquals("Only one event", 1, result.getEventsSize());
        QuarantineEvent event = result.getEvents().get(0);

        // Verify that we cannot see the additional metadata
        assertEquals("One retrieved additional metadata mapping", 1, event.getAdditionalMetadata().getEntriesSize());
        Map<String, MetadataEntry> entries = event.getAdditionalMetadata().getEntries();
        assertTrue("Additional metadata contains stacktrace key", entries.containsKey("stacktrace"));
        assertEquals("Stacktrace value is correct", stacktrace, entries.get("stacktrace").getValue());
        assertEquals("Metadata entry has the correct (system high) visibility", SYSTEM_HIGH_VISIBILITY, entries.get("stacktrace").getVisibility().getFormalVisibility());

        // Try again with more auths
        results = service.getQuarantinedObjects(Lists.newArrayList(id), communitiesToken);

        assertEquals("Only one quarantine result", 1, results.size());
        result = results.get(0);

        assertEquals("Only one event", 1, result.getEventsSize());
        event = result.getEvents().get(0);

        // Verify that we can see the additional metadata
        assertEquals("One retrieved additional metadata mapping", 2, event.getAdditionalMetadata().getEntriesSize());
    }

    @Test
    public void testSendToQuarantine_additionalMetadataMultipleEvents() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        AdditionalMetadata addlMetadata = new AdditionalMetadata();
        Exception e = new RuntimeException();
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        e.printStackTrace(writer);
        String stacktrace = sw.toString();
        addlMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(stacktrace));
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, addlMetadata, fouoToken);

        addlMetadata = new AdditionalMetadata();
        addlMetadata.putToEntries("anotherKey", new MetadataEntry().setValue("anotherValue"));
        service.sendToQuarantine(qo, error, addlMetadata, fouoToken);

        // Verify that the QuarantinedEvents list returned contains the metadata
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);

        assertEquals("Only one quarantine result", 1, results.size());
        QuarantineResult result = results.get(0);

        assertEquals("Two events returned for this object", 2, result.getEventsSize());
        boolean found1 = false, found2 = false;
        for (QuarantineEvent event : result.getEvents()) {
            assertEquals("Each event only has one additional metadata object", 1, event.getAdditionalMetadata().getEntriesSize());
            Map<String, MetadataEntry> entry = event.getAdditionalMetadata().getEntries();
            if (entry.containsKey("stacktrace")) {
                assertEquals("Stack trace has correct content", stacktrace, entry.get("stacktrace").getValue());
                found1 = true;
            } else if (entry.containsKey("anotherKey")) {
                assertEquals("Additional value has correct content", "anotherValue", entry.get("anotherKey").getValue());
                found2 = true;
            }
        }
        assertTrue("Found the first object", found1);
        assertTrue("Found the second object", found2);
    }

    @Test
    public void testGetObjectsForPipeline() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        // Send data for another pipeline that we shouldn't receive later
        error = "SHOULDN'T GET THIS BACK!";
        object = "here is content".getBytes();
        qo = createObject("another_pipeline", "pipe3", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);

        assertEquals("Correct number of objects retrieved", 2, results.size());
    }

    @Test
    public void testGetObjectsForPipeline_CannotRetrieveWithLowerAuths() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "Q&R&W");
        service.sendToQuarantine(qo, error, null, fouoToken);
        String id = IDGenerationUtility.getId(qo);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);

        assertEquals("Correct number of objects retrieved", 2, results.size());
        assertTrue("Retrieved the ID of the object that we don't have auths for", results.contains(id));

        // Attempt to retrieve the first object, but fail because we don't have the correct auths
        try {
            service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
            assertTrue("Should not have received the object from quarantine", false);
        } catch (ObjectNotQuarantinedException e) {
            assertTrue("Correctly threw object not quarantined exception", true);
        }
    }

    @Test
    public void testGetPipeObjectCount() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "ANOTHER ONE!";
        object = "here is content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        String pipelineId1 = "ingest_pipeline_1";
        error = "ANOTHER ONE!";
        object = "here is content".getBytes();
        qo = createObject(pipelineId1, "pipe3", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "MORE ERRORS?!!";
        object = "here is content1".getBytes();
        qo = createObject(pipelineId1, "pipe3", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        Set<EventWithCount> results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be two different pipes with events", 2, results.size());
        boolean found1 = false, found2 = false;
        for (EventWithCount event : results) {
            if (event.getEvent().getPipeId().equals("pipe1")) {
                found1 = 1 == event.getCount();
            } else if (event.getEvent().getPipeId().equals("pipe2")) {
                found2 = 2 == event.getCount();
            } else {
                assertTrue(false);
            }
        }
        assertTrue("Found the correct counts for pipes 1 and 2", found1 && found2);

        results = service.getObjectCountPerPipe(pipelineId1, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Only one pipe to report", 1, results.size());
        EventWithCount event = results.iterator().next();
        assertEquals("Correct pipeline name", "pipe3", event.getEvent().getPipeId());
        assertEquals("Correct count of 2", 2, event.getCount());
        assertEquals("Latest event text is correct", "MORE ERRORS?!!", event.getEvent().getEvent());
    }

    @Test
    public void testGetPipeObjectCount_SameObjectTwice() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "ANOTHER ONE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        Set<EventWithCount> results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be two different pipes with events", 2, results.size());
        boolean found1 = false, found2 = false;
        for (EventWithCount event : results) {
            if (event.getEvent().getPipeId().equals("pipe1")) {
                found1 = 1 == event.getCount();
            } else if (event.getEvent().getPipeId().equals("pipe2")) {
                found2 = 1 == event.getCount();
            } else {
                assertTrue(false);
            }
        }
        assertTrue("Found the correct counts for pipes 1 and 2", found1 && found2);

        EventWithCount event = service.getLatestEventForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be the most recent event text", error, event.getEvent().getEvent());

        QuarantineEvent pipeEvent = service.getLatestEventForPipe(pipelineId, "pipe2", Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Latest event for pipe should equal latest event for pipeline", event.getEvent(), pipeEvent);
    }

    @Test
    public void testGetPipeObjectCount_SpecificStatus() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);
        service.updateStatus(Lists.newArrayList(IDGenerationUtility.getId(qo)), ObjectStatus.APPROVED_FOR_REINGEST, "test update", fouoToken);

        error = "ANOTHER ONE!";
        object = "here is content".getBytes();
        qo = createObject(pipelineId, "pipe2", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        String pipelineId1 = "ingest_pipeline_1";
        error = "ANOTHER ONE!";
        object = "here is content".getBytes();
        qo = createObject(pipelineId1, "pipe3", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "MORE ERRORS?!!";
        object = "here is content1".getBytes();
        qo = createObject(pipelineId1, "pipe3", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        Set<EventWithCount> results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.APPROVED_FOR_REINGEST), fouoToken);
        assertEquals("Should be two different pipes with events", 2, results.size());
        boolean found1 = false, found2 = false;
        for (EventWithCount event : results) {
            if (event.getEvent().getPipeId().equals("pipe1")) {
                found1 = 1 == event.getCount();
            } else if (event.getEvent().getPipeId().equals("pipe2")) {
                found2 = 2 == event.getCount();
            } else {
                assertTrue(false);
            }
        }
        assertTrue("Found the correct counts for pipes 1 and 2", found1 && found2);

        results = service.getObjectCountPerPipe(pipelineId1, Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.APPROVED_FOR_REINGEST), fouoToken);
        assertEquals("Only one pipe to report", 1, results.size());
        EventWithCount event = results.iterator().next();
        assertEquals("Correct pipeline name", "pipe3", event.getEvent().getPipeId());
        assertEquals("Correct count of 2", 2, event.getCount());
        assertEquals("Latest event text is correct", "MORE ERRORS?!!", event.getEvent().getEvent());

        // Check number of objects for specific status
        results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST), fouoToken);
        assertEquals("Only one pipe to report", 1, results.size());
        event = results.iterator().next();
        assertEquals("Correct pipeline name", "pipe2", event.getEvent().getPipeId());
        assertEquals("Correct count of 1", 1, event.getCount());

        results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Two pipes with quarantined objects", 2, results.size());
        found1 = false;
        found2 = false;
        for (EventWithCount eventWithCount : results) {
            if (eventWithCount.getEvent().getPipeId().equals("pipe1")) {
                found1 = 1 == eventWithCount.getCount();
            } else if (eventWithCount.getEvent().getPipeId().equals("pipe2")) {
                found2 = 1 == eventWithCount.getCount();
            } else {
                assertTrue(false);
            }
        }
        assertTrue("Found the correct counts for pipes 1 and 2", found1 && found2);
    }

    @Test
    public void testGetEventCount() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException, InvalidUpdateException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        object = "here is more content".getBytes();
        qo = createObject(pipelineId, "pipe1", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        Set<EventWithCount> results = service.getEventCountPerPipe(pipelineId, "pipe1", Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Only one event", 1, results.size());
        assertEquals("Correct event count before status update", 2, results.iterator().next().getCount());

        results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Only one event", 1, results.size());
        assertEquals("Correct object count for pipe1", 2, results.iterator().next().getCount());

        service.updateStatus(Lists.newArrayList(IDGenerationUtility.getId(qo)), ObjectStatus.APPROVED_FOR_REINGEST, "test update", fouoToken);
        results = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Only one event with status of QUARANTINED", 1, results.size());
        EventWithCount event = results.iterator().next();
        assertEquals("Correct pipe name of pipe1", "pipe1", event.getEvent().getPipeId());
        assertEquals("Correct count of 1", 1, event.getCount());
        assertEquals("Latest event text is correct", error, event.getEvent().getEvent());

        results = service.getEventCountPerPipe(pipelineId, "pipe1", Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Two different events", 2, results.size());
        boolean found1 = false, found2 = false;
        for (EventWithCount eventWithCount : results) {
            if (eventWithCount.getEvent().getEvent().equals(error)) {
                found1 = 1 == eventWithCount.getCount();
            } else if (eventWithCount.getEvent().getEvent().equals("test update")) {
                found2 = 1 == eventWithCount.getCount();
            } else {
                assertTrue(false);
            }
        }
        assertTrue("Found the correct counts for pipes 1 and 2", found1 && found2);
    }

    @Test
    public void testGetQuarantinedObject() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");

        service.sendToQuarantine(qo, error, null, fouoToken);

        Thread.sleep(1000);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Correct number of objects retrieved", 1, results.size());

        List<QuarantineResult> result = service.getQuarantinedObjects(Lists.newArrayList(results.get(0)), fouoToken);

        assertNotNull("Correct number of results returned", result);
        assertEquals("Classification is correct", result.get(0).getObject().getVisibility(), new Visibility().setFormalVisibility("A"));
        assertArrayEquals("object is correct", result.get(0).getObject().getContent(), "here is the content".getBytes());
        assertEquals("pipeline is correct", result.get(0).getObject().getPipelineId(), "ingest_pipeline");
        assertEquals("status is correct", result.get(0).getStatus(), ObjectStatus.QUARANTINED);
    }

    @Test
    public void testGetMultipleObjectsById() throws TableNotFoundException, TException, NoSuchAlgorithmException, UnsupportedEncodingException, ObjectNotQuarantinedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "pipe1", object, "A");
        String id1 = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "SOMETHING ELSE BROKE!";
        object = "here is more content".getBytes();

        qo = createObject(pipelineId, "pipe2", object, "A");
        qo.setVisibility(new Visibility().setFormalVisibility("A"));
        String id2 = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error, null, fouoToken);


        boolean found1 = false;
        boolean found2 = false;
        List<QuarantineResult> resultObjects = service.getQuarantinedObjects(Lists.newArrayList(id1, id2), fouoToken);
        for (QuarantineResult result : resultObjects) {
            QuarantinedObject resultObject = result.getObject();
            if (resultObject.getVisibility().equals(new Visibility().setFormalVisibility("A")) &&
                    resultObject.getPipelineId().equals("ingest_pipeline") &&
                    result.getEventsSize() > 0 &&
                    result.getEvents().get(0).getEvent().equals("SOMETHING BROKE!") &&
                    Arrays.equals(resultObject.getContent(), "here is the content".getBytes())) {
                found1 = true;
            } else if (resultObject.getVisibility().equals(new Visibility().setFormalVisibility("A")) &&
                    resultObject.getPipelineId().equals("ingest_pipeline") &&
                    result.getEventsSize() > 0 &&
                    result.getEvents().get(0).getEvent().equals("SOMETHING ELSE BROKE!") &&
                    Arrays.equals(resultObject.getContent(), "here is more content".getBytes())) {
                found2 = true;
            }
        }
        assertTrue("Found first object", found1);
        assertTrue("Found second object", found2);
    }

    @Test
    public void testDeleteQuarantinedObject() throws TException, ObjectNotQuarantinedException, TableNotFoundException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Correct number of objects retrieved", 2, results.size());

        String idToDelete = results.get(0);
        service.deleteFromQuarantine(Lists.newArrayList(idToDelete), fouoToken);

        try {
            service.getQuarantinedObjects(Lists.newArrayList(idToDelete), fouoToken);
            assertTrue("Should not have found objects by ID", false);
        } catch (ObjectNotQuarantinedException e) {
            assertTrue("Quarantine does not contain object", true);
        }

        IdsResponse forEvent = service.getObjectsForPipeAndEvent(pipelineId, "test_pipe", error, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, 10, fouoToken);
        assertEquals("Only one result for the error", 1, forEvent.getIdsSize());
        IdAndStatus eventResult = Iterables.getFirst(forEvent.getIds(), null);
        assertNotNull(eventResult);
        assertNotEquals("Not the ID we deleted", idToDelete, eventResult.getId());
        assertEquals("Has the correct status", ObjectStatus.QUARANTINED, eventResult.getStatus());

        results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Only one item left", 1, results.size());

        // Delete remaining item and check for event counts
        service.deleteFromQuarantine(Lists.newArrayList(results.get(0)), fouoToken);

        Set<EventWithCount> eventCount = service.getEventCountPerPipe(pipelineId, "test_pipe", Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertTrue("Event count is empty", eventCount.isEmpty());

        Set<EventWithCount> pipeCount = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertTrue("Object count is empty", pipeCount.isEmpty());
    }

    @Test
    public void testDeleteObjectsByEvent() throws TException, ObjectNotQuarantinedException, TableNotFoundException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Correct number of objects retrieved", 2, results.size());

        service.deleteObjectsByEvent(pipelineId, "test_pipe", ObjectStatus.QUARANTINED, error, fouoToken);

        try {
            results = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
            assertTrue("Should have caught the exception", false);
        } catch (ObjectNotQuarantinedException e) {
            assertTrue(true);
        }

        Set<EventWithCount> eventCount = service.getEventCountPerPipe(pipelineId, "test_pipe", Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertTrue("Event count is empty", eventCount.isEmpty());

        Set<EventWithCount> pipeCount = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertTrue("Object count is empty", pipeCount.isEmpty());
    }

    @Test
    public void testCannotRetrieveObjectFromAnotherSecurityId() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, otherIdToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, otherIdToken);
        assertEquals("Only received one object from quarantine", 1, results.size());
        assertEquals("Retrieved correct ID", id, results.get(0));
    }

    @Test
    public void testRetrievePipelinesForUser() throws TException, UnsupportedEncodingException, NoSuchAlgorithmException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject("first_pipeline", "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject("second_pipeline", "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject("third_pipeline", "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        qo.setApplicationName(ALT_APP_NAME);
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> pipelines = service.getPipelinesForUser(fouoToken);
        assertEquals("Three pipeline IDs returned", 3, pipelines.size());
        assertTrue("Contains first pipeline", pipelines.contains("first_pipeline"));
        assertTrue("Contains first pipeline", pipelines.contains("second_pipeline"));
        assertTrue("Contains first pipeline", pipelines.contains("third_pipeline"));
    }

    @Test
    public void testCannotRetrieveObjectFromAnotherAppGroup() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        qo.setApplicationName(ALT_APP_NAME);
        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Only received one object from quarantine", 1, results.size());
        assertEquals("Retrieved correct ID", id, results.get(0));
    }

    @Test
    public void testRetrieveAllObjectsWithSameSecurityIDAsQuarantine() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "some other error".getBytes(), "A");
        String id = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, otherIdToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Only received one object from quarantine", 2, results.size());
    }

    @Test
    public void testUpdateStatus() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, "test_pipe", "other".getBytes(), "A");

        service.sendToQuarantine(qo, "Another error", null, fouoToken);

        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
        assertEquals("Correct status", ObjectStatus.QUARANTINED, results.get(0).getStatus());

        // Update the entry
        service.updateStatus(Lists.newArrayList(id), ObjectStatus.APPROVED_FOR_REINGEST, "approving this item", fouoToken);

        List<String> ids = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.APPROVED_FOR_REINGEST), 0, (short) 5, fouoToken);
        assertEquals("Should contain both IDs", 2, ids.size());

        ids = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST), 0, (short) 5, fouoToken);
        assertEquals("Should only contain one ID", 1, ids.size());
        assertEquals("Proper ID returned", id, ids.get(0));

        results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
        assertEquals("Correct status", ObjectStatus.APPROVED_FOR_REINGEST, results.get(0).getStatus());
    }

    @Test
    public void testUpdateStatusPipelineSecurityId() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error, null, fouoToken);

        // Update the entry
        service.updateStatus(Lists.newArrayList(id), ObjectStatus.APPROVED_FOR_REINGEST, "approving this item", fouoToken);
    }

    @Test
    public void testUpdateStatusRetainsOriginalSecurityId() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException {
        String pipelineId = "ingest_pipeline";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error, null, otherIdToken);

        // Update the entry
        service.updateStatus(Lists.newArrayList(id), ObjectStatus.APPROVED_FOR_REINGEST, "approving this item", fouoToken);

        // Verify that we can still see the entry (still has a security ID of "other")
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), otherIdToken);
        assertEquals("Got one result back", 1, results.size());
    }

    @Test
    public void testSendMultipleTimes() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException {
        String pipelineId = "ingest_pipeline";
        String error1 = "SOMETHING BROKE!";
        String error2 = "Another error";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error1, null, fouoToken);
        service.sendToQuarantine(qo, error2, null, fouoToken);

        List<String> resultIds = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Only one result", 1, resultIds.size());

        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
        assertEquals("Contains two events", 2, results.get(0).getEventsSize());

        boolean found1 = false, found2 = false;
        for (QuarantineEvent event : results.get(0).getEvents()) {
            if (event.getEvent().equals(error1)) found1 = true;
            if (event.getEvent().equals(error2)) found2 = true;
        }
        assertTrue("Found both errors", found1 && found2);
    }

    @Test
    public void testUpdateThenGetCountWithSameTimestamps() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error1 = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id1 = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error1, null, fouoToken);

        object = "here is different content".getBytes();
        qo = createObject(pipelineId, pipeId, object, "A");
        String id2 = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error1, null, fouoToken);

        // Update them at the same time
        service.updateStatus(Lists.newArrayList(id1, id2), ObjectStatus.APPROVED_FOR_REINGEST, "approved these objects", fouoToken);

        Thread.sleep(50);

        // Update one and make sure that the counts are correct
        service.updateStatus(Lists.newArrayList(id1), ObjectStatus.ARCHIVED, "now it's rejected", fouoToken);

        Set<EventWithCount> results = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.QUARANTINED, ObjectStatus.ARCHIVED), fouoToken);
        assertEquals("There are two different events", 2, results.size());

        boolean found1 = false, found2 = false;
        for (EventWithCount event : results) {
            if (event.getStatus() == ObjectStatus.APPROVED_FOR_REINGEST && event.getEvent().getEvent().equals("approved these objects")) found1 = true;
            if (event.getStatus() == ObjectStatus.ARCHIVED && event.getEvent().getEvent().equals("now it's rejected")) found2 = true;
        }
        assertTrue("Found both errors", found1 && found2);
    }

    @Test
    public void testSendMultipleAfterUpdate() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException {
        String pipelineId = "ingest_pipeline";
        String error1 = "SOMETHING BROKE!";
        String error2 = "Another error";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, "A");
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error1, null, fouoToken);
        service.updateStatus(Lists.newArrayList(id), ObjectStatus.APPROVED_FOR_REINGEST, "approving this item", fouoToken);
        List<QuarantineResult> results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
        assertEquals("Status has been updated", ObjectStatus.APPROVED_FOR_REINGEST, results.get(0).getStatus());

        service.sendToQuarantine(qo, error2, null, fouoToken);
        results = service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
        assertEquals("Contains three events", 3, results.get(0).getEventsSize());
        assertEquals("Status updated back to QUARANTINED", ObjectStatus.QUARANTINED, results.get(0).getStatus());
    }

    @Test
    public void testQuarantine_CommunitiesAuths() throws TException, ObjectNotQuarantinedException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidUpdateException {
        String pipelineId = "ingest_pipeline";
        String error1 = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        Visibility visibility = new Visibility().setFormalVisibility("A").setAdvancedMarkings(new AdvancedMarkings().setExternalCommunityVisibility("TEST"));
        QuarantinedObject qo = createObject(pipelineId, "test_pipe", object, visibility);
        String id = IDGenerationUtility.getId(qo);

        service.sendToQuarantine(qo, error1, null, fouoToken);

        try {
            service.getQuarantinedObjects(Lists.newArrayList(id), fouoToken);
            assertTrue("Should have gotten an exception", false);
        } catch (ObjectNotQuarantinedException e) {
            assertTrue("Got the exception because we couldn't view the object", true);
        }

        List<QuarantineResult> result = service.getQuarantinedObjects(Lists.newArrayList(id), communitiesToken);
        assertEquals("One result returned", 1, result.size());
        assertEquals("Correct ID returned", id, result.get(0).getId());
        assertArrayEquals("Content is correct", object, result.get(0).getObject().getContent());
        assertEquals("Correct visibility", visibility, result.get(0).getObject().getVisibility());
    }

    @Test
    public void testPing_serviceIsHealthy() {
        assertTrue("Service should be healthy", service.ping());
    }

    @Test
    public void testPing_serviceIsDown() throws ExecutionException, InterruptedException {
        node.client().admin().indices().delete(new DeleteIndexRequest(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)).get();
        Thread.sleep(2000);
        assertFalse("Service should be down", service.ping());
    }

    @Test(expected = InvalidUpdateException.class)
    public void testInvalidUpdate() throws TException, InvalidUpdateException {
        service.updateStatus(Lists.newArrayList("whatever"), ObjectStatus.CANNOT_BE_REINGESTED, "whatever", fouoToken);
    }

    @Test
    public void testExportImport() throws TException, ObjectNotQuarantinedException, TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException, ExecutionException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "pipe";
        String error = "SOMETHING BROKE!";
        String key = "this is a password 12345";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");

        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Correct number of objects retrieved", 1, results.size());

        // Send the object to quarantine again with a different error to get another event for export
        service.sendToQuarantine(qo, error + " AGAIN!", null, fouoToken);

        ByteBuffer exported = service.exportData(results, key, fouoToken);

        // Get a fresh table
        node.client().admin().indices().delete(new DeleteIndexRequest(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)).get();
        Thread.sleep(3000);
        service = getService();
        Thread.sleep(3000);

        try {
            service.getQuarantinedObjects(results, fouoToken);
            assertTrue("Should have thrown ObjectNotQuarantinedException", false);
        } catch (ObjectNotQuarantinedException e) {
            assertTrue("No objects were in quarantine", true);
        }
        ImportResult importResult = service.importData(exported, key, fouoToken);
        assertEquals("Correct total records", 1, importResult.getTotalRecords());
        assertEquals("Correct duplicate records", 0, importResult.getDuplicateRecords());
        assertEquals("Correct imported records", 1, importResult.getRecordsImported());

        // Make sure we can query by pipeline ID
        List<String> pipelineQuarantineObjects = service.getObjectsForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Got one ID back", 1, pipelineQuarantineObjects.size());
        assertEquals("Got the correct ID back", results.get(0), pipelineQuarantineObjects.get(0));

        List<QuarantineResult> quarantineResults = service.getQuarantinedObjects(results, fouoToken);
        assertEquals("Got one result back", 1, quarantineResults.size());
        QuarantineResult result = quarantineResults.get(0);

        // Verify the content is the same
        assertEquals("Pipe ID is correct", pipeId, result.getObject().getPipeId());
        assertEquals("Pipeline ID is correct", pipelineId, result.getObject().getPipelineId());
        assertEquals("Classification is correct", "A", result.getObject().getVisibility().getFormalVisibility());
        assertEquals("Status is QUARANTINED", ObjectStatus.QUARANTINED, result.getStatus());
        assertArrayEquals("Object is the same", object, result.getObject().getContent());
        assertEquals("Two total events", 2, result.getEventsSize());

        // Loop through and look for both errors
        boolean found1 = false, found2 = false;
        for (QuarantineEvent event : result.getEvents()) {
            if (event.getEvent().equals(error)) {
                found1 = true;
            } else if (event.getEvent().equals(error + " AGAIN!")) {
                found2 = true;
            }
        }
        assertTrue("Found first error", found1);
        assertTrue("Found second error", found2);
    }

    @Test
    public void testImportWithDuplicates() throws TException, ObjectNotQuarantinedException, TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "pipe";
        String error = "SOMETHING BROKE!";
        String key = "this is a password 12345";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");

        service.sendToQuarantine(qo, error, null, fouoToken);

        List<String> results = service.getObjectsForPipeline("ingest_pipeline", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, (short) 5, fouoToken);
        assertEquals("Correct number of objects retrieved", 1, results.size());

        // Send the object to quarantine again with a different error to get another event for export
        service.sendToQuarantine(qo, error + " AGAIN!", null, fouoToken);

        ByteBuffer exported = service.exportData(results, key, fouoToken);
        ImportResult importResult = service.importData(exported, key, fouoToken);
        assertEquals("Correct total records", 1, importResult.getTotalRecords());
        assertEquals("Correct duplicate records", 1, importResult.getDuplicateRecords());
        assertEquals("Correct imported records", 0, importResult.getRecordsImported());
    }

    @Test
    public void testGetObjectByEvent() throws TException, ObjectNotQuarantinedException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        IdsResponse ids = service.getObjectsForPipeAndEvent(pipelineId, pipeId, "SOMETHING BROKE!", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, 10, fouoToken);
        IdAndStatus id = Iterables.getFirst(ids.getIds(), null);
        assertNotNull(id);
        String expected = IDGenerationUtility.getId(qo);
        assertEquals("Returned ID is correct", expected, id.getId());

        // Add more objects to test more queries
        service.updateStatus(Lists.newArrayList(IDGenerationUtility.getId(qo)), ObjectStatus.APPROVED_FOR_REINGEST, "approving", fouoToken);
        qo = createObject(pipelineId, pipeId, "other content".getBytes(), "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        error = "MORE STUFF BROKE";
        qo = createObject(pipelineId, pipeId + "1", "more content".getBytes(), "A");
        service.sendToQuarantine(qo, error, null, fouoToken);
        qo = createObject(pipelineId, pipeId + "1", "another piece of content".getBytes(), "A");
        service.sendToQuarantine(qo, error, null, fouoToken);

        ids = service.getObjectsForPipeAndEvent(pipelineId, pipeId, "approving", Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.APPROVED_FOR_REINGEST), 0, 10, fouoToken);
        assertEquals("One approved object for test_pipe", 1, ids.getIdsSize());
        ids = service.getObjectsForPipeAndEvent(pipelineId, pipeId, "SOMETHING BROKE!", Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.APPROVED_FOR_REINGEST), 0, 10, fouoToken);
        assertEquals("One object for test_pipe with error message", 1, ids.getIdsSize());
        ids = service.getObjectsForPipeAndEvent(pipelineId, pipeId + "1", "MORE STUFF BROKE", Sets.newHashSet(ObjectStatus.QUARANTINED), 0, 10, fouoToken);
        assertEquals("Two objects quarantined for test_pipe1", 2, ids.getIdsSize());
    }

    @Test(expected = InvalidUpdateException.class)
    public void testUpdateWith_CANNOT_BE_REINGESTED() throws TException, ObjectNotQuarantinedException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        qo.setSerializable(false);
        service.sendToQuarantine(qo, error, null, fouoToken);

        // Add more objects to test more queries
        service.updateStatus(Lists.newArrayList(IDGenerationUtility.getId(qo)), ObjectStatus.APPROVED_FOR_REINGEST, "approving", fouoToken);
    }

    @Test(expected = InvalidUpdateException.class)
    public void testUpdateEventWith_CANNOT_BE_REINGESTED() throws TException, ObjectNotQuarantinedException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        qo.setSerializable(false);
        service.sendToQuarantine(qo, error, null, fouoToken);

        // Add more objects to test more queries
        service.updateStatusOfEvent(pipelineId, pipeId, ObjectStatus.CANNOT_BE_REINGESTED, ObjectStatus.APPROVED_FOR_REINGEST, error, "some update", fouoToken);
    }

    @Test
    public void testChangeStatusOfObjectAndGetCounts() throws TException, ObjectNotQuarantinedException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidUpdateException, InterruptedException {
        String pipelineId = "ingest_pipeline";
        String pipeId = "test_pipe";
        String error = "SOMETHING BROKE!";
        byte[] object = "here is the content".getBytes();

        QuarantinedObject qo = createObject(pipelineId, pipeId, object, "A");
        String id1 = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, fouoToken);

        qo = createObject(pipelineId, pipeId, "other content".getBytes(), "A");
        String id2 = IDGenerationUtility.getId(qo);
        service.sendToQuarantine(qo, error, null, fouoToken);

        Set<EventWithCount> results = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be one event initially", 1, results.size());
        EventWithCount eventWithCount = Iterables.getFirst(results, null);
        assertNotNull(eventWithCount);
        assertEquals("Event should have a count of 2", 2, eventWithCount.getCount());

        // Add more objects to test more queries
        service.updateStatus(Lists.newArrayList(id2), ObjectStatus.APPROVED_FOR_REINGEST, "approving", fouoToken);

        results = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be two different events", 2, results.size());

        // Check to make sure that there is one quarantined event and one approved event. The quarantined event for the object that was approved should have been removed
        boolean found1 = false, found2 = false;
        for (EventWithCount event : results) {
            if (event.getEvent().getId().equals(id1)) {
                assertEquals("Correct event type", EventType.ERROR, event.getEvent().getType());
                assertEquals("Correct status", ObjectStatus.QUARANTINED, event.getStatus());
                assertEquals("Correct event", error, event.getEvent().getEvent());
                found1 = true;
            } else if (event.getEvent().getId().equals(id2)) {
                assertEquals("Correct event type", EventType.STATUS_UPDATE, event.getEvent().getType());
                assertEquals("Correct status", ObjectStatus.APPROVED_FOR_REINGEST, event.getStatus());
                assertEquals("Correct event", "approving", event.getEvent().getEvent());
                found2 = true;
            }
        }
        assertTrue("Found both events", found1 && found2);

        service.updateStatus(Lists.newArrayList(id1), ObjectStatus.APPROVED_FOR_REINGEST, "approving", fouoToken);

        results = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be one event only", 1, results.size());

        // Check to make sure that there is one approved event
        EventWithCount event = Iterables.getFirst(results, null);
        assertNotNull(event);
        assertEquals("Correct event type", EventType.STATUS_UPDATE, event.getEvent().getType());
        assertEquals("Correct status", ObjectStatus.APPROVED_FOR_REINGEST, event.getStatus());
        assertEquals("Correct event", "approving", event.getEvent().getEvent());
        assertEquals("Correct ID", id1, event.getEvent().getId());

        // Try to get event count for status of QUARANTINED. Should not exist
        results = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Should be no quarantined events", 0, results.size());
    }

//    @Test
    public void testAddingManyRecordsAndUpdatesAndDeletes() throws TException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidUpdateException, ObjectNotQuarantinedException, InterruptedException {
        final int totalErrors = 2000;
        List<String> pipelineIds = Lists.newArrayList("pipeline_1", "second_pipeline", "anotherPipeline", "FOURTHPIPELINE");
        List<String> pipeIds = Lists.newArrayList("warehouse-pipe", "analyzer-pipe", "geospatial-pipe", "crazy-pipe");
        List<String> errorMessages = Lists.newArrayList("OH MY GOD IT'S ON FIRE", "It's just a flesh wound", "There's a snake in my boots",
                                                            "DOH!", "Nuke it from orbit");
        List<String> updateMessages = Lists.newArrayList("Because I said so", "This is probably okay", "This is a terrible idea");
        List<Pair<QuarantinedObject, String>> quarantinedObjectsAndEvents = Lists.newArrayList();
        List<String> objectsToUpdate = Lists.newArrayList();
        List<String> objectsToDelete = Lists.newArrayList();
        Map<String, QuarantinedObject> idsToObjects = Maps.newHashMap();

        String pipelineId = null;
        String pipeId = null;
        String error = null;
        QuarantinedObject object = null;
        for (int i = 0; i < totalErrors; i++) {
            if (i == totalErrors - 1) {
                // if this is the last error sleep for a short period of time to ensure our verification steps
                Thread.sleep(5);
            }
            pipelineId = pipelineIds.get(new Random().nextInt(4));
            pipeId = pipeIds.get(new Random().nextInt(4));
            error = errorMessages.get(new Random().nextInt(5));
            object = createObject(pipelineId, pipeId, Integer.toString(i).getBytes(), "A");
            service.sendToQuarantine(object, error, null, fouoToken);
            quarantinedObjectsAndEvents.add(new Pair<>(object, error));
            int rand = new Random().nextInt(10);
            String id = IDGenerationUtility.getId(object);
            if (rand == 4) {
                objectsToUpdate.add(id);
            } else if (rand == 8) {
                objectsToDelete.add(id);
            }
            idsToObjects.put(id, object);
        }

        // Make sure the latest event is correct
        EventWithCount event = service.getLatestEventForPipeline(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        assertEquals("Event has correct pipe ID", pipeId, event.getEvent().getPipeId());
        assertEquals("Event has correct text", error, event.getEvent().getEvent());
        assertEquals("Event has the correct ID", IDGenerationUtility.getId(object), event.getEvent().getId());

        // Use the latest event to check events returned from other endpoints
        Set<EventWithCount> events = service.getObjectCountPerPipe(pipelineId, Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
        boolean foundEvent = false;
        for (EventWithCount eventWithCount : events) {
            if (eventWithCount.getEvent().getPipeId().equals(pipeId)) {
                foundEvent = eventWithCount.getEvent().getEvent().equals(error) &&
                        eventWithCount.getEvent().getId().equals(IDGenerationUtility.getId(object));
            }
        }
        assertTrue("Found most recent event in the events with counts", foundEvent);

        // Verify that the total from each pipe adds up to the total inserted
        int total = 0;
        for (int i = 0; i < 4; i++) {
            Set<EventWithCount> objectCount = service.getObjectCountPerPipe(pipelineIds.get(i), Sets.newHashSet(ObjectStatus.QUARANTINED), fouoToken);
            for (EventWithCount eventWithCount : objectCount) {
                total += eventWithCount.getCount();
            }
        }
        assertEquals("Inserted the correct amount of errors", totalErrors, total);

        // Make updates
        Map<String, Integer> updatesForPipelineAndPipe = Maps.newHashMap();
        for (String idToUpdate : objectsToUpdate) {
            String updateComment = updateMessages.get(new Random().nextInt(3));
            ObjectStatus status = null;
            while (status == null || status == ObjectStatus.CANNOT_BE_REINGESTED || status == ObjectStatus.QUARANTINED) {
                status = ObjectStatus.values()[new Random().nextInt(ObjectStatus.values().length)];
            }
            service.updateStatus(Lists.newArrayList(idToUpdate), status, updateComment, fouoToken);

            object = idsToObjects.get(idToUpdate);
            pipelineId = object.getPipelineId();
            pipeId = object.getPipeId();
            String key = pipelineId + "::" + pipeId + "::" + status.toString();

            Integer count = updatesForPipelineAndPipe.get(key);
            if (count == null) {
                count = 0;
            }
            updatesForPipelineAndPipe.put(key, count + 1);
        }

        // Check the update count for each pipeline/pipe/status combo
        for (Map.Entry<String, Integer> statusEntry : updatesForPipelineAndPipe.entrySet()) {
            int expected = statusEntry.getValue();
            String[] keySplit = statusEntry.getKey().split("::");
            pipelineId = keySplit[0];
            pipeId = keySplit[1];
            ObjectStatus status = ObjectStatus.valueOf(keySplit[2]);
            Set<EventWithCount> eventCounts = service.getEventCountPerPipe(pipelineId, pipeId, Sets.newHashSet(status), fouoToken);
            total = 0;
            for (EventWithCount eventWithCount : eventCounts) {
                total += eventWithCount.getCount();
            }
            assertEquals(String.format("Correct number of updates for pipeline %s, pipe %s, and status %s", pipelineId, pipeId, status.toString()), expected, total);
        }

        // Do a bulk update of all of the items and make sure that we get the proper count
        String updateComment = "abandon all hope";
        service.updateStatus(objectsToUpdate, ObjectStatus.ARCHIVED, updateComment, fouoToken);
        int totalUpdates = 0;
        int expected = objectsToUpdate.size();
        for (String plId : pipelineIds) {
            for (String pId : pipeIds) {
                try {
                    IdsResponse objects = service.getObjectsForPipeAndEvent(plId, pId, updateComment, Sets.newHashSet(ObjectStatus.APPROVED_FOR_REINGEST, ObjectStatus.ARCHIVED, ObjectStatus.QUARANTINED), 0, 10, fouoToken);
                    totalUpdates += objects.getIdsSize();
                    for (IdAndStatus id : objects.getIds()) {
                        objectsToUpdate.remove(id.getId());
                    }
                } catch (ObjectNotQuarantinedException e) {
                    // There's a small chance that some combination of the above doesn't exist, let's just ignore it
                    // since it isn't an error
                }
            }
        }
        assertEquals("Each object we found was unique", expected, totalUpdates);
        assertTrue("All objects have been located", objectsToUpdate.isEmpty());

        // Delete a bunch of IDs and re-count everything
        service.deleteFromQuarantine(objectsToDelete, fouoToken);
        total = 0;
        for (int i = 0; i < 4; i++) {
            Set<EventWithCount> objectCount = service.getObjectCountPerPipe(pipelineIds.get(i), Sets.newHashSet(ObjectStatus.QUARANTINED, ObjectStatus.ARCHIVED), fouoToken);
            for (EventWithCount eventWithCount : objectCount) {
                total += eventWithCount.getCount();
            }
        }
        assertEquals("Correct total of objects in quarantine after deletion", totalErrors - objectsToDelete.size(), total);
    }

    private QuarantinedObject createObject(String pipelineId, String pipeId, byte[] content, String visibility) {
        return createObject(pipelineId, pipeId, content, new Visibility().setFormalVisibility(visibility));
    }

    private QuarantinedObject createObject(String pipelineId, String pipeId, byte[] content, Visibility visibility) {
        QuarantinedObject qo = new QuarantinedObject();
        qo.setPipelineId(pipelineId);
        qo.setPipeId(pipeId);
        qo.setContent(content);
        qo.setVisibility(visibility);
        qo.setApplicationName(APP_NAME);
        return qo;
    }

    private static QuarantineService getService() throws EzSecurityTokenException {
        EzbakeSecurityClient client = mock(EzbakeSecurityClient.class);
        when(client.fetchDerivedTokenForApp(fouoToken, "groups")).thenReturn(fouoToken);
        when(client.fetchDerivedTokenForApp(secretToken, "groups")).thenReturn(secretToken);
        when(client.fetchDerivedTokenForApp(otherIdToken, "groups")).thenReturn(otherIdToken);
        when(client.fetchDerivedTokenForApp(communitiesToken, "groups")).thenReturn(communitiesToken);
        return new QuarantineService(props, new ElasticsearchUtility(SYSTEM_HIGH_VISIBILITY, QUARANTINE_SECURITY_ID, client, mockPool), client);
    }
}
