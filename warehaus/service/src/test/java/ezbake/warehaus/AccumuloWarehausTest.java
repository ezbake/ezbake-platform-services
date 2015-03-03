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

package ezbake.warehaus;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.CancelStatus;
import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.PlatformObjectVisibilities;
import ezbake.base.thrift.PurgeException;
import ezbake.base.thrift.PurgeState;
import ezbake.base.thrift.PurgeStatus;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.data.common.TimeUtil;
import ezbake.data.test.TestUtils;
import ezbake.security.serialize.VisibilitySerialization;
import ezbake.security.serialize.thrift.VisibilityWrapper;
import ezbake.thrift.ThriftUtils;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class AccumuloWarehausTest {
	private static Visibility visibility = new Visibility().setFormalVisibility("U");
	private static EzSecurityToken token = new EzSecurityToken();
	private Calendar calendar = Calendar.getInstance(java.util.TimeZone.getTimeZone("GMT+0:00"));
	private Properties config;
	private AccumuloWarehaus warehaus;
	private FileSystem hdfs;
	
	@Before
	public void beforeEach() throws Exception {
		warehaus = new AccumuloWarehaus();
		warehaus.setConfigurationProperties(config);
        warehaus.getThriftProcessor();
		calendar.set(Calendar.MILLISECOND,0);
	}

	@After
	public void afterEach() throws Exception {
		warehaus.resetTable();
	}
	
	public AccumuloWarehausTest() throws Exception {
	    config = new ClasspathConfigurationLoader().loadConfiguration();

        EzBakeApplicationConfigurationHelper helper = new EzBakeApplicationConfigurationHelper(config);
        token = TestUtils.createTestToken(String.valueOf(helper.getSecurityID()),
                                    Sets.newHashSet("U", "S", "C", "FOUO", "USA"),
                                    String.valueOf(helper.getSecurityID()));
		Configuration fsConfig = new Configuration();
		this.hdfs = FileSystem.get(fsConfig);
	}

	@Test
	public void replayTest() throws TException, InterruptedException, EntryNotInWarehausException {
		List<Repository> repositories = Lists.newLinkedList();
		Repository repository = new Repository();
		repository.setUri("this://testfeed/"+UUID.randomUUID().toString());
		repository.setRawData(UUID.randomUUID().toString().getBytes());
		repository.setParsedData(UUID.randomUUID().toString().getBytes());
		repositories.add(repository);
		warehaus.insert(repository, visibility, token);
        Thread.sleep(2000);
		repository = new Repository();
		repository.setUri("this://testfeed/"+UUID.randomUUID().toString());
		repository.setRawData(UUID.randomUUID().toString().getBytes());
		repository.setParsedData(UUID.randomUUID().toString().getBytes());
		repositories.add(repository);
		warehaus.insert(repository, visibility, token);
        Thread.sleep(2000);
		repository = new Repository();
		String uri = "this://testfeed/"+UUID.randomUUID().toString();
		repository.setUri(uri);
		repository.setRawData(UUID.randomUUID().toString().getBytes());
		repository.setParsedData(UUID.randomUUID().toString().getBytes());
		repositories.add(repository);
		warehaus.insert(repository, visibility, token);
        Thread.sleep(2000);
		List<DatedURI> result = warehaus.replay("this://testfeed", false, null, null, null, token);
		assertEquals(3,result.size());
		assertTrue(compare(result.get(0).getTimestamp(),result.get(1).getTimestamp()) < 0);
		assertTrue(compare(result.get(1).getTimestamp(),result.get(2).getTimestamp()) < 0);
		assertEquals(repositories.get(0).getUri(),result.get(0).getUri());
		assertEquals(repositories.get(1).getUri(),result.get(1).getUri());
		assertEquals(repositories.get(2).getUri(),result.get(2).getUri());
        BinaryReplay version = warehaus.getParsed(result.get(0).getUri(), TimeUtil.convertFromThriftDateTime(result.get(0).getTimestamp()), token);
        assertFalse(version == null);
        assertEquals(repositories.get(0).getUri(), version.getUri());
        assertArrayEquals(repositories.get(0).getParsedData(), version.getPacket());

        // Due to timestampFilter the seems to round up time, look past a sec 
        // so end time is at least a sec after the entry's update time.
        // Hopefully won't be an issue in real world since chances someone's 
        // replaying within a sec of update is not bound to happen.
        DateTime endTime = TimeUtil.convertToThriftDateTime(
                TimeUtil.convertFromThriftDateTime(result.get(1).getTimestamp()) + 1000);
		result = warehaus.replay("this://testfeed", false, null, endTime, null, token);
		assertEquals(2,result.size());
		assertTrue(compare(result.get(0).getTimestamp(),result.get(1).getTimestamp()) < 0);
		assertEquals(repositories.get(0).getUri(),result.get(0).getUri());
		assertEquals(repositories.get(1).getUri(),result.get(1).getUri());
		// Look a sec before the entry's update time, for the same reason 
		// noted above with the timestamp filter
		DateTime startTime = TimeUtil.convertToThriftDateTime(
                TimeUtil.convertFromThriftDateTime(result.get(1).getTimestamp()) - 1000);
		result = warehaus.replay("this://testfeed", false, startTime, null, null, token);
		assertEquals(2,result.size());
		assertTrue(compare(result.get(0).getTimestamp(),result.get(1).getTimestamp()) < 0);
		assertEquals(repositories.get(1).getUri(),result.get(0).getUri());
		assertEquals(repositories.get(2).getUri(),result.get(1).getUri());
		endTime = TimeUtil.convertToThriftDateTime(
                TimeUtil.convertFromThriftDateTime(result.get(0).getTimestamp()) + 1000);
		startTime = TimeUtil.convertToThriftDateTime(
                        TimeUtil.convertFromThriftDateTime(result.get(0).getTimestamp()) - 1000);
		result = warehaus.replay("this://testfeed", false, startTime, endTime, null, token);
		assertEquals(1,result.size());
		assertEquals(repositories.get(1).getUri(), result.get(0).getUri());
		
		try {
	        result = warehaus.replay(null, false, null, null, null, token);
	        fail("Replay should fail on null urn");
		} catch (TException ex) {
		    assertTrue("Replay should fail on null urn", true);
		}
		
        // test replay only latest
        repository = new Repository();
        repository.setUri(uri);
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(1000);
        List<DatedURI> latest = warehaus.replay("this://testfeed", false, null, null, null, token);
        assertEquals(4, latest.size());
        latest = warehaus.replay("this://testfeed", true, null, null, null, token);
        assertEquals(3, latest.size());
	}

    @Test
    public void replayTest_BulkInsert() throws TException, InterruptedException, EntryNotInWarehausException {
        PutRequest request = new PutRequest();
        String prefix = "test://replay";

        for (int i = 0; i < 10; i++) {
            PutUpdateEntry entry = new PutUpdateEntry();
            entry.setVisibility(visibility);
            UpdateEntry update = new UpdateEntry();
            String suffix = Integer.toString(i);
            update.setParsedData(suffix.getBytes());
            update.setUri(prefix + "/" + suffix);
            entry.setEntry(update);
            request.addToEntries(entry);
        }
        long timestamp = warehaus.put(request, token).getTimestamp();
        DateTime start = TimeUtil.convertToThriftDateTime(timestamp - 1000);
        DateTime end = TimeUtil.convertToThriftDateTime(timestamp + 1000);

        // Run replay and validate that we get all the URIs we expect
        List<DatedURI> replayResults = warehaus.replay(prefix, true, start, end, null, token);

        // Verify the size of the returned list
        assertEquals("The number of replay results is equal to the number of entries inserted", request.getEntriesSize(), replayResults.size());
    }

    @Test
    public void replayTest_ReplayLatestWithNewerReport() throws TException, InterruptedException, EntryNotInWarehausException {
        int numOfEntries = 10;
        PutRequest request = new PutRequest();
        String prefix = "test://replay";

        for (int i = 0; i < numOfEntries; i++) {
            PutUpdateEntry entry = new PutUpdateEntry();
            entry.setVisibility(visibility);
            UpdateEntry update = new UpdateEntry();
            String suffix = Integer.toString(i);
            update.setParsedData(suffix.getBytes());
            update.setUri(prefix + "/" + suffix);
            entry.setEntry(update);
            request.addToEntries(entry);
        }
        long startTs = warehaus.put(request, token).getTimestamp();

        // Insert uri suffix 0 again
        UpdateEntry update = new UpdateEntry();
        update.setParsedData("0".getBytes());
        update.setUri(prefix + "/0");
        long endTs = warehaus.updateEntry(update, visibility, token).getTimestamp();


        DateTime start = TimeUtil.convertToThriftDateTime(startTs - 1000);
        DateTime end = TimeUtil.convertToThriftDateTime(endTs + 1000);

        // Run replay and validate that we get all the URIs we expect
        List<DatedURI> replayResults = warehaus.replay(prefix, true, start, end, null, token);

        // Verify the size of the returned list
        assertEquals("The number of replay results is equal to the number of entries inserted", numOfEntries, replayResults.size());

        // Verify that the most recent URI corresponds with the new entry
        DatedURI latestUri = replayResults.get(replayResults.size() - 1);
        String updatedUri = latestUri.getUri();
        assertEquals("The latest URI is the updated one", prefix + "/0", updatedUri);
        assertEquals("The latest URI has the updated timestamp value", endTs, TimeUtil.convertFromThriftDateTime(latestUri.getTimestamp()));

        // Verify that we don't have any duplicates
        Set<String> uniquenessCheck = Sets.newHashSet();
        for (DatedURI dUri : replayResults) {
            uniquenessCheck.add(dUri.getUri());
        }
        assertEquals("All unique URIs returned", numOfEntries, uniquenessCheck.size());
    }

    @Test
    public void replayTest_WithNewerReportDontReplayOnlyLatest() throws TException, InterruptedException, EntryNotInWarehausException {
        int numOfEntries = 10;
        PutRequest request = new PutRequest();
        String prefix = "test://replay";

        for (int i = 0; i < numOfEntries; i++) {
            PutUpdateEntry entry = new PutUpdateEntry();
            entry.setVisibility(visibility);
            UpdateEntry update = new UpdateEntry();
            String suffix = Integer.toString(i);
            update.setParsedData(suffix.getBytes());
            update.setUri(prefix + "/" + suffix);
            entry.setEntry(update);
            request.addToEntries(entry);
        }
        long startTs = warehaus.put(request, token).getTimestamp();

        // Insert uri suffix 0 again
        UpdateEntry update = new UpdateEntry();
        update.setParsedData("0".getBytes());
        update.setUri(prefix + "/0");
        long endTs = warehaus.updateEntry(update, visibility, token).getTimestamp();


        DateTime start = TimeUtil.convertToThriftDateTime(startTs - 1000);
        DateTime end = TimeUtil.convertToThriftDateTime(endTs + 1000);

        // Run replay and validate that we get all the URIs we expect
        List<DatedURI> replayResults = warehaus.replay(prefix, false, start, end, null, token);

        // Verify the size of the returned list
        assertEquals("The number of replay results is equal to the number of entries inserted", numOfEntries + 1, replayResults.size());

        // Verify that the updated URI appears twice
        List<DatedURI> matches = Lists.newArrayList();
        for (DatedURI uri : replayResults) {
            if (uri.getUri().equals(prefix + "/0")) {
                matches.add(uri);
            }
        }
        assertEquals("Should have 2 matching URIs", 2, matches.size());
        assertNotEquals("The timestamps should not be the same", matches.get(0).getTimestamp(), matches.get(1).getTimestamp());
    }

    @Test
    public void replayTest_RealisticURI() throws TException, InterruptedException {
        List<Repository> repositories = Lists.newLinkedList();
        Repository repository = new Repository();
        repository.setUri("SOCIAL://chirp/social:otherstuff,WOOOO:./");
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(50);
        repository = new Repository();
        repository.setUri("SOCIAL://chirp/social:ANOTHERone,WOOOO:./");
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(1000);

        List<DatedURI> result = warehaus.replay("SOCIAL://chirp", false, null, null, null, token);
        assertEquals(2,result.size());
        assertTrue(compare(result.get(0).getTimestamp(),result.get(1).getTimestamp()) < 0);
        assertEquals(repositories.get(0).getUri(),result.get(0).getUri());
        assertEquals(repositories.get(1).getUri(),result.get(1).getUri());
    }

    @Test
    public void replayTest_ReplayRawData() throws TException, InterruptedException {
        List<UpdateEntry> repositories = Lists.newLinkedList();
        UpdateEntry update = new UpdateEntry();
        update.setUri("SOCIAL://chirp/social:otherstuff,WOOOO:./");
        update.setRawData(UUID.randomUUID().toString().getBytes());
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);

        // Only provide a RAW object for this entry
        update = new UpdateEntry();
        update.setUri("SOCIAL://chirp/social:ANOTHERone,WOOOO:./");
        update.setRawData(UUID.randomUUID().toString().getBytes());
        repositories.add(update);
        warehaus.updateEntry(update, visibility, token);

        // Replay PARSED and verify that one URI is returned
        List<DatedURI> result = warehaus.replay("SOCIAL://chirp", false, null, null, null, token);
        assertEquals(1, result.size());

        // Replay RAW and verify that two URIs are returned
        result = warehaus.replay("SOCIAL://chirp", false, null, null, GetDataType.RAW, token);
        assertEquals(2, result.size());
        assertTrue(compare(result.get(0).getTimestamp(),result.get(1).getTimestamp()) < 0);
        assertEquals(repositories.get(0).getUri(),result.get(0).getUri());
        assertEquals(repositories.get(1).getUri(),result.get(1).getUri());
    }

    @Test(expected = TException.class)
    public void replayTest_InvalidDataType() throws TException, InterruptedException {
        List<UpdateEntry> repositories = Lists.newLinkedList();
        UpdateEntry update = new UpdateEntry();
        update.setUri("SOCIAL://chirp/social:otherstuff,WOOOO:./");
        update.setRawData(UUID.randomUUID().toString().getBytes());
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(update);
        warehaus.updateEntry(update, visibility, token);

        // Replay VIEW and expect TException
        warehaus.replay("SOCIAL://chirp", false, null, null, GetDataType.VIEW, token);
    }

    @Test
    public void testUpdate_ParsedOnly() throws TException, InterruptedException, EntryNotInWarehausException {
        List<UpdateEntry> updates = Lists.newLinkedList();
        UpdateEntry update = new UpdateEntry();
        update.setUri("this://" + UUID.randomUUID().toString());
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);

        BinaryReplay result = warehaus.getLatestParsed(update.getUri(), token);
        assertArrayEquals("Contents are equal", update.getParsedData(), result.getPacket());
    }

    @Test
    public void testUpdate_RawThenParsed() throws TException, InterruptedException, EntryNotInWarehausException {
        List<UpdateEntry> updates = Lists.newLinkedList();
        String uri = "this://testfeed/" + UUID.randomUUID().toString();
        DateTime startTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis());
        UpdateEntry update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);


        BinaryReplay result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setRawData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);
        DateTime endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);

        result = warehaus.getLatestRaw(uri, token);
        assertArrayEquals("Contents of raw are equal", updates.get(1).getRawData(), result.getPacket());

        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());


        // Verify that replay returns 1 URI since we only return parsed objects
        List<DatedURI> replayResults = warehaus.replay("this://testfeed", false, startTime, endTime, null, token);
        assertEquals("Should only have 1 result", 1, replayResults.size());
    }

    @Test
    public void testUpdate_UpdateTypeForEntry() throws TException, InterruptedException, EntryNotInWarehausException {
        List<UpdateEntry> updates = Lists.newLinkedList();
        String uri = "this://" + UUID.randomUUID().toString();
        DateTime startTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis());
        UpdateEntry update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);


        BinaryReplay result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setRawData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);

        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());

        result = warehaus.getLatestRaw(uri, token);
        assertArrayEquals("Contents of raw are equal", updates.get(1).getRawData(), result.getPacket());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(50);
        DateTime endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);


        result = warehaus.getLatestRaw(uri, token);
        assertArrayEquals("Contents of raw are equal", updates.get(1).getRawData(), result.getPacket());
        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(2).getParsedData(), result.getPacket());


        // Verify that replay returns 2 URIs since we only retrieve parsed objects
        List<DatedURI> replayResults = warehaus.replay(uri, false, startTime, endTime, null, token);
        assertEquals("Should only have 2 results", 2, replayResults.size());
    }

    @Test
    public void testUpdate_UpdateParsedNewClassification() throws TException, InterruptedException, EntryNotInWarehausException {
        List<UpdateEntry> updates = Lists.newLinkedList();
        String prefix = "this://test";
        String uri = prefix + "/" + UUID.randomUUID().toString();
        DateTime startTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis());
        UpdateEntry update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(1000);

        BinaryReplay result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());
        List<DatedURI> replayResults = warehaus.replay(prefix, false, null, null, null, token);
        assertEquals("Correct visibility", "U", replayResults.get(0).getVisibility().getFormalVisibility());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, new Visibility().setFormalVisibility("U&USA"), token);
        Thread.sleep(100);
        DateTime endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);

        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(1).getParsedData(), result.getPacket());

        // Verify that replay returns 2 URIs since we only retrieve parsed objects
        replayResults = warehaus.replay(prefix, false, startTime, endTime, null, token);
        assertEquals("Should only have 2 results", 2, replayResults.size());
        assertEquals("Old visibility", "U", replayResults.get(0).getVisibility().getFormalVisibility());
    }

    @Test
    public void testUpdate_MultipleTypes() throws TException, InterruptedException, EntryNotInWarehausException {
        List<UpdateEntry> updates = Lists.newLinkedList();
        String uri = "this://" + UUID.randomUUID().toString();
        DateTime startTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis());
        UpdateEntry update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, visibility, token);
        Thread.sleep(1000);

        BinaryReplay result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());
        List<DatedURI> replayResults = warehaus.replay(uri, false, null, null, null, token);
        assertEquals("Correct visibility", "U", replayResults.get(0).getVisibility().getFormalVisibility());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setRawData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, new Visibility().setFormalVisibility("U&FOUO"), token);
        Thread.sleep(1000);

        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(0).getParsedData(), result.getPacket());
        replayResults = warehaus.replay(uri, false, null, null, null, token);
        // should have old visibility
        assertEquals("New visibility", "U", replayResults.get(0).getVisibility().getFormalVisibility());

        result = warehaus.getLatestRaw(uri, token);
        assertArrayEquals("Contents of raw are equal", updates.get(1).getRawData(), result.getPacket());

        update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        updates.add(update);
        warehaus.updateEntry(update, new Visibility().setFormalVisibility("S"), token);
        Thread.sleep(50);

        result = warehaus.getLatestRaw(uri, token);
        assertArrayEquals("Contents of raw are equal", updates.get(1).getRawData(), result.getPacket());
        result = warehaus.getLatestParsed(uri, token);
        assertArrayEquals("Contents of parsed are equal", updates.get(2).getParsedData(), result.getPacket());
        DateTime endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);


        // Verify that replay returns 2 URIs, since we only retrieve parsed objects
        replayResults = warehaus.replay(uri, false, startTime, endTime, null, token);
        assertEquals("Should only have 2 results", 2, replayResults.size());
        assertEquals("Old visibility", "U", replayResults.get(0).getVisibility().getFormalVisibility());
    }

    @Test(expected = EntryNotInWarehausException.class)
    public void entryNotInWarehaus() throws TException, EntryNotInWarehausException {
        warehaus.getLatestParsed("fake", token);
    }
	
	@Test
	public void versionsTest() throws TException, InterruptedException, EntryNotInWarehausException {
		Repository[] repositories = new Repository[3];
		long[] timestamps = new long[3];
		String uri = "http://hellopeople/12345";
		Repository repository = new Repository();
		repository.setUri(uri);
		repository.setRawData("raw".getBytes());
		repository.setParsedData("parsed".getBytes());
		repositories[0] = repository;
		timestamps[0] = warehaus.insert(repository, visibility, token).getTimestamp();
        Thread.sleep(50);
		repository = new Repository();
		repository.setUri(uri);
		repository.setRawData("raw1".getBytes());
		repository.setParsedData("parsed1".getBytes());
		repositories[1] = repository;
		timestamps[1] = warehaus.insert(repository, visibility, token).getTimestamp();
		assertTrue(timestamps[0] < timestamps[1]);
        Thread.sleep(50);
        repository = new Repository();
        repository.setUri(uri);
        repository.setRawData("raw2".getBytes());
        repository.setParsedData("parsed2".getBytes());
        repositories[2] = repository;
        timestamps[2] = warehaus.insert(repository, visibility, token).getTimestamp();
		List<Long> versions = warehaus.getVersions(uri, token);
		assertEquals(3,versions.size());
		assertTrue(versions.get(0) > versions.get(1)); // most recent one comes first
        assertTrue(versions.get(1) > versions.get(2));
        Thread.sleep(500);
		
		BinaryReplay br1 = warehaus.getParsed(uri, timestamps[0], token);
		BinaryReplay br2 = warehaus.getLatestParsed(uri, token);
		assertNotEquals(br1.getPacket().length,br2.getPacket().length);
        assertArrayEquals(repositories[2].getParsedData(), br2.getPacket());
        assertArrayEquals(repositories[0].getParsedData(), br1.getPacket());
	}

    // TODO ignoring this while we determine if importFromHadoop should be implemented
//	@Test
	public void importTest() throws TException, IOException, EntryNotInWarehausException {
		String raw = "1,two,<3,4444,2+2 for very large values of two";
		String parsed = "<list><value>1</value><value>2</value><value>3</value><value>4</value><value>5</value></list>";
		String uri = "test://listofcrap";
		String filename = "datawarehaustestfile.txt";
		Path file = new Path(filename);
        try {
            OutputStream os = hdfs.create(file,true);
            Repository data = new Repository();
            data.setRawData(raw.getBytes());
            data.setParsedData(parsed.getBytes());
            data.setUri(uri);
            byte[] serialized = new TSerializer().serialize(data);
            os.write(serialized);
            os.flush();
            os.close();
            warehaus.importFromHadoop(filename, visibility, token);
            assertEquals(raw,new String(warehaus.getLatestRaw(uri, token).getPacket()));
            assertEquals(parsed,new String(warehaus.getLatestParsed(uri,token).getPacket()));
        } finally {
            hdfs.delete(file, false);
        }
	}

	@Test
	public void viewsTest() throws TException, InterruptedException, EntryNotInWarehausException {
		ViewId id = new ViewId("view_test_uri","testcases","myview");
		long ts1 = warehaus.insertView(ByteBuffer.wrap("show me this".getBytes()), id, visibility, token).getTimestamp();
		Thread.sleep(1000);
		long ts2 = warehaus.insertView(ByteBuffer.wrap("show me that instead".getBytes()), id, visibility, token).getTimestamp();
		assertTrue(ts1 < ts2);
		BinaryReplay val1 = warehaus.getView(id, ts1, token);
		BinaryReplay val2 = warehaus.getLatestView(id, token);
		assertEquals("show me this", new String(val1.getPacket()));
		assertEquals("show me that instead", new String(val2.getPacket()));
	}

	@Test
	public void smugglingTest() throws TException, InterruptedException {
		String raw = "1,two,<3,4444,2+2 for very large values of two";
		String parsed = "<list><value>1</value><value>2</value><value>3</value><value>4</value><value>5</value></list>";
		String uri = "test://listofcrap";
		Repository data = new Repository();
		data.setUri(uri);
		data.setRawData(raw.getBytes());
		data.setParsedData(parsed.getBytes());
		warehaus.insert(data, visibility, token);
		Thread.sleep(1000);
		int count = warehaus.replayCount(uri, null, null, null, token);
		assertEquals(1,count);
	}
	
	@Test
	public void insertPurgeStatus_NullState() {
	   
	   try {
         warehaus.insertPurgeStatus(null);
         fail("The method should not accept a null purgeState.");
      } catch (TException e) {
      }
	}

   //@Test
	public void insertPurgeStatus_Basic() throws TException {
      
      long purgeId = new Date().getTime();
      Repository repository = new Repository();
      repository.setUri("test_uri_1");
      repository.setRawData("raw".getBytes());
      repository.setParsedData("parsed".getBytes());
      warehaus.insert(repository, visibility, token);
      
      PurgeState state = new PurgeState();
      state.setNotPurged(new TreeSet<Long>());
      state.setPurged(new TreeSet<Long>());
      state.setPurgeId(purgeId);
      state.setPurgeStatus(PurgeStatus.PURGING);
      state.setSuggestedPollPeriod(2000);
      state.setTimeStamp(TimeUtil.getCurrentThriftDateTime());
      
      this.warehaus.insertPurgeStatus(state);
      
      PurgeState fetchedState = this.warehaus.purgeStatus(token, purgeId);
      
      assertEquals(state, fetchedState);
	}
	
   //@Test
	public void beginPurge_NullIdsToPurge() throws PurgeException, EzSecurityTokenException, TException {
      
	   long purgeId = new Date().getTime();
      TreeSet<Long> idsToPurge = null;
      
      PurgeState state = warehaus.beginPurge("purgeCallbackService", purgeId, idsToPurge, token);
      assertEquals("Purge Id", purgeId, state.getPurgeId());
      assertEquals("Purge Status", PurgeStatus.FINISHED_COMPLETE, state.getPurgeStatus());
      assertNotNull("Not Purged Set Not Null", state.getNotPurged());
      assertEquals("Not Purged Set Size", 0, state.getNotPurged().size());
      assertNotNull("Purged Set Not Null", state.getPurged());
      assertEquals("Purged Set Size", 0, state.getPurged().size());
	}
	
   //@Test
	public void beginPurge_EmptyIdsToPurge() throws PurgeException, EzSecurityTokenException, TException {
      
      long purgeId = new Date().getTime();
      TreeSet<Long> idsToPurge = new TreeSet<Long>();
      
      PurgeState state = warehaus.beginPurge("purgeCallbackService", purgeId, idsToPurge, token);
      assertEquals("Purge Id", purgeId, state.getPurgeId());
      assertEquals("Purge Status", PurgeStatus.FINISHED_COMPLETE, state.getPurgeStatus());
      assertNotNull("Not Purged Set Not Null", state.getNotPurged());
      assertEquals("Not Purged Set Size", 0, state.getNotPurged().size());
      assertNotNull("Purged Set Not Null", state.getPurged());
      assertEquals("Purged Set Size", 0, state.getPurged().size());
	}
   
   // TODO: mock provenance id mapping
   public void beginPurge_BasicCase() {
   }
   
   //@Test
   public void purgeStatus_PurgeIdNotFound() throws TException {
      
      long purgeId = new Date().getTime();
      
      PurgeState fetchedState = this.warehaus.purgeStatus(token, purgeId);
      
      assertEquals("Purge Id", purgeId, fetchedState.getPurgeId());
      assertEquals("Purge Status", PurgeStatus.UNKNOWN_ID, fetchedState.getPurgeStatus());
      assertNotNull("Not Purged Set Not Null", fetchedState.getNotPurged());
      assertEquals("Not Purged Set Size", 0, fetchedState.getNotPurged().size());
      assertNotNull("Purged Set Not Null", fetchedState.getPurged());
      assertEquals("Purged Set Size", 0, fetchedState.getPurged().size());
   }
	
    @Test
    public void remove_SingleUri() throws Exception {
        
        String uri = "test://listofcrap";
        Repository data = new Repository();
        data.setUri(uri);
        data.setRawData("hellohellohello".getBytes());
        data.setParsedData("lololo".getBytes());
        
        long timestamp = warehaus.insert(data, visibility, token).getTimestamp();
        Thread.sleep(1000);
        
        // Verify that inserted item is in the warehaus.
        try {
            warehaus.getParsed(uri, timestamp, token);
        } catch (EntryNotInWarehausException e) {
            fail("Received an EntryNotInWarehausException; should not have seen this exception");
        }
        int count = warehaus.replayCount(uri, null, null, null, token);
        assertEquals(1, count);
        
        // Remove the item.
        ArrayList<String> uris = new ArrayList<String>();
        uris.add(uri);
        warehaus.remove(uris, token);
        
        // Verify that deleted item is not in the warehaus.
        try {
           warehaus.getLatestParsed(uri, token);
           fail("When getLastestParsed is called using the uri that was deleted then an EntryNotInWarehausException should be thrown.");
        } catch (EntryNotInWarehausException e) {
        }
        count = warehaus.replayCount(uri, null, null, null, token);
        assertEquals(0, count);
    }
    
    @Test
    public void remove_MultipleUris() throws Exception {

       String uri1 = "test://test/1";
       String uri2 = "test://test/2";
       String uri3 = "test://test/3";

       Repository data = new Repository();
       data.setUri(uri1);
       data.setRawData("111111111111".getBytes());
       data.setParsedData("one".getBytes());
       long timestamp1 = warehaus.insert(data, visibility, token).getTimestamp();

       data.setUri(uri2);
       data.setRawData("222222222222".getBytes());
       data.setParsedData("two".getBytes());
       long timestamp2 = warehaus.insert(data, visibility, token).getTimestamp();

       data.setUri(uri3);
       data.setRawData("33333333333".getBytes());
       data.setParsedData("three".getBytes());
       long timestamp3 = warehaus.insert(data, visibility, token).getTimestamp();
       Thread.sleep(1000);

       // Verify that inserted item is in the warehaus.
       try {
          warehaus.getParsed(uri1, timestamp1, token);
       } catch (EntryNotInWarehausException e) {
          fail("Received an EntryNotInWarehausException for uri1; should not have seen this exception");
       }
       try {
          warehaus.getParsed(uri2, timestamp2, token);
       } catch (EntryNotInWarehausException e) {
          fail("Received an EntryNotInWarehausException for uri2; should not have seen this exception");
       }
       try {
          warehaus.getParsed(uri3, timestamp3, token);
       } catch (EntryNotInWarehausException e) {
          fail("Received an EntryNotInWarehausException for uri1; should not have seen this exception");
       }

       warehaus.remove(Arrays.asList(uri1, uri2, uri3), token);

       // Verify that deleted items are not in the warehaus.
       List<Long> versions = warehaus.getVersions(uri1, token);
       assertEquals("Uri1 should have been deleted.", 0, versions.size());

       versions = warehaus.getVersions(uri2, token);
       assertEquals("Uri2 should have been deleted.", 0, versions.size());

       versions = warehaus.getVersions(uri3, token);
       assertEquals("Uri3 should have been deleted.", 0, versions.size());
    }
    
    @Test
    public void remove_WithUpdates() throws Exception {

       Repository data = new Repository();
       data.setUri("urn://removewithupdates/1");
       data.setRawData("111111111111".getBytes());
       data.setParsedData("one".getBytes());
       warehaus.insert(data, visibility, token);
       Thread.sleep(200);

       data.setParsedData("oneA".getBytes());
       warehaus.insert(data, visibility, token);
       Thread.sleep(200);

       data.setParsedData("oneB".getBytes());
       warehaus.insert(data, visibility, token);
       Thread.sleep(200);
       
       List<Long> versions = warehaus.getVersions(data.getUri(), token);
       assertEquals("Verify version count before remove.", 3, versions.size());

       warehaus.remove(Arrays.asList(data.getUri()), token);
       
       Thread.sleep(200);
       versions = warehaus.getVersions(data.getUri(), token);
       assertEquals("Verify version count after remove.", 0, versions.size());
    }
    
    //@Test
    public void cancelPurge_WithoutToken() throws EzSecurityTokenException, TException {
       
       PurgeState status = warehaus.cancelPurge(null,  new Date().getTime());
       assertEquals(status, CancelStatus.CANNOT_CANCEL);
    }
    
    //@Test
    public void cancelPurge_BasicCase() throws EzSecurityTokenException, TException {
       
       PurgeState status = warehaus.cancelPurge(token,  new Date().getTime());
       assertEquals(status, CancelStatus.CANNOT_CANCEL);
    }

    @Test
    public void testGet() throws TException, InterruptedException {
        List<Repository> repositories = Lists.newLinkedList();
        Repository repository = new Repository();
        repository.setUri("this://testfeed/"+UUID.randomUUID().toString());
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(100);
        repository = new Repository();
        repository.setUri("this://testfeed/"+UUID.randomUUID().toString());
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(100);
        repository = new Repository();
        repository.setUri("this://testfeed/"+UUID.randomUUID().toString());
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(1000);
        
        List<DatedURI> result = warehaus.replay("this://testfeed", false, null, null, null, token);
        assertEquals(3, result.size());
        List<RequestParameter> rps = Lists.newArrayList();
        for(DatedURI datedUri : result) {
            RequestParameter rparam = new RequestParameter();
            rparam.setUri(datedUri.getUri());
            rparam.setTimestamp(datedUri.getTimestamp());
            rps.add(rparam);
        }
        GetRequest getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.RAW); // get RAW data
        getRequest.setLatestVersion(false);
        List<ezbake.warehaus.BinaryReplay> getResults = null;
        
        try {
            getResults = warehaus.get(getRequest, token);
        } catch (MaxGetRequestSizeExceededException ex) {
            fail("Should not exceed max get request size");
        }
        assertEquals(3, getResults.size());
        for (int i = 0; i < getResults.size(); i++) {
            assertEquals("uri must match", repositories.get(i).getUri(), getResults.get(i).getUri());
            // content should be RAW
            assertArrayEquals("content must match", repositories.get(i).getRawData(), getResults.get(i).getPacket());
        }
        getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.PARSED); // get PARSED data
        getRequest.setLatestVersion(false);
        try {
            getResults = warehaus.get(getRequest, token);
        } catch (MaxGetRequestSizeExceededException ex) {
            fail("Should not exceed max get request size");
        }
        assertEquals(3, getResults.size());
        for (int i = 0; i < getResults.size(); i++) {
            assertEquals("uri must match", repositories.get(i).getUri(), getResults.get(i).getUri());
            // content should be PARSED
            assertArrayEquals("content must match", repositories.get(i).getParsedData(), getResults.get(i).getPacket());
            assertFalse(repositories.get(i).getRawData().equals(getResults.get(i).getPacket()));
        }

        ViewId id = new ViewId("view_test_uri_1", "testcases", "myview1");
        long ts1 = warehaus.insertView(ByteBuffer.wrap("some gobbledygook".getBytes()), id, visibility, token).getTimestamp();
        Thread.sleep(100);
        
        id = new ViewId("view_test_uri_2", "testcases", "myview2");
        long ts2 = warehaus.insertView(ByteBuffer.wrap("some more gobbledygook".getBytes()), id, visibility, token).getTimestamp();
        Thread.sleep(100);
        
        rps = Lists.newArrayList();
        RequestParameter rparam = new RequestParameter();
        rparam.setUri("view_test_uri_1");
        rparam.setTimestamp(TimeUtil.convertToThriftDateTime(ts1));
        rparam.setSpacename("testcases");
        rparam.setView("myview1");
        rps.add(rparam);
        rparam = new RequestParameter();
        rparam.setUri("view_test_uri_2");
        rparam.setTimestamp(TimeUtil.convertToThriftDateTime(ts2));
        rparam.setSpacename("testcases");
        rparam.setView("myview2");
        rps.add(rparam);
        
        getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.VIEW); // get VIEW data
        getRequest.setLatestVersion(false);
        try {
            getResults = warehaus.get(getRequest, token);
        } catch (MaxGetRequestSizeExceededException ex) {
            fail("Should not exceed max get request size");
        }
        assertEquals(2, getResults.size());
        assertEquals("view uri must match", rps.get(0).getUri(), getResults.get(0).getUri());
        assertEquals("content must match", "some gobbledygook", new String(getResults.get(0).getPacket()));
        assertEquals("view uri must match", rps.get(1).getUri(), getResults.get(1).getUri());
        assertEquals("content must match", "some more gobbledygook", new String(getResults.get(1).getPacket()));
        
        // test max exceeded exception for timestamped data
        String uri = "this://timestamped_uri";
        repository = new Repository();
        repository.setUri(uri);
        StringBuilder dataSB = new StringBuilder();
        for (int k = 0; k < 1000; k++) {
            dataSB.append("a really long string \n");
        }
        repository.setRawData(dataSB.toString().getBytes());
        repository.setParsedData(dataSB.toString().getBytes());
        repositories.add(repository);
        long ts = warehaus.insert(repository, visibility, token).getTimestamp();
        Thread.sleep(50);

        rparam = new RequestParameter();
        rparam.setUri(uri);
        rparam.setTimestamp(TimeUtil.convertToThriftDateTime(ts));
        rps.add(rparam);
        getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.RAW);
        getRequest.setLatestVersion(false);
        try {
            warehaus.get(getRequest, token);
            fail("Should've run into max get request size exceeded exception");
        } catch (MaxGetRequestSizeExceededException ex) {
            assertTrue("Expected max size exceeded exception", true);
        }
            
        // test get latest stuff
        
        // add 2 entries for a uri
        repositories.clear();
        uri = "this://testfeed/"+UUID.randomUUID().toString();
        repository = new Repository();
        repository.setUri(uri);
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(100);
        repository = new Repository();
        repository.setUri(uri);
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(100);
        
        rps = Lists.newArrayList();
        rparam = new RequestParameter();
        rparam.setUri(uri);
        rps.add(rparam);
        getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.PARSED);
        getRequest.setLatestVersion(true);
        
        try {
            getResults = warehaus.get(getRequest, token);
        } catch (MaxGetRequestSizeExceededException ex) {
            fail("Should not exceed max get request size");
        }
        assertEquals("Should have 1 latest result", 1, getResults.size());
        assertEquals("uri must match", repositories.get(1).getUri(), getResults.get(0).getUri());
        assertArrayEquals("content must match", repositories.get(1).getParsedData(), getResults.get(0).getPacket());

        // test max exceeded exception for latest data
        rparam = new RequestParameter();
        uri = "this://latest_uri";
        rparam.setUri(uri);
        rps.add(rparam);
        repository = new Repository();
        repository.setUri(uri);
        dataSB = new StringBuilder();
        for (int k = 0; k < 1000; k++) {
            dataSB.append("a really long string \n");
        }
        repository.setUri(uri);
        repository.setRawData(dataSB.toString().getBytes());
        repository.setParsedData(dataSB.toString().getBytes());
        repositories.add(repository);
        warehaus.insert(repository, visibility, token);
        Thread.sleep(50);
        getRequest = new GetRequest();
        getRequest.setRequestParams(rps);
        getRequest.setGetDataType(GetDataType.RAW);
        try {
            warehaus.get(getRequest, token);
            fail("Should've run into max get request size exceeded exception");
        } catch (MaxGetRequestSizeExceededException ex) {
            assertTrue("Expected max size exceeded exception", true);
        }
    }

    @Test
    public void testSerialization() throws TException, IOException, ClassNotFoundException {
        StringBuilder dataSB = new StringBuilder();
        for (int k = 0; k < 1000; k++) {
            dataSB.append("a really long string \n");
        }
        byte[] vc = ThriftUtils.serialize(new VersionControl(ByteBuffer.wrap(dataSB.toString().getBytes()), "some-id"));
        VersionControl deserialized = ThriftUtils.deserialize(VersionControl.class, vc);

        Value val = VisibilitySerialization.serializeVisibilityWithDataToValue(new Visibility().setFormalVisibility("U"), vc);
        VisibilityWrapper wrapper = VisibilitySerialization.deserializeVisibilityWrappedValue(val);

        assertEquals(vc.length, wrapper.getValue().length);
        assertArrayEquals(vc, wrapper.getValue());
    }

    @Test
    public void testPut() throws TException, InterruptedException {

        // entries for initial insert
        List<PutUpdateEntry> insertEntries = Lists.newLinkedList();
        DateTime startTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis());
        UpdateEntry entry = new UpdateEntry();
        String uri1 = "this://"+UUID.randomUUID().toString(); 
        entry.setUri(uri1);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        PutUpdateEntry putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        insertEntries.add(putEntry);
        entry = new UpdateEntry();
        String uri2 = "this://"+UUID.randomUUID().toString(); 
        entry.setUri(uri2);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        insertEntries.add(putEntry);
        entry = new UpdateEntry();
        String uri3 = "this://"+UUID.randomUUID().toString(); 
        entry.setUri(uri3);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        insertEntries.add(putEntry);
        
        PutRequest putReq = new PutRequest();
        putReq.setEntries(insertEntries);
        
        // initial insert
        long ts1 = warehaus.put(putReq, token).getTimestamp();
        Thread.sleep(100);
        DateTime endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);
        
        // Verify that replay returns one record per uri
        List<DatedURI> replayResults = warehaus.replay(uri1, false, startTime, endTime, null, token);
        assertEquals("Should have 1 result", 1, replayResults.size());
        replayResults = warehaus.replay(uri2, false, startTime, endTime, null, token);
        assertEquals("Should have 1 result", 1, replayResults.size());
        replayResults = warehaus.replay(uri3, false, startTime, endTime, null, token);
        assertEquals("Should have 1 result", 1, replayResults.size());

        // entries for update
        List<PutUpdateEntry> updateEntries = Lists.newLinkedList();
        entry = new UpdateEntry();
        entry.setUri(uri1);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        updateEntries.add(putEntry);
        entry = new UpdateEntry();
        entry.setUri(uri2);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        updateEntries.add(putEntry);
        entry = new UpdateEntry();
        entry.setUri(uri3);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData("part of a bulk update".getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        updateEntries.add(putEntry);
        
        putReq = new PutRequest();
        putReq.setEntries(updateEntries);

        // subsequent update
        long ts2 = warehaus.put(putReq, token).getTimestamp();
        Thread.sleep(100);
        endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);
        assertTrue("Should have more recent updated time", ts2 > ts1);
        
        // Verify that replay returns 2 records per uri
        replayResults = warehaus.replay(uri1, false, startTime, endTime, null, token);
        assertEquals("Should have 2 results", 2, replayResults.size());
        replayResults = warehaus.replay(uri2, false, startTime, endTime, null, token);
        assertEquals("Should have 2 results", 2, replayResults.size());
        replayResults = warehaus.replay(uri3, false, startTime, endTime, null, token);
        assertEquals("Should have 2 results", 2, replayResults.size());
        
        try {
            BinaryReplay result = warehaus.getLatestParsed(updateEntries.get(2).getEntry().getUri(), token);
            assertEquals("Contents are equal", "part of a bulk update", new String(result.getPacket()));
        } catch (EntryNotInWarehausException ex) {
            fail("Should not run into this exception");
        }

        updateEntries.clear();
        entry = new UpdateEntry();
        entry.setUri(uri1);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        updateEntries.add(putEntry);
        entry = new UpdateEntry();
        entry.setUri(uri2);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(visibility);
        updateEntries.add(putEntry);
        entry = new UpdateEntry();
        entry.setUri(uri3);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        putEntry.setVisibility(new Visibility().setFormalVisibility("U&FOUO"));
        updateEntries.add(putEntry);
        
        putReq = new PutRequest();
        putReq.setEntries(updateEntries);

        // yet another subsequent update
        long ts3 = warehaus.put(putReq, token).getTimestamp();
        Thread.sleep(100);
        endTime = TimeUtil.convertToThriftDateTime(System.currentTimeMillis() + 1000);
        assertTrue("Should have more recent update time", ts3 > ts2);
        
        // Verify that replay returns 3 records per uri
        replayResults = warehaus.replay(uri1, false, startTime, endTime, null, token);
        assertEquals("Should have 3 results", 3, replayResults.size());
        replayResults = warehaus.replay(uri2, false, startTime, endTime, null, token);
        assertEquals("Should have 3 results", 3, replayResults.size());
        replayResults = warehaus.replay(uri3, false, startTime, endTime, null, token);
        assertEquals("Should have 3 results", 3, replayResults.size());
        // should have old visibility
        assertEquals("Has new visibility", "U",replayResults.get(0).getVisibility().getFormalVisibility());
        
    }
    
    //@Test
    public void testGetEntryDetails() throws TException, InterruptedException {

        String uri = "this://testfeed/someuri";
        Repository repository = new Repository();
        repository.setUri(uri);
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());
        warehaus.insert(repository, new Visibility().setFormalVisibility("U"), token);
        Thread.sleep(50);
        
        UpdateEntry update = new UpdateEntry();
        update.setUri(uri);
        update.setParsedData(UUID.randomUUID().toString().getBytes());
        warehaus.updateEntry(update, new Visibility().setFormalVisibility("S"), token);
        Thread.sleep(50);
        
        EntryDetail detail = warehaus.getEntryDetails(uri, token);
        assertEquals(uri, detail.getUri());
        assertEquals(2, detail.getVersions().size());
        assertEquals(uri, detail.getVersions().get(0).getUri());
        assertEquals(uri, detail.getVersions().get(1).getUri());
        assertEquals("warehaus-test", detail.getVersions().get(0).getSecurityId());
        assertEquals("warehaus-test", detail.getVersions().get(1).getSecurityId());
        assertNotNull(detail.getVersions().get(0).getTimestamp());
        assertNotNull(detail.getVersions().get(1).getTimestamp());
    }
    
    @Test
    public void testPut_ExternalCommunityMarkings() throws TException, InterruptedException {
        // entries for initial insert
        List<PutUpdateEntry> insertEntries = Lists.newLinkedList();
        UpdateEntry entry = new UpdateEntry();
        String uri1 = "this://test/" + UUID.randomUUID().toString();
        entry.setUri(uri1);
        entry.setRawData(UUID.randomUUID().toString().getBytes());
        entry.setParsedData(UUID.randomUUID().toString().getBytes());
        PutUpdateEntry putEntry = new PutUpdateEntry();
        putEntry.setEntry(entry);
        Visibility withCommunity = new Visibility().setFormalVisibility("U").setAdvancedMarkings(new AdvancedMarkings().setExternalCommunityVisibility("TEST"));
        putEntry.setVisibility(withCommunity);
        insertEntries.add(putEntry);

        PutRequest putReq = new PutRequest();
        putReq.setEntries(insertEntries);

        // initial insert
        // should fail when token has no external comm. auths
        IngestStatus status = warehaus.put(putReq, token);
        assertTrue("Given user token should not have the required authorizations to add/update document", 
                status.getStatus() == IngestStatusEnum.FAIL);

        // add external comm. auths to token and insert, should work
        token.getAuthorizations().setExternalCommunityAuthorizations(Sets.newHashSet("TEST"));
        try {
            warehaus.put(putReq, token);
        } catch (TException e) {
            fail("Should not have run into exception");
        }
        
        token.getAuthorizations().setExternalCommunityAuthorizations(Sets.newHashSet(""));
        try {
            warehaus.getLatestParsed(uri1, token);
            assertTrue("An exception should have been thrown here", false);
        } catch (EntryNotInWarehausException e) {
            assertTrue("Was not able to see the entry with communities", true);
        }

        EzSecurityToken tokenWithCommunities = TestUtils.createTestToken(String.valueOf("warehaus"),
                                                Sets.newHashSet("U", "S", "C", "FOUO", "USA"),
                                                String.valueOf("some_id"));
        tokenWithCommunities.getAuthorizations().setExternalCommunityAuthorizations(Sets.newHashSet("TEST"));

        BinaryReplay result = warehaus.getLatestParsed(uri1, tokenWithCommunities);
        assertNotNull("Got the object back from the warehaus", result);
        assertEquals("Visibility is correct", withCommunity, result.getVisibility());
        assertArrayEquals("Correct data returned", entry.getParsedData(), result.getPacket());
    }

    @Test
    public void testPermissions() throws TException, InterruptedException {

        EzBakeApplicationConfigurationHelper helper = new EzBakeApplicationConfigurationHelper(config);
        EzSecurityToken fouoToken = TestUtils.createTestToken(String.valueOf(helper.getSecurityID()),
                                    Sets.newHashSet("U", "USA", "FOUO"),
                                    String.valueOf(helper.getSecurityID()));
        fouoToken.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1l));
        EzSecurityToken secretToken = TestUtils.createTestToken(String.valueOf(helper.getSecurityID()),
                                        Sets.newHashSet("U", "USA", "FOUO", "S"),
                                        String.valueOf(helper.getSecurityID()));
        secretToken.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(545l));
        secretToken.getAuthorizations().setExternalCommunityAuthorizations(Sets.newHashSet("Foo"));

        EzSecurityToken tsToken = TestUtils.createTestToken(String.valueOf(helper.getSecurityID()),
                                        Sets.newHashSet("U", "USA", "FOUO", "S", "TS"),
                                        String.valueOf(helper.getSecurityID()));
        
        Repository repository = new Repository();
        String uri = "this://testfeed/"+UUID.randomUUID().toString();
        repository.setUri(uri);
        repository.setRawData(UUID.randomUUID().toString().getBytes());
        repository.setParsedData(UUID.randomUUID().toString().getBytes());

        long ts = 0;
        IngestStatus status = warehaus.insert(repository, 
            new Visibility().setFormalVisibility("S").setAdvancedMarkings(
                    new AdvancedMarkings().setExternalCommunityVisibility("Bar")), secretToken);
        assertTrue("An exception should have been thrown here", status.getStatus() == IngestStatusEnum.FAIL);
        
        status = warehaus.insert(repository, 
            new Visibility().setFormalVisibility("S").setAdvancedMarkings(
                    new AdvancedMarkings().setExternalCommunityVisibility("Foo")), secretToken);
        assertTrue("Data should have been inserted", status.getStatus() == IngestStatusEnum.SUCCESS);
        ts = status.getTimestamp();
        
        try {
            warehaus.getParsed(uri, ts, fouoToken);
            fail("An exception should have been thrown here");
        } catch (EntryNotInWarehausException e) {
            assertTrue("Should not have auth to see this document", true);
        }

        try {
            warehaus.getParsed(uri, ts, secretToken);
        } catch (EntryNotInWarehausException e) {
            fail("No exception should have been thrown here");
        }
        
        UpdateEntry update = new UpdateEntry();
        update.setParsedData("something".getBytes());
        update.setUri(uri);
        status = warehaus.updateEntry(update, new Visibility().setFormalVisibility("S"), fouoToken);
        assertTrue("An exception should have been thrown here", status.getStatus() == IngestStatusEnum.FAIL);

        status = warehaus.updateEntry(update, new Visibility().setFormalVisibility("S"), secretToken);
        assertTrue("No exception should have been thrown here", status.getStatus() == IngestStatusEnum.SUCCESS);
        
        String uri2 = "this://testfeed/"+UUID.randomUUID().toString();
        final Authorizations auths = new Authorizations();
        auths.setFormalAuthorizations(Sets.newHashSet("U", "C", "S", "TS", "USA"));
        auths.setExternalCommunityAuthorizations(Sets.newHashSet("Foo", "Bar"));
        auths.setPlatformObjectAuthorizations(Sets.newHashSet(4L, 57L, 2786L, 123876592238L));
        
        EzSecurityToken tokenWithPoa = TestUtils.createTestToken(
                String.valueOf(helper.getSecurityID()),
                Sets.newHashSet("U", "C", "S", "TS", "USA"),
                String.valueOf(helper.getSecurityID()));
        tokenWithPoa.setAuthorizations(auths);
        
        Visibility visWithPov = new Visibility();
        visWithPov.setFormalVisibility("TS&USA");
        PlatformObjectVisibilities pov = new PlatformObjectVisibilities();
        pov.setPlatformObjectReadVisibility(Sets.newHashSet(56L, 2785L, 123876592237L));
        pov.setPlatformObjectWriteVisibility(Sets.newHashSet(2785L));
        pov.setPlatformObjectDiscoverVisibility(Sets.newHashSet(3L, 56L, 2785L, 123876592237L));
        pov.setPlatformObjectManageVisibility(Sets.newHashSet(123876592237L));
        visWithPov.setAdvancedMarkings(
                new AdvancedMarkings().setExternalCommunityVisibility("Bar"));
        visWithPov.getAdvancedMarkings().setPlatformObjectVisibility(pov);
        
        Repository repository2 = new Repository();
        repository2.setUri(uri2);
        repository2.setRawData(UUID.randomUUID().toString().getBytes());
        repository2.setParsedData(UUID.randomUUID().toString().getBytes());
        
        status = warehaus.insert(repository2, visWithPov, tokenWithPoa);
        assertTrue("Insert should fail due to missing group auths", status.getStatus() == IngestStatusEnum.FAIL);
        
        // now get the right groups in the token
        auths.setPlatformObjectAuthorizations(Sets.newHashSet(3L, 56L, 2785L, 123876592237L));
        tokenWithPoa.setAuthorizations(auths);

        status = warehaus.insert(repository2, visWithPov, tokenWithPoa);
        assertTrue("Required group auths now present, insert should succeed", 
                status.getStatus() == IngestStatusEnum.SUCCESS);
        
        // TS token with no group auths, should fail
        try {
            warehaus.getLatestParsed(uri2, tsToken);
            fail("getLatestParsed should fail due to missing group auths");
        } catch (EntryNotInWarehausException e) {
            assertTrue("getLatestParsed should fail due to missing group auths", true);
        }
        // should pass now with the right token
        try {
            warehaus.getLatestParsed(uri2, tokenWithPoa);
        } catch (EntryNotInWarehausException e) {
            fail("getLatestParsed should succeed");
        }

        // remove manage auth for the group, update should fail
        auths.setPlatformObjectAuthorizations(Sets.newHashSet(3L, 56L, 2785L));
        tokenWithPoa.setAuthorizations(auths);
        
        UpdateEntry someupdate = new UpdateEntry();
        someupdate.setParsedData("something".getBytes());
        someupdate.setUri(uri2);
        status = warehaus.updateEntry(someupdate, visWithPov, tokenWithPoa);
        assertTrue("Required group manage auths not present, update should fail", 
                status.getStatus() == IngestStatusEnum.FAIL);

        // insert should succeed even with the manage auth removed
        String uri3 = "this://testfeed/"+UUID.randomUUID().toString();
        Repository repository3 = new Repository();
        repository3.setUri(uri3);
        repository3.setRawData(UUID.randomUUID().toString().getBytes());
        repository3.setParsedData(UUID.randomUUID().toString().getBytes());
        status = warehaus.insert(repository3, visWithPov, tokenWithPoa);
        assertTrue("Insert should succeed w/o manage auth but w/ write auth for the group", 
                    status.getStatus() == IngestStatusEnum.SUCCESS);
        
        // add group manage auth back in, update should now work
        auths.setPlatformObjectAuthorizations(Sets.newHashSet(3L, 56L, 2785L, 123876592237L));
        tokenWithPoa.setAuthorizations(auths);
        status = warehaus.updateEntry(someupdate, visWithPov, tokenWithPoa);
        assertTrue("Required group manage auth present, update should succeed", 
                    status.getStatus() == IngestStatusEnum.SUCCESS);
        
    }
    
	private int compare(DateTime d1, DateTime d2) {
		return (int)(TimeUtil.convertFromThriftDateTime(d1) - TimeUtil.convertFromThriftDateTime(d2));
	}
	
}
