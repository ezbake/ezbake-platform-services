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

package ezbake.replay;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.common.TimeUtil;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.ezbroadcast.core.thrift.SecureMessage;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import ezbake.thrift.ThriftUtils;
import ezbake.warehaus.*;
import ezbakehelpers.accumulo.AccumuloHelper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.junit.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.*;

public class BroadcasterReplayTest {
    private static ThriftServerPool servers;
    private static ThriftClientPool pool;
    private static BroadcasterReplay replay;
    private static EzSecurityToken token;
    private static AccumuloHelper accumulo;

    @BeforeClass
    public static void initialize() throws Exception {
        Properties props = new Properties();
        props.setProperty(EzBakePropertyConstants.THRIFT_USE_SSL, Boolean.toString(false));
        props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "replay");
        props.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:49998");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_USE_MOCK, "true");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_USERNAME, "user");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_PASSWORD, "secret");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_NAMESPACE, "test");
        props.setProperty("ezbatch.user", "user");
        props.setProperty("warehaus.purge.system.visibility", "U");
        props.setProperty(EzBakePropertyConstants.THRIFT_SERVER_MODE, ThriftConfigurationHelper.ThriftServerMode.ThreadedPool.toString());
        props.setProperty(EzBroadcaster.BROADCASTER_CLASS, "ezbake.replay.ReplayBroadcaster");
        props.setProperty(EzBroadcaster.PRODUCTION_MODE, "false");
        props.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true");
        props.setProperty("ezbake.security.client.mode", "MOCK");
        servers = new ThriftServerPool(props, 49999);
        replay = new BroadcasterReplay();
        EzSecurityTokenWrapper warehausToken = new EzSecurityTokenWrapper(getMockToken().deepCopy());
        warehausToken.getValidity().setIssuedFor("warehaus");
        replay.setConfigurationProperties(props);
        replay.getThriftProcessor();
        replay.securityClient = mock(EzbakeSecurityClient.class);
        when(replay.securityClient.fetchDerivedTokenForApp(getMockToken(), "warehaus")).thenReturn(warehausToken);
        accumulo = new AccumuloHelper(props);
        servers.startCommonService(new AccumuloWarehaus(), WarehausServiceConstants.SERVICE_NAME, "warehaus");
//        servers.startCommonService(new BroadcasterReplay(), ReplayServiceConstants.SERVICE_NAME, "replay");
        pool = new ThriftClientPool(props);
    }

    private static EzSecurityToken getMockToken() {
        if (token == null) {
            EzSecurityToken mockToken = MockEzSecurityToken.getMockUserToken("Eric Perry", "TS", Sets.newHashSet("A", "B", "C", "FOUO", "USA"), null, false);
            MockEzSecurityToken.populateAppInfo(mockToken, "replay", "whatever");
            token = mockToken;
            return mockToken;
        } else {
            return token;
        }
    }

    @AfterClass
    public static void destroy() {
        pool.close();
        servers.shutdown();
    }

    @After
    public void tearDown() throws TException, IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        // Clear out all data in the warehaus between tests
        accumulo.getConnector(true).tableOperations().delete("oneaa_warehaus");
        accumulo.getConnector(true).tableOperations().create("oneaa_warehaus", false);
        ReplayBroadcaster.clearBroadcasted();

        // Clear out all user history between tests
        try {
            for (String key : replay.getUserHistory(getMockToken()).getReplayHistory().keySet()) {
                replay.removeUserHistory(getMockToken(), Long.parseLong(key));
            }
        } catch (NoReplayHistory e) {
            // Not a problem
        }
    }


    @Test
    public void testPing() throws TException {
        assertEquals("Ping should be true", true, replay.ping());
    }

    @Test
    public void testReplay_OneRecord() throws TException, InterruptedException {
        WarehausService.Client warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
        Visibility visibility = new Visibility().setFormalVisibility("A");
        Repository repo = new Repository("DEV://replay/test", ByteBuffer.wrap("this is raw data".getBytes()), ByteBuffer.wrap("this is parsed data".getBytes()));
        IngestStatus status = warehaus.insert(repo, visibility, getMockToken());
        pool.returnToPool(warehaus);
        DateTime startTime = TimeUtil.convertToThriftDateTime(status.getTimestamp() - 1000);
        DateTime endTime = TimeUtil.convertToThriftDateTime(status.getTimestamp() + 1000);
        replay.replay("DEV://replay", startTime, endTime, getMockToken(), "group", "topic", true, GetDataType.PARSED, 0);

        // Allow time for the request to be processed
        Thread.sleep(1000);

        Multimap<String, byte[]> broadcasted = ReplayBroadcaster.getBroadcasted();
        assertTrue("Broadcasted onto topic 'topic'", broadcasted.containsKey("topic"));
        byte[] message = Iterables.getFirst(broadcasted.get("topic"), null);
        assertNotNull("Parsed broadcasted data should not be null", message);

        String broadcastedParsedData = new String(ThriftUtils.deserialize(SecureMessage.class, message).getContent());
        assertEquals("Parsed data is correct", "this is parsed data", broadcastedParsedData);
    }

    @Test
    public void testReplay_getStatus() throws TException, InterruptedException, NoReplayHistory {
        Set<String> parsedObjects = Sets.newHashSet();
        WarehausService.Client warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
        Visibility visibility = new Visibility().setFormalVisibility("A");
        String parsed = "this is parsed data";
        parsedObjects.add(parsed);
        Repository repo = new Repository("DEV://replay/test", ByteBuffer.wrap("this is raw data".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
        IngestStatus firstStatus = warehaus.insert(repo, visibility, getMockToken());
        parsed = "this is parsed data1";
        parsedObjects.add(parsed);
        repo = new Repository("DEV://replay/test1", ByteBuffer.wrap("this is raw data1".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
        warehaus.insert(repo, visibility, getMockToken());
        parsed = "this is parsed data2";
        parsedObjects.add(parsed);
        repo = new Repository("DEV://replay/test2", ByteBuffer.wrap("this is raw data2".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
        IngestStatus lastStatus = warehaus.insert(repo, visibility, getMockToken());
        pool.returnToPool(warehaus);
        DateTime startTime = TimeUtil.convertToThriftDateTime(firstStatus.getTimestamp() - 1000);
        DateTime endTime = TimeUtil.convertToThriftDateTime(lastStatus.getTimestamp() + 1000);

        replay.replay("DEV://replay", startTime, endTime, getMockToken(), "group", "topic", true, GetDataType.PARSED, 0);
        // Allow time for the request to be processed
        Thread.sleep(1000);

        ReplayHistory history = replay.getUserHistory(getMockToken());
        Map<String, RequestHistory> requestHistoryMap = history.getReplayHistory();
        RequestHistory request = Iterables.getFirst(requestHistoryMap.values(), null);
        assertNotNull("History should not be null", request);
        assertEquals("Correct count", "3", request.getCount());
        assertEquals("Correct total", "3", request.getTotal());
        assertEquals("Correct topic", "topic", request.getTopic());
        assertEquals("Correct group ID", "group", request.getGroupId());
        assertEquals("Correct status", "complete", request.getStatus());


        Multimap<String, byte[]> broadcasted = ReplayBroadcaster.getBroadcasted();
        assertTrue("Broadcasted onto topic 'topic'", broadcasted.containsKey("topic"));
        for (byte[] message : broadcasted.get("topic")) {
            assertNotNull("Parsed broadcasted data should not be null", message);

            String broadcastedParsedData = new String(ThriftUtils.deserialize(SecureMessage.class, message).getContent());
            assertTrue("Parsed data is correct", parsedObjects.contains(broadcastedParsedData));
            parsedObjects.remove(broadcastedParsedData);
        }
        assertTrue("All parsed objects found", parsedObjects.isEmpty());
    }

    @Test
    public void testReplay_NoResults() throws TException, InterruptedException, NoReplayHistory {
        DateTime startTime = TimeUtil.convertToThriftDateTime(new java.util.Date().getTime());
        DateTime endTime = TimeUtil.convertToThriftDateTime(new java.util.Date().getTime());

        replay.replay("DEV://replay", startTime, endTime, getMockToken(), "group", "topic", true, GetDataType.PARSED, 0);
        // Allow time for the request to be processed
        Thread.sleep(1000);

        ReplayHistory history = replay.getUserHistory(getMockToken());
        Map<String, RequestHistory> requestHistoryMap = history.getReplayHistory();
        RequestHistory request = Iterables.getFirst(requestHistoryMap.values(), null);
        assertNotNull("History should not be null", request);
        assertEquals("Correct count", "0", request.getCount());
        assertEquals("Correct total", "0", request.getTotal());
        assertEquals("Correct topic", "topic", request.getTopic());
        assertEquals("Correct group ID", "group", request.getGroupId());
        assertEquals("Correct status", "no matches", request.getStatus());

        Multimap<String, byte[]> broadcasted = ReplayBroadcaster.getBroadcasted();
        assertTrue("Nothing broadcasted", broadcasted.isEmpty());
    }

    @Test
    public void testReplay_ManyRecordsMultipleClients() throws TException, InterruptedException {
        Set<String> parsedObjects = Sets.newHashSet();
        WarehausService.Client warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
        Visibility visibility = new Visibility().setFormalVisibility("A");
        String parsed = "this is parsed data";
        parsedObjects.add(parsed);
        Repository repo = new Repository("DEV://replay/test", ByteBuffer.wrap("this is raw data".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
        IngestStatus firstInsert = warehaus.insert(repo, visibility, getMockToken());
        for (int i = 0; i < 1000; i++) {
            parsed = Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
            parsedObjects.add(parsed);
            repo = new Repository("DEV://replay/test/" + parsed, ByteBuffer.wrap("this is raw data1".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
            warehaus.insert(repo, visibility, getMockToken());
        }
        parsed = "last entry";
        parsedObjects.add(parsed);
        repo = new Repository("DEV://replay/test/neat", ByteBuffer.wrap("this is raw data2".getBytes()), ByteBuffer.wrap(parsed.getBytes()));
        IngestStatus lastInsert = warehaus.insert(repo, visibility, getMockToken());
        pool.returnToPool(warehaus);
        DateTime startTime = TimeUtil.convertToThriftDateTime(firstInsert.getTimestamp() - 1000);
        DateTime endTime = TimeUtil.convertToThriftDateTime(lastInsert.getTimestamp() + 1000);

        final boolean[] running = new boolean[1];
        running[0] = true;
        Thread statusThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(running[0]) {
                        Thread.sleep(100);
                        ReplayService.Client statusChecker = null;
                        try {
                            statusChecker = pool.getClient("replay", ReplayService.Client.class);
                            ReplayHistory status = statusChecker.getUserHistory(getMockToken());
                            RequestHistory request = Iterables.getFirst(status.getReplayHistory().values(), null);
                            assertNotNull("RequestHistory is not null", request);
                            if (request.getStatus().equals("broadcasting")) {
                                assertEquals("Correct total", Integer.toString(1002), request.getTotal());
                                assertTrue("Count is less than total", Integer.parseInt(request.getCount()) < Integer.parseInt(request.getTotal()));
                            } else if (request.getStatus().equals("complete")) {
                                assertEquals("Correct total", Integer.toString(1002), request.getTotal());
                                assertEquals("Count equals total", Integer.parseInt(request.getTotal()), Integer.parseInt(request.getCount()));
                            }
                        } catch (NoReplayHistory noReplayHistory) {
                            // No status for now
                        } catch (TException e) {
                            // Try again
                        } finally {
                            pool.returnToPool(statusChecker);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        statusThread.start();

        ReplayBroadcaster.lock = new Semaphore(0);
        replay.replay("DEV://replay", startTime, endTime, getMockToken(), "group", "topic", true, GetDataType.PARSED, 0);

        ReplayBroadcaster.lock.acquire();
        running[0] = false;
        Multimap<String, byte[]> broadcasted = ReplayBroadcaster.getBroadcasted();
        assertTrue("Broadcasted onto topic 'topic'", broadcasted.containsKey("topic"));
        for (byte[] message : broadcasted.get("topic")) {
            assertNotNull("Parsed broadcasted data should not be null", message);

            String broadcastedParsedData = new String(ThriftUtils.deserialize(SecureMessage.class, message).getContent());
            assertTrue("Parsed data is correct", parsedObjects.contains(broadcastedParsedData));
            parsedObjects.remove(broadcastedParsedData);
        }
        assertEquals("All parsed objects found", 0, parsedObjects.size());
    }

    @Test
    public void testRemoveHistory() throws TException, InterruptedException {
        DateTime startTime = TimeUtil.convertToThriftDateTime(new java.util.Date().getTime());
        DateTime endTime = TimeUtil.convertToThriftDateTime(new java.util.Date().getTime());

        replay.replay("DEV://replay", startTime, endTime, getMockToken(), "group", "topic", true, GetDataType.PARSED, 0);
        // Allow time for the request to be processed
        Thread.sleep(1000);

        ReplayHistory history = null;
        try {
            history = replay.getUserHistory(getMockToken());
        } catch (NoReplayHistory e) {
            assertTrue("ReplayHistory should exist at this point", false);
        }
        Map<String, RequestHistory> requestHistoryMap = history.getReplayHistory();
        Map.Entry<String, RequestHistory> request = Iterables.getFirst(requestHistoryMap.entrySet(), null);
        assertNotNull("History should not be null", request);
        assertEquals("Correct count", "0", request.getValue().getCount());
        assertEquals("Correct total", "0", request.getValue().getTotal());
        assertEquals("Correct topic", "topic", request.getValue().getTopic());
        assertEquals("Correct group ID", "group", request.getValue().getGroupId());
        assertEquals("Correct status", "no matches", request.getValue().getStatus());

        replay.removeUserHistory(getMockToken(), Long.parseLong(request.getKey()));

        try {
            replay.getUserHistory(getMockToken());
            assertTrue("getUserHistory call should have thrown an exception", false);
        } catch (NoReplayHistory noReplayHistory) {
            assertTrue("History was empty after remove call", true);
        }
    }
}
