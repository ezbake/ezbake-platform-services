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

import org.apache.thrift.TException;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ReplayHistoryTest {

    @Test
    public void AddBroadcastTest() throws TException {
        HistoryData data = new HistoryData();

        Long dateEntry = System.currentTimeMillis();
        data.addBroadcast("groupId", "myuriz", "03/04/2014 00:06:00 EST", "03/04/2014 12:20:00 EST",
                "mytopic", "userDn", dateEntry);

        Long dateEntry1 = System.currentTimeMillis();
        RequestHistory history = data.addBroadcast("groupId", "myuri", "03/04/2014 00:00:00 EST", "03/04/2014 12:00:00 EST",
                "mytopic", "bobDn", dateEntry1);

        history.setTotal(Integer.toString(100));
        history.setCount(Integer.toString(40));
        history.setStatus("broadcasting");
        data.updateEntry("bobDn", dateEntry1, history);

        ReplayHistory replayHistory = data.getUserHistory("bobDn");
        assertEquals ("History has one entry", 1, replayHistory.getReplayHistorySize());

        for (Map.Entry<String, RequestHistory> entry : replayHistory.getReplayHistory().entrySet()) {
            assertEquals("Timestamps are equal", Long.toString(dateEntry1), entry.getKey());
            RequestHistory requestHistory = entry.getValue();
            assertEquals("myuri", requestHistory.getUri());
            assertEquals("03/04/2014 00:00:00 EST", requestHistory.getStart());
            assertEquals("03/04/2014 12:00:00 EST", requestHistory.getFinish());
            assertEquals("mytopic", requestHistory.getTopic());
            assertEquals("groupId", requestHistory.getGroupId());
            assertEquals("broadcasting", requestHistory.getStatus());
            assertEquals("100", requestHistory.getTotal());
            assertEquals("40", requestHistory.getCount());
        }
    }

    @Test
    public void Add2BroadcastTest() throws TException, InterruptedException {
        HistoryData data = new HistoryData();

        Long dateEntry = System.currentTimeMillis();
        data.addBroadcast("groupId", "myuriz", "03/04/2014 00:06:00 EST", "03/04/2014 12:20:00 EST",
                "mytopic", "bobDn", dateEntry);
        Thread.sleep(1000);
        Long dateEntry1 = System.currentTimeMillis();
        RequestHistory history = data.addBroadcast("groupId", "myuri", "03/04/2014 00:08:00 EST", "03/04/2014 12:00:00 EST",
                "mytopic", "bobDn", dateEntry1);
        Long dateEntry2 = System.currentTimeMillis();
        data.addBroadcast("groupId", "myuri", "03/04/2014 00:00:00 EST", "03/04/2014 12:00:00 EST",
                "mytopic", "sueDn", dateEntry2);

        history.setTotal(Integer.toString(100));
        history.setCount(Integer.toString(50));
        history.setStatus("broadcasting");
        data.updateEntry("bobDn", dateEntry1, history);

        assertEquals("Correct history size", 2, data.getUserHistory("bobDn").getReplayHistorySize());
    }

    @Test
    public void testRemoveEntry() {
        HistoryData history = new HistoryData();
        Long dateEntry = System.currentTimeMillis();
        history.addBroadcast("groupId", "myuriz", "03/04/2014 00:06:00 EST", "03/04/2014 12:20:00 EST",
                "mytopic", "bobDn", dateEntry);
        Long dateEntry1 = System.currentTimeMillis();
        history.addBroadcast("groupId", "myuriz", "03/04/2014 00:06:00 EST", "03/04/2014 12:20:00 EST",
                "mytopic", "anotherDn", dateEntry1);

        assertNotNull("user history is not null", history.getUserHistory("bobDn"));
        assertEquals("history should have one entry", 1, history.getUserHistory("bobDn").getReplayHistorySize());

        history.removeEntry("bobDn", Long.toString(dateEntry));
        assertNull("User history should be null for DN", history.getUserHistory("bobDn"));
    }
}
