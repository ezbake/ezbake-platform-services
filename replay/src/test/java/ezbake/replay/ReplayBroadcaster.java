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

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.ezbroadcast.core.Receiver;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftTestUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.*;

/**
 * This class is for testing purposes ONLY. It can be used in situations where a broadcaster
 * is required but there is no message bus running (unit testing for example).
 */
public class ReplayBroadcaster extends EzBroadcaster {
    private static Multimap<String, byte[]> broadcasted;
    public static Semaphore lock = new Semaphore(0);

    @Override
    protected void broadcastImpl(String topic, byte[] payload) throws IOException {
        broadcasted.put(topic, payload);
        try {
            security = mock(EzbakeSecurityClient.class);
            when(security.fetchAppToken()).thenReturn(new EzSecurityTokenWrapper(ThriftTestUtils.generateTestSecurityToken("A", "B", "C", "D", "AB", "USA", "CAN")));
        } catch (EzSecurityTokenException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Optional<byte[]> receiveImpl(String topic) throws IOException {
        Collection<byte[]> dataForTopic = broadcasted.get(topic);
        byte[] first = Iterables.getFirst(dataForTopic, null);
        if (first == null) {
            return Optional.absent();
        }
        dataForTopic.remove(first);
        return Optional.of(first);
    }

    @Override
    public void startListening(Receiver receiver) {
    }

    @Override
    protected void prepare(Properties props, String groupId) {
        broadcasted = ArrayListMultimap.create();
    }

    @Override
    public void subscribe(String topic) {
    }

    @Override
    public void register(String topic) {
    }

    @Override
    public void unsubscribe(String topic) {
    }

    @Override
    public void unregister(String topic) {
    }

    @Override
    public void close() throws IOException {
        lock.release();
    }

    public static Multimap<String, byte[]> getBroadcasted() {
        return broadcasted;
    }

    public static void clearBroadcasted() {
        if (broadcasted != null) {
            broadcasted.clear();
        }
    }
}
