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

package ezbake.publisher.service;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.ezbroadcast.core.Receiver;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.VisibilityEvaluator;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

/**
 * This class is for testing purposes ONLY. It can be used in situations where a broadcaster
 * is required but there is no message bus running (unit testing for example).
 */
public class PublisherBroadcaster extends EzBroadcaster {

    private static Multimap<String, byte[]> broadcasted;
    private static boolean isBroadcasting = false;

    @Override
    protected void broadcastImpl(String topic, byte[] payload) throws IOException {
        broadcasted.put(topic, payload);
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
        isBroadcasting = true;
        broadcasted = ArrayListMultimap.create();
    }
    
    @Override
    protected void register(String arg0) {
    }

    @Override
    protected void subscribe(String arg0) {
    }

    @Override
    protected void unregister(String arg0) {
    }

    @Override
    protected void unsubscribe(String arg0) {
    }

    @Override
    public void close() throws IOException {
        isBroadcasting = false;
    }

    public static Multimap<String, byte[]> getBroadcasted() {
        while(isBroadcasting);
        return broadcasted;
    }

    public static void clearBroadcasted() {
        if (broadcasted != null) {
            broadcasted.clear();
        }
    }
}
