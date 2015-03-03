/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.security.service.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class is supposed to implement the Jedis PubSub model, but I haven't quite gotten it to work. Use at own risk
 */
public class EncryptedRedisKeyWatcher extends JedisPubSub implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptedRedisKeyWatcher.class);

    private final ExecutorService executor;
    private final Jedis jedis;
    public EncryptedRedisKeyWatcher(Jedis jedis) {
        this.jedis = jedis;
        executor = Executors.newFixedThreadPool(100);
    }

    public void addSubscription(String channel) {
        executor.submit(new SubscriptionListener(channel));
    }

    /**
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        executor.shutdown();
    }


    @Override
    public void onMessage(String s, String s2) {
        LOGGER.debug("Received message: {} - {}", s, s2);
    }

    @Override
    public void onPMessage(String s, String s2, String s3) {
        LOGGER.debug("Received pmessage: {} - {} - {}", s, s2, s3);
    }

    @Override
    public void onSubscribe(String s, int i) {
        LOGGER.debug("Received subscribe on {}, subscribed channels {}", s, i);
    }

    @Override
    public void onUnsubscribe(String s, int i) {
        LOGGER.debug("Received unsubscribe on {}, subscribed channels {}", s, i);
    }

    @Override
    public void onPUnsubscribe(String s, int i) {
        LOGGER.debug("Received punsubscribe on {}, subscribed channels {}", s, i);
    }

    @Override
    public void onPSubscribe(String s, int i) {
        LOGGER.debug("Received psubscribe on {}, subscribed channels {}", s, i);
    }

    class SubscriptionListener implements Runnable, AutoCloseable {

        private String channel;
        public SubscriptionListener(String channel) {
            this.channel = channel;
        }

        @Override
        public void run() {
            jedis.subscribe(EncryptedRedisKeyWatcher.this, channel);
        }

        @Override
        public void close() throws Exception {
            EncryptedRedisKeyWatcher.this.unsubscribe();
        }
    }
}
