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

package ezbake.groups.graph.impl;

import ezbake.groups.graph.api.GroupIDProvider;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 6/23/14
 * Time: 11:52 AM
 */
public class NaieveGroupIDProviderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(NaieveGroupIDProviderTest.class);

    public GroupIDProvider provider;
    @Before
    public void setUp() throws Exception {
        provider = new NaiveIDProvider();
    }

    @Test
    public void testGetNextInitial() throws Exception {
        long l1 = provider.nextID();
        Assert.assertEquals(1, l1);
    }

    @Test
    public void testGetNextFewTimes() throws Exception {
        Assert.assertEquals(1, provider.nextID());
        Assert.assertEquals(2, provider.nextID());
        Assert.assertEquals(3, provider.nextID());
    }

    @Test
    public void testGetNextIdConcurrency() throws InterruptedException {
        final List<Long> ids = Collections.synchronizedList(new ArrayList<Long>());

        Runnable idGetter = new Runnable() {
            @Override
            public void run() {
                try {
                    ids.add(provider.nextID());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        ExecutorService threads = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            threads.submit(idGetter);
        }
        threads.awaitTermination(2, TimeUnit.SECONDS);
        LOGGER.debug("Received set of ids: {}", ids);

        Assert.assertTrue(!ids.contains(null));
        Collections.sort(ids);
        Assert.assertArrayEquals(ids.toArray(), new HashSet<Long>(ids).toArray());

    }

}
