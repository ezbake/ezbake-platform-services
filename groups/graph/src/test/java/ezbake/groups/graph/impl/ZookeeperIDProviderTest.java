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

import ezbake.local.zookeeper.LocalZookeeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 9/18/14
 * Time: 1:05 PM
 */
public class ZookeeperIDProviderTest {

    LocalZookeeper zoo;
    @Before
    public void setUp() throws Exception {
        zoo = new LocalZookeeper();
    }

    @After
    public void stop() throws IOException {
        if (zoo != null) {
            zoo.shutdown();
        }
    }

    public CuratorFramework getCurator() {
        return CuratorFrameworkFactory.builder()
                .connectString(zoo.getConnectionString())
                .retryPolicy(new RetryNTimes(5, 1000))
                .build();
    }

    @Test
    public void testNamespacing() {
        ZookeeperIDProvider provider = new ZookeeperIDProvider(getCurator(), new MockGroupIdPublisher(), null);
        Assert.assertEquals(ZookeeperIDProvider.zookeeper_namespace+"/"+ZookeeperIDProvider.index_counter, provider.getIndexZkPath());
        Assert.assertEquals(ZookeeperIDProvider.zookeeper_namespace+"/"+ZookeeperIDProvider.counter_lock, provider.getLockZkPath());

        ZookeeperIDProvider named = new ZookeeperIDProvider(getCurator(), new MockGroupIdPublisher(), "name");
        Assert.assertEquals(ZookeeperIDProvider.zookeeper_namespace+"/name/"+ZookeeperIDProvider.index_counter, named.getIndexZkPath());
        Assert.assertEquals(ZookeeperIDProvider.zookeeper_namespace+"/name/"+ZookeeperIDProvider.counter_lock, named.getLockZkPath());
    }

    @Test
    public void test() throws Exception {
        ZookeeperIDProvider provider = new ZookeeperIDProvider(getCurator(), new MockGroupIdPublisher(), null);

        provider.setCurrentID(0);
        Assert.assertEquals(0, provider.currentID());

        Assert.assertEquals(1, provider.nextID());
        Assert.assertEquals(1, provider.currentID());

        Assert.assertEquals(2, provider.nextID());
        Assert.assertEquals(2, provider.currentID());
    }

    @Test
    public void testGetNextIdConcurrency() throws InterruptedException {
        final ZookeeperIDProvider provider = new ZookeeperIDProvider(getCurator(), new MockGroupIdPublisher(), null);
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

        Assert.assertTrue(!ids.contains(null));
        Collections.sort(ids);
        Assert.assertArrayEquals(ids.toArray(), new HashSet<>(ids).toArray());

    }
}
