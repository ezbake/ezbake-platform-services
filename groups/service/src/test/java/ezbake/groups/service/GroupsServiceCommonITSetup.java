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

 package ezbake.groups.service;

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.local.redis.LocalRedis;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Random;

public class GroupsServiceCommonITSetup extends GraphCommonSetup {

    LocalRedis redisServer;
    ThriftClientPool clientPool;
    ThriftServerPool pool;
    EzBakeThriftService service;

    static final Random portChooser = new Random(System.currentTimeMillis());
    public static int getRandomPort(int start, int end) {
        return portChooser.nextInt((end - start) + 1) + start;
    }

    @Before
    public void startService() throws Exception {
        int zooPort = getRandomPort(20000, 20499);

        redisServer = new LocalRedis();

        Properties ezProps = new EzConfiguration(new ClasspathConfigurationLoader("/test.properties")).getProperties();
        ezProps.putAll(graphConfiguration);
        ezProps.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:"+Integer.toString(zooPort));
        ezProps.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(redisServer.getPort()));
        ezProps.setProperty(EzBakePropertyConstants.REDIS_HOST, InetAddress.getLocalHost().getCanonicalHostName());
        ezProps.setProperty(GroupsServiceModule.LOG_TIMER_STATS_PROPERTY, Boolean.TRUE.toString());

        pool = new ThriftServerPool(ezProps, 32844);
        service = new EzBakeThriftService();
        pool.startCommonService(service, EzGroupsConstants.SERVICE_NAME, "12345");

        clientPool = new ThriftClientPool(ezProps);
    }

    @After
    public void stopService() throws IOException {
        if (redisServer != null) {
            redisServer.close();
        }
        if (service != null) {
            service.shutdown();
        }
        if (pool != null) {
            pool.shutdown();
        }
        if (clientPool != null) {
            clientPool.close();
        }
    }
}
