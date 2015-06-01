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

package ezbake.groups.cli.commands;

import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.local.redis.LocalRedis;
import ezbake.local.zookeeper.LocalZookeeper;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 12:09 PM
 */
public class CommandTestBase {
    public static final String REDIS_HOST = "localhost";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void cleanFileSystem() throws IOException {
        FileUtils.cleanDirectory(folder.getRoot());
    }

    static LocalZookeeper zoo;
    static LocalRedis redisServer;
    static Properties globalProperties = new Properties();

    public static void startZookeeper() throws Exception {
        zoo = new LocalZookeeper();
        globalProperties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, zoo.getConnectionString());
    }

    public static void stopZookeeper() throws IOException {
        if (zoo != null) {
            zoo.shutdown();
        }
    }

    public static void startRedis() throws IOException {
        redisServer = new LocalRedis();
        globalProperties.setProperty(EzBakePropertyConstants.REDIS_HOST, REDIS_HOST);
        globalProperties.setProperty(EzBakePropertyConstants.REDIS_PORT, Integer.toString(redisServer.getPort()));
    }

    public static void stopRedis() throws IOException {
        if (redisServer != null) {
            redisServer.close();
        }
    }
}
