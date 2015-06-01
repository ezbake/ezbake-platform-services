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

import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.cli.commands.user.CreateAppUserCommand;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 8:13 PM
 */
public class CreateAppUserTest extends CommandTestBase {

    @BeforeClass
    public static void setUpClass() throws Exception {
        startZookeeper();
        startRedis();
    }

    @AfterClass
    public static void shutDownClass() throws IOException, InterruptedException {
        stopZookeeper();
        stopRedis();
    }

    Properties properties;
    @Before
    public void setUp() {
        properties = new Properties();
        properties.putAll(globalProperties);
        properties.setProperty("storage.directory", folder.getRoot().getAbsolutePath());
    }

    @Test
    public void testCreateAppUser() throws EzConfigurationLoaderException {
        CreateAppUserCommand command = new CreateAppUserCommand("App1234", "Test", properties);
        command.runCommand();
    }
}
