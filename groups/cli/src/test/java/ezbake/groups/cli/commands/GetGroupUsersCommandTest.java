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
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.groups.cli.commands.group.GetGroupUsersCommand;
import ezbake.groups.cli.commands.user.UserCommand;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.GroupInheritancePermissions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/23/14
 * Time: 2:59 PM
 */
public class GetGroupUsersCommandTest extends CommandTestBase {

    @BeforeClass
    public static void setUpClass() throws Exception {
        startZookeeper();
        startRedis();
    }

    @AfterClass
    public static void tearDownClass() throws IOException, InterruptedException {
        stopZookeeper();
        stopRedis();
    }

    GroupNameHelper nameHelper = new GroupNameHelper();
    Properties properties;
    @Before
    public void setUp() {
        properties = new Properties();
        properties.putAll(globalProperties);
        properties.setProperty("storage.directory", folder.getRoot().getAbsolutePath());

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(properties.getProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING))
                .retryPolicy(new RetryNTimes(1, 10)).build();
        client.start();
        try {
            client.delete().deletingChildrenIfNeeded().forPath("/ezbake");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNoGroup() throws EzConfigurationLoaderException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        GetGroupUsersCommand command = new GetGroupUsersCommand("app.AppOne.ezbAudits", "App1", properties);
        command.userType = UserCommand.UserType.APP;

        EzGroupsGraphImpl graph = command.getGraph();
        graph.addUser(BaseVertex.VertexType.APP_USER, "App1", "AppOne");
        graph.addGroup(BaseVertex.VertexType.APP_USER, "App1", "ezbAudits", "root.app.AppOne",
                new GroupInheritancePermissions(),
                UserGroupPermissionsWrapper.ownerPermissions(), false, false);


        command.runCommand();
    }

    @Test
    public void testAudits() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, EzConfigurationLoaderException, InvalidGroupNameException {
        String securityID = "12345";
        String name = "ReplayWeb";

        GetGroupUsersCommand command = new GetGroupUsersCommand("app."+name+".ezbAudits", securityID, properties);
        command.userType = UserCommand.UserType.APP;

        EzGroupsGraphImpl graph = command.getGraph();
        // Create the app user. root.app.name and root.appaccess.name will be created
        User appUser = graph.addUser(BaseVertex.VertexType.APP_USER, securityID, name);
        User auditUser = graph.addUser(BaseVertex.VertexType.USER, "Karl", "Karl");

        String appGroup = nameHelper.getNamespacedAppGroup(name, null);
        Group auditGroup = graph.addGroup(BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.AUDIT_GROUP, appGroup,
                new GroupInheritancePermissions(false, false, false, false, false),
                new UserGroupPermissionsWrapper(false, true, true, true, true), true, false);
        graph.addUserToGroup(BaseVertex.VertexType.USER, "Karl", auditGroup, true, false, false, false, false);

        command.runCommand();
    }


}
