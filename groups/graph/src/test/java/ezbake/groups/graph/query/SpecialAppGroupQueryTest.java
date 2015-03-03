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

package ezbake.groups.graph.query;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.GraphCommonSetup;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.UserGroupPermissions;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * User: jhastings
 * Date: 7/29/14
 * Time: 9:07 PM
 */
public class SpecialAppGroupQueryTest extends GraphCommonSetup {
    private static final Logger logger = LoggerFactory.getLogger(SpecialAppGroupQueryTest.class);


    @Test
    public void testJustOne() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        // Create an apps subgroup
        User appUser = graph.addUser(BaseVertex.VertexType.APP_USER, "test", "test");

        Vertex appGroup = graph.getGraph().query().has(Group.GROUP_NAME, "root.app.test").vertices().iterator().next();
        graph.addGroup(BaseVertex.VertexType.APP_USER, "test", "special", "root.app.test", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);
        graph.addGroup(BaseVertex.VertexType.APP_USER, "test", "not", "root.app.test", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);

        // Create a regular user to add to the special group
        User user = graph.addUser(BaseVertex.VertexType.USER, "user1", "User");
        graph.addUserToGroup(BaseVertex.VertexType.USER, "user1", "root.app.test.special", true, false, false, false, false);

        Set<String> groups = query.specialAppNamesQuery("special", user.asVertex());
        Set<String> expected = Sets.newHashSet("test");
        Assert.assertEquals(expected, groups);
    }

    @Test
    public void testMultipleApps() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        // Create an apps subgroup
        User appUser1 = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "app1");
        User appUser2 = graph.addUser(BaseVertex.VertexType.APP_USER, "app2", "app2");

        // Add subgroups for app1
        Vertex appGroup1 = graph.getGraph().query().has(Group.GROUP_NAME, "root.app.app1").vertices().iterator().next();
        graph.addGroup(BaseVertex.VertexType.APP_USER, "app1", "special", "root.app.app1", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);
        graph.addGroup(BaseVertex.VertexType.APP_USER, "app1", "not", "root.app.app1", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);

        // Add subgroups for app2
        Vertex appGroup2 = graph.getGraph().query().has(Group.GROUP_NAME, "root.app.app2").vertices().iterator().next();
        graph.addGroup(BaseVertex.VertexType.APP_USER, "app2", "special", "root.app.app2", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);
        graph.addGroup(BaseVertex.VertexType.APP_USER, "app2", "not", "root.app.app2", new GroupInheritancePermissions(), new UserGroupPermissions(), true, false);


        // Create a regular user to add to the special group
        User user = graph.addUser(BaseVertex.VertexType.USER, "user1", "User");
        graph.addUserToGroup(BaseVertex.VertexType.USER, "user1", "root.app.app1.special", true, false, false, false, false);
        graph.addUserToGroup(BaseVertex.VertexType.USER, "user1", "root.app.app2.special", true, false, false, false, false);

        Set<String> groups = query.specialAppNamesQuery("special", user.asVertex());
        Set<String> expected = Sets.newHashSet("app1", "app2");
        Assert.assertEquals(expected, groups);
    }
}
