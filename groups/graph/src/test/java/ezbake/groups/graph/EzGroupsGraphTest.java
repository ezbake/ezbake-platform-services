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

package ezbake.groups.graph;

import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;

import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.UserGroupPermissions;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: jhastings
 * Date: 6/16/14
 * Time: 12:40 PM
 */
public class EzGroupsGraphTest extends GraphCommonSetup {
    private static final Logger logger = LoggerFactory.getLogger(EzGroupsGraphTest.class);

    @Test
    public void testGetGroupInheritancePermissions() throws VertexExistsException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException, VertexNotFoundException, InvalidVertexTypeException {
        User user = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Group parent = graph.addGroup(BaseVertex.VertexType.USER, user.getPrincipal(), "Parent", new GroupInheritancePermissions(true, true, true, true, true), new UserGroupPermissions(), true, false);
        Group child = graph.addGroup(BaseVertex.VertexType.USER, user.getPrincipal(), "Child", parent);
        
        // test default inheritance permissions  
        GroupInheritancePermissions expected = new GroupInheritancePermissions(true, false, false, false, false);
        GroupInheritancePermissions actual = graph.getGroupInheritancePermissions(child.getGroupName());
        Assert.assertEquals(expected, actual);
            
        // set child group to inherit everything from parent
        graph.setGroupInheritance(child.getGroupName(), true, true, true, true, true);
        expected = new GroupInheritancePermissions(true, true, true, true, true);
        actual = graph.getGroupInheritancePermissions(child.getGroupName());
        Assert.assertEquals(expected, actual);
    }
    
    @Test(expected=VertexExistsException.class)
    public void siblingDuplicationFromCommonTest() throws VertexExistsException, UserNotFoundException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Group tg1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "TestGroup1");

        // Expected to throw
        Group tg2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "TestGroup1");
    }

    @Test(expected=VertexExistsException.class)
    public void siblingDuplicationFromGroupTest() throws VertexExistsException, UserNotFoundException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Group baseGroup = graph.addGroup(BaseVertex.VertexType.USER, "User1", "baseGroup");
        Group subG1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "subGroup1", baseGroup);

        // Expected to throw
        Group subG2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "subGroup1", baseGroup);
    }

    @Test
    public void createWithRequireFlags() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, InvalidGroupNameException {
        graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        graph.addGroup(BaseVertex.VertexType.USER, "User1", "test", new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(), true, true);
        graph.addGroup(BaseVertex.VertexType.USER, "User1", "test2", new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(), false, true);
        graph.addGroup(BaseVertex.VertexType.USER, "User1", "test3", new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(), true, false);
        graph.addGroup(BaseVertex.VertexType.USER, "User1", "test4", new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(), false, false);

        Group g1 = graph.getFramedGraph().query().has(Group.GROUP_NAME, "root.test").vertices(Group.class).iterator().next();
        Assert.assertTrue(g1.isRequireOnlyUser());
        Assert.assertTrue(g1.isRequireOnlyApp());

        Group g2 = graph.getFramedGraph().query().has(Group.GROUP_NAME, "root.test2").vertices(Group.class).iterator().next();
        Assert.assertFalse(g2.isRequireOnlyUser());
        Assert.assertTrue(g2.isRequireOnlyApp());

        Group g3 = graph.getFramedGraph().query().has(Group.GROUP_NAME, "root.test3").vertices(Group.class).iterator().next();
        Assert.assertTrue(g3.isRequireOnlyUser());
        Assert.assertFalse(g3.isRequireOnlyApp());

        Group g4 = graph.getFramedGraph().query().has(Group.GROUP_NAME, "root.test4").vertices(Group.class).iterator().next();
        Assert.assertFalse(g4.isRequireOnlyUser());
        Assert.assertFalse(g4.isRequireOnlyApp());
    }

    @Test
    public void ownerGetsDirectAdminEdges() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        // Users should get them
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Group group = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group");
        Object gid = group.asVertex().getId();

        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user1)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", gid).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user1)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", gid).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user1)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", gid).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user1)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", gid).hasNext());

        // App Users should too
        Object appUser = graph.addUser(BaseVertex.VertexType.APP_USER, "AppUser", "App User One").asVertex().getId();
        Group appUserG1 = graph.addGroup(BaseVertex.VertexType.APP_USER, "AppUser", "Apps group 1");
        Object ag1id = appUserG1.asVertex().getId();

        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(appUser)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", ag1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(appUser)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", ag1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(appUser)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", ag1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(appUser)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", ag1id).hasNext());
    }

    @Test
    public void createChildPathExistsTest() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two").asVertex().getId();

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1",
                new GroupInheritancePermissions(true, false, false, true, true),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group2", g1);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group3", g2);

        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "User2", "group4", new GroupInheritancePermissions(),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Object g1id = g1.asVertex().getId();
        Object g2id = g2.asVertex().getId();
        Object g4id = g4.asVertex().getId();

        // User1 can create child directly (group did not inherit permissions)
        Assert.assertTrue(graph.pathExists(user1, g2id, BaseEdge.EdgeType.A_CREATE_CHILD.toString()));

        // User2 can create child on group that inherited, but not on group that did not inherit
        Assert.assertTrue(graph.pathExists(user2, g1id, BaseEdge.EdgeType.A_CREATE_CHILD.toString()));
        Assert.assertTrue(!graph.pathExists(user2, g2id, BaseEdge.EdgeType.A_CREATE_CHILD.toString()));

        // User2 can create child on group off common that did not inherit, but User1 cannot
        Assert.assertTrue(graph.pathExists(user2, g4id, BaseEdge.EdgeType.A_CREATE_CHILD.toString()));
        Assert.assertTrue(!graph.pathExists(user1, g4id, BaseEdge.EdgeType.A_CREATE_CHILD.toString()));
    }

    @Test(expected=AccessDeniedException.class)
    public void regularUsersCantAddAppGroups() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, VertexNotFoundException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "User1sAppGroup", "root.app");
    }


    @Test
    public void testAddUserToGroupPermissions() throws UserNotFoundException, AccessDeniedException, VertexExistsException, InvalidVertexTypeException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        User user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");

        Object g1id = g1.asVertex().getId();

        // Add user with only data access
        Object user0 = graph.addUser(BaseVertex.VertexType.USER, "User0", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User0", Group.COMMON_GROUP+".group1", false, false, false, false, false);
        // Make sure app user has all the edges on the app group
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user0)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user0)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user0)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user0)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user0)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());

        // Add user with only data access
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User2", Group.COMMON_GROUP+".group1", true, false, false, false, false);
        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());

        // Add user with data and...
        Object user3 = graph.addUser(BaseVertex.VertexType.USER, "User3", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User3", Group.COMMON_GROUP+".group1", true, true, false, false, false);
        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());

        // Add user with data and...
        Object user4 = graph.addUser(BaseVertex.VertexType.USER, "User4", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User4", Group.COMMON_GROUP+".group1", true, true, true, false, false);
        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user4)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user4)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user4)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user4)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user4)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());

        // Add user with data and...
        Object user5 = graph.addUser(BaseVertex.VertexType.USER, "User5", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User5", Group.COMMON_GROUP+".group1", true, true, true, true, false);
        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user5)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user5)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user5)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user5)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(user5)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());

        // Add user with data and...
        Object user6 = graph.addUser(BaseVertex.VertexType.USER, "User6", "User One").asVertex().getId();
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User6", Group.COMMON_GROUP+".group1", true, true, true, true, true);
        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user6)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user6)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user6)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user6)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user6)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", g1id).hasNext());
    }

    @Test
    public void userGroupsList() throws VertexExistsException, UserNotFoundException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group2", g1);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group3", g2);
        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group4");

        ImmutableSortedSet<String> expectedGroupNames = new ImmutableSortedSet.Builder<String>(Ordering.natural())
                .add(Group.COMMON_GROUP)
                .add(Group.COMMON_GROUP+".group1")
                .add(Group.COMMON_GROUP+".group1.group2")
                .add(Group.COMMON_GROUP+".group1.group2.group3")
                .add(Group.COMMON_GROUP+".group4")
                .build();
        ImmutableSortedSet.Builder<String> returnedGroups = new ImmutableSortedSet.Builder<>(Ordering.natural());

        Set<Group> groups = graph.userGroups(user1);
        for (Group g : groups) {
            returnedGroups.add(g.getGroupName());
        }
        Assert.assertEquals(expectedGroupNames, returnedGroups.build());
    }

    @Test
    public void userExplicitGroups() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        // Create some users
        Object adminUser = graph.addUser(BaseVertex.VertexType.USER, "admin", "User One").asVertex().getId();
        Object user = graph.addUser(BaseVertex.VertexType.USER, "user", "User One").asVertex().getId();

        // Create a couple groups
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "admin", "group1");
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "admin", "group2", g1);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "admin", "group3", g2);
        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "admin", "group4");

        // Add the user to some of the groups
        graph.addUserToGroup(BaseVertex.VertexType.USER, "user", g1);
        graph.addUserToGroup(BaseVertex.VertexType.USER, "user", g4);

        // Check the explict memberships
        ImmutableSortedSet<String> expectedGroupNames = new ImmutableSortedSet.Builder<String>(Ordering.natural())
                .add(Group.COMMON_GROUP)
                .add(Group.COMMON_GROUP+".group1")
                .add(Group.COMMON_GROUP+".group4")
                .build();
        ImmutableSortedSet.Builder<String> returnedGroups = new ImmutableSortedSet.Builder<>(Ordering.natural());
        Set<Group> groups = graph.userGroups(graph.getGraph().getVertex(user), true, false);
        for (Group g : groups) {
            returnedGroups.add(g.getGroupName());
        }
        Assert.assertEquals(expectedGroupNames, returnedGroups.build());

        // Check the inherited memberships, for good measure
        expectedGroupNames = new ImmutableSortedSet.Builder<String>(Ordering.natural())
                .add(Group.COMMON_GROUP)
                .add(Group.COMMON_GROUP+".group1")
                .add(Group.COMMON_GROUP+".group1.group2")
                .add(Group.COMMON_GROUP+".group1.group2.group3")
                .add(Group.COMMON_GROUP+".group4")
                .build();
        returnedGroups = new ImmutableSortedSet.Builder<>(Ordering.natural());
        for (Group g : graph.userGroups(user)) {
            returnedGroups.add(g.getGroupName());
        }
        Assert.assertEquals(expectedGroupNames, returnedGroups.build());
    }

    @Test
    public void getUserGroupsOnlyActive() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex();
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group2", g1);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group3", g2);
        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group4");

        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", EzGroupsConstants.ROOT+EzGroupsConstants.GROUP_NAME_SEP+"group1.group2", false);

        ImmutableSortedSet<String> expectedGroupNames = new ImmutableSortedSet.Builder<String>(Ordering.natural())
                .add(Group.COMMON_GROUP)
                .add(Group.COMMON_GROUP+".group1")
                .add(Group.COMMON_GROUP+".group1.group2.group3")
                .add(Group.COMMON_GROUP+".group4")
                .build();
        ImmutableSortedSet.Builder<String> returnedGroups = new ImmutableSortedSet.Builder<>(Ordering.natural());

        Set<Group> groups = graph.userGroups(user1, false, false);
        for (Group g : groups) {
            returnedGroups.add(g.getGroupName());
        }
        Assert.assertEquals(expectedGroupNames, returnedGroups.build());
    }


    @Test
    public void appUserGroupsList() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "AppUser", "App User One").asVertex().getId();
        Group g1 = graph.addGroup(BaseVertex.VertexType.APP_USER, "AppUser", "group1");
        Group g2 = graph.addGroup(BaseVertex.VertexType.APP_USER, "AppUser", "group2", g1);
        Group g3 = graph.addGroup(BaseVertex.VertexType.APP_USER, "AppUser", "group3", g2);
        Group g4 = graph.addGroup(BaseVertex.VertexType.APP_USER, "AppUser", "group4");

        ImmutableSet<String> groupNames = new ImmutableSortedSet.Builder<String>(Ordering.natural())
                .add(Group.COMMON_GROUP)
                .add(Joiner.on(EzGroupsGraph.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, EzGroupsConstants.APP_ACCESS_GROUP))
                    .add(Group.COMMON_GROUP + ".group1")
                    .add(Group.COMMON_GROUP + ".group1.group2")
                    .add(Group.COMMON_GROUP + ".group1.group2.group3")
                    .add(Group.COMMON_GROUP + ".group4")
                    .add(Joiner.on(EzGroupsGraph.GROUP_NAME_SEP).join(Group.COMMON_GROUP, Group.APP_ACCESS_GROUP, "App User One"))
                    .add(Joiner.on(EzGroupsGraph.GROUP_NAME_SEP).join(Group.COMMON_GROUP, Group.APP_GROUP, "App User One"))
                    .build();
        ImmutableSet.Builder<String> returnedGroups = new ImmutableSortedSet.Builder<>(Ordering.natural());
        Set<Group> groups = graph.userGroups(app);
        for (Group g : groups) {
            returnedGroups.add(g.getGroupName());
        }
        Assert.assertEquals(groupNames, returnedGroups.build());
    }


    @Test
    public void groupMembersList() throws VertexExistsException, UserNotFoundException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex();
        Object user1Id = user1.getId();
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two").asVertex().getId();
        Object user3 = graph.addUser(BaseVertex.VertexType.USER, "User3", "User Three").asVertex().getId();
        Object app1 = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "App One").asVertex().getId();
        Object app2 = graph.addUser(BaseVertex.VertexType.APP_USER, "app2", "App Two").asVertex().getId();


        Group group1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Object g1 = group1.asVertex().getId();

        /* NOTE: Below I am checking group membership at every step because I want to make sure there are no membership
         * side effects when I add users to groups
         */

        // User 1 is a member, User 2, 3 are not. App Users are not
        Assert.assertTrue(new GremlinPipeline(user1).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app1)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());

        // Assign user 2 to g1
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User2", group1);

        // User 1 and 2 are members, User 3 is not. App Users are not
        Assert.assertTrue(new GremlinPipeline(user1).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app1)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());

        // Assign user 3 to g1
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User3", group1);

        // User 1, 2, 3 are members. App Users are not
        Assert.assertTrue(new GremlinPipeline(user1).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app1)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());

        // Assign app 1 to g1
        graph.addUserToGroup(BaseVertex.VertexType.APP_USER, "app1", group1);

        // User 1, 2, 3, 4, App 1 are members. App 2 is not
        Assert.assertTrue(new GremlinPipeline(user1).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app1)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(!new GremlinPipeline(graph.getGraph().getVertex(app2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());

        // Assign app 2 to g1
        graph.addUserToGroup(BaseVertex.VertexType.APP_USER, "app2", group1);

        // User 1, 2, 3, 4, App 1, 2 are members
        Assert.assertTrue(new GremlinPipeline(user1).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(user3)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app1)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app2)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1).hasNext());

        Set<User> users = graph.groupMembers(user1, graph.getGraph().getVertex(g1), true, true, false);
        List<String> userNames = new ArrayList<>();
        for (User u : users) {
            userNames.add(u.getPrincipal());
        }
        Assert.assertTrue(userNames.containsAll(Arrays.asList("User1", "User2", "User3", "app1", "app2")));
    }

    @Test
    public void groupMembersExplicit() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex();
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two");
        Object user3 = graph.addUser(BaseVertex.VertexType.USER, "User3", "User Three");

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Object g1id = g1.asVertex().getId();
        Vertex g1v = g1.asVertex();

        Set<String> expectedPrincipals = new HashSet<>();
        Set<String> receivedPrincipals = new HashSet<>();

        // Make sure g1 has only user1 as an explict member
        Set<User> userList = graph.groupMembers(user1, g1v, true, true, true);
        Assert.assertEquals(1, userList.size());
        expectedPrincipals.add("User1");
        for (User u : userList) {
            receivedPrincipals.add(u.getPrincipal());
        }
        Assert.assertTrue(receivedPrincipals.containsAll(expectedPrincipals));

        // Add user2 explictly
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User2", g1);

        // Make sure user2 is in the group now
        userList = graph.groupMembers(user1, g1v, true, true, true);
        Assert.assertEquals(2, userList.size());
        expectedPrincipals.add("User2");
        receivedPrincipals.clear();
        for (User u : userList) {
            receivedPrincipals.add(u.getPrincipal());
        }
        Assert.assertTrue(receivedPrincipals.containsAll(expectedPrincipals));

        // Add user3 explictly
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User3", g1);

        // Make sure user3 is in the group now
        userList = graph.groupMembers(user1, g1v, true, true, true);
        Assert.assertEquals(3, userList.size());
        expectedPrincipals.add("User3");
        receivedPrincipals.clear();
        for (User u : userList) {
            receivedPrincipals.add(u.getPrincipal());
        }
        Assert.assertTrue(receivedPrincipals.containsAll(expectedPrincipals));
    }

    @Test
    public void groupMembersTest() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        User user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        User user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two");

        // User 1 creates the groups and has explicit access
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1",
                new GroupInheritancePermissions(true, false, false, false, false),
                UserGroupPermissionsWrapper.ownerPermissions(), false, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group2", "root.group1");
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group3");
        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group4", "root.group1.group2");


        Set<String> expectedPrincipals = new HashSet<>();
        Set<String> receivedPrincipals = new HashSet<>();

        // Group 1 should have [User1] explicit and [User1, User2] non-explicit
        Set<User> explicitUsersG1 = graph.groupMembers(user1.asVertex(), g1.asVertex(), true, true, true);
        Assert.assertEquals(1, explicitUsersG1.size());
        expectedPrincipals.add(user1.getPrincipal());
        for (User u : explicitUsersG1) {
            receivedPrincipals.add(u.getPrincipal());
        }
        Assert.assertTrue(receivedPrincipals.containsAll(expectedPrincipals));
        expectedPrincipals.clear();
        receivedPrincipals.clear();

        Set<User> nonExplicitUsersG1 = graph.groupMembers(user1.asVertex(), g1.asVertex(), true, true, false);
        Assert.assertEquals(2, nonExplicitUsersG1.size());
        expectedPrincipals.add(user1.getPrincipal());
        expectedPrincipals.add(user2.getPrincipal());
        for (User u : nonExplicitUsersG1) {
            receivedPrincipals.add(u.getPrincipal());
        }
        Assert.assertTrue(receivedPrincipals.containsAll(expectedPrincipals));
        expectedPrincipals.clear();
        receivedPrincipals.clear();
    }


    @Test
    public void removeUserFromGroupTest() throws VertexExistsException, UserNotFoundException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        // Create the group
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Object g1id = g1.asVertex().getId();

        // Create user2 and add him to the group
        User user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two");
        graph.addUserToGroup(BaseVertex.VertexType.USER, "User2", g1);

        // Make sure user is in the group
        Assert.assertTrue(new GremlinPipeline(user2.asVertex()).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());

        // Remove user2 from the group
        graph.removeUserFromGroup(BaseVertex.VertexType.USER, "User2", g1id);

        // Make sure user is NOT in the group anymore
        Assert.assertTrue(!new GremlinPipeline(user2.asVertex()).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", g1id).hasNext());
    }

    @Test
    public void userModifyPrincipal() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, VertexNotFoundException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();

        graph.updateUser(BaseVertex.VertexType.USER, "User1", "User1-a");

        Assert.assertTrue(graph.getGraph().getVertex(user1).getProperty("principal").equals("User1-a"));
    }

    @Test
    public void updateAppUserName() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.APP_USER, "APP123", "App One").asVertex().getId();
        graph.updateUser(BaseVertex.VertexType.APP_USER, "APP123", "APP123", "App Two");

        GroupNameHelper gnh = new GroupNameHelper();
        Group appGroup = graph.getGroup(gnh.getNamespacedAppGroup("App Two"));
        Assert.assertEquals(gnh.getNamespacedAppGroup("App Two"), appGroup.getGroupName());
        Assert.assertEquals("App Two", appGroup.getGroupFriendlyName());

        Group appAccessGroup = graph.getGroup(gnh.getNamespacedAppAccessGroup("App Two"));
        Assert.assertEquals(gnh.getNamespacedAppAccessGroup("App Two"), appAccessGroup.getGroupName());
        Assert.assertEquals("App Two", appAccessGroup.getGroupFriendlyName());
    }

    @Test
    public void deleteUser() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        // Create the user, and make sure they exist
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Assert.assertNotNull(graph.getGraph().getVertex(user1));

        // Delete the user and make sure they don't
        graph.deleteUser(BaseVertex.VertexType.USER, "User1");
        Assert.assertNull(graph.getGraph().getVertex(user1));
    }

    @Test
    public void activeDeactiveTest() throws UserNotFoundException, VertexExistsException, InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        // Create the user, and make sure they are active
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Assert.assertTrue(Boolean.valueOf(graph.getGraph().getVertex(user1).getProperty(User.ACTIVE).toString()));

        graph.setUserActiveOrNot(BaseVertex.VertexType.USER, "User1", false);
        Assert.assertTrue(!Boolean.valueOf(graph.getGraph().getVertex(user1).getProperty(User.ACTIVE).toString()));

        graph.setUserActiveOrNot(BaseVertex.VertexType.USER, "User1", true);
        Assert.assertTrue(Boolean.valueOf(graph.getGraph().getVertex(user1).getProperty(User.ACTIVE).toString()));

    }

    @Test
    public void groupActiveNotTest() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        // Create the user to create the group
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();

        String groupName = "group1";
        String fullGroupName = EzGroupsConstants.ROOT + EzGroupsConstants.GROUP_NAME_SEP + groupName;
        Group g = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");


        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, false);
        Assert.assertTrue(!Boolean.valueOf(g.asVertex().getProperty(Group.ACTIVE).toString()));

        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, true);
        Assert.assertTrue(Boolean.valueOf(g.asVertex().getProperty(Group.ACTIVE).toString()));
    }

    @Test
    public void groupActiveNotChildTest() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        // Create the user to create the group
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex().getId();
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User One").asVertex().getId();

        String groupName = "group1";
        String childGroup = "child";
        String notChild = "not your child";
        String fullGroupName = EzGroupsConstants.ROOT + EzGroupsConstants.GROUP_NAME_SEP + groupName;
        String fullChildGroup = fullGroupName + EzGroupsConstants.GROUP_NAME_SEP + childGroup;
        String notChildGroup = fullChildGroup + EzGroupsConstants.GROUP_NAME_SEP + notChild;

        Group pgroup = graph.addGroup(BaseVertex.VertexType.USER, "User1", groupName, new GroupInheritancePermissions().setAdminCreateChild(true), new UserGroupPermissions().setAdminManage(true), false, false);
        Group cgroup = graph.addGroup(BaseVertex.VertexType.USER, "User1", childGroup, pgroup.asVertex());
        Group ngroup = graph.addGroup(BaseVertex.VertexType.USER, "User2", notChild, pgroup.asVertex());


        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, false);
        Assert.assertTrue(!Boolean.valueOf(pgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(cgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(ngroup.asVertex().getProperty(Group.ACTIVE).toString()));

        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, false, true);
        Assert.assertTrue(!Boolean.valueOf(pgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(!Boolean.valueOf(cgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(ngroup.asVertex().getProperty(Group.ACTIVE).toString()));

        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, true);
        Assert.assertTrue(Boolean.valueOf(pgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(!Boolean.valueOf(cgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(ngroup.asVertex().getProperty(Group.ACTIVE).toString()));

        graph.setGroupActiveOrNot(BaseVertex.VertexType.USER, "User1", fullGroupName, true, true);
        Assert.assertTrue(Boolean.valueOf(pgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(cgroup.asVertex().getProperty(Group.ACTIVE).toString()));
        Assert.assertTrue(Boolean.valueOf(ngroup.asVertex().getProperty(Group.ACTIVE).toString()));

    }


    @Test
    public void testEdgesBetweenVertices() {
        Vertex v1 = graph.getGraph().addVertex(null);
        Vertex v2 = graph.getGraph().addVertex(null);
        Vertex v3 = graph.getGraph().addVertex(null);

        graph.assignEdges(v1, v2, true, false, true, true, true);
        graph.assignEdges(v2, v3, true, true, true, false, true);

        Set<BaseEdge.EdgeType> edges = graph.edgesBetweenVertices(v1, v3,
                BaseEdge.EdgeType.A_CREATE_CHILD,
                BaseEdge.EdgeType.A_READ,
                BaseEdge.EdgeType.A_WRITE,
                BaseEdge.EdgeType.DATA_ACCESS,
                BaseEdge.EdgeType.A_MANAGE);

        ImmutableSet<BaseEdge.EdgeType> expectedEdges = ImmutableSortedSet.of(
                BaseEdge.EdgeType.A_CREATE_CHILD,
                BaseEdge.EdgeType.A_WRITE,
                BaseEdge.EdgeType.DATA_ACCESS);

        Assert.assertEquals(expectedEdges, ImmutableSortedSet.copyOf(edges));
    }

    @Test
    public void testGetGroup() throws InvalidVertexTypeException, VertexExistsException, AccessDeniedException, UserNotFoundException, IndexUnavailableException, InvalidGroupNameException, VertexNotFoundException {
        String id = "CN=Anakin Skywalker";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName = "localgroup";
        GroupNameHelper gnh = new GroupNameHelper();
        
        graph.addUser(type, id, "Darth Vader");
        Group expected = graph.addGroup(type, id, groupName);
        Group actual = graph.getGroup(type, id, gnh.addRootGroupPrefix(groupName));
        
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testGetGroups() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, VertexNotFoundException, InvalidGroupNameException {
        String id = "User1";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Group g1 = graph.addGroup(type, id, groupName1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g3 = graph.addGroup(type, id, groupName3,
                new GroupInheritancePermissions(true, false, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Set<Long> expectedIds = Sets.newTreeSet(Lists.newArrayList(g1.getIndex(), g2.getIndex(), g3.getIndex()));
        Set<Group> groups = graph.getGroups(Sets.newHashSet(g1.getGroupName(), g2.getGroupName(), g3.getGroupName()));
        Set<Long> receivedIds = Sets.newTreeSet();
        for (Group g : groups) {
            receivedIds.add(g.getIndex());
        }
        Assert.assertEquals(expectedIds, receivedIds);
    }

    @Test
    public void testChildGroups() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Group g1 = graph.addGroup(type, id, groupName1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g3 = graph.addGroup(type, id, groupName3,
                new GroupInheritancePermissions(true, false, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        // g3 is in the set even though admin read is false, because the user has direct admin
        Set<Object> expected = Sets.newHashSet(g1.asVertex().getId(), g2.asVertex().getId(), g3.asVertex().getId());
        Set<Object> actual = Sets.newHashSet();

        // Get child groups off common
        Set<Group> gs = graph.getGroupChildren(BaseVertex.VertexType.USER, id, Group.COMMON_GROUP);
        for (Group g : gs) {
            actual.add(g.asVertex().getId());
        }

        // Make sure children only includes g1 and g2
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testChildGroupsNestedLower() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";

        String expectedGroup1 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1);
        String expectedGroup2 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName2);
        String expectedGroup3 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName3);

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Group g1 = graph.addGroup(type, id, groupName1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2, expectedGroup1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g3 = graph.addGroup(type, id, groupName3, expectedGroup1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Set<Object> expected = Sets.newHashSet(g2.asVertex().getId(), g3.asVertex().getId());
        Set<Object> actual = Sets.newHashSet();

        // Get child groups off common
        Set<Group> gs = graph.getGroupChildren(BaseVertex.VertexType.USER, id, expectedGroup1, false);
        for (Group g : gs) {
            actual.add(g.asVertex().getId());
        }

        // Make sure children only includes g1 and g2
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testChildGroupsRecurse() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";

        String expectedGroup1 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1);
        String expectedGroup2 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName2);
        String expectedGroup3 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName3);

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();

        Group g1 = graph.addGroup(type, id, groupName1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2, expectedGroup1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g3 = graph.addGroup(type, id, groupName3, expectedGroup1, new GroupInheritancePermissions(true, true, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Set<Object> expected = Sets.newHashSet(g2.asVertex().getId(), g3.asVertex().getId());
        Set<Object> actual = Sets.newHashSet();

        // Get child groups off common
        Set<Group> gs = graph.getGroupChildren(BaseVertex.VertexType.USER, id, expectedGroup1, true);
        for (Group g : gs) {
            actual.add(g.asVertex().getId());
        }

        // Make sure children only includes g1 and g2
        Assert.assertEquals(expected, actual);
    }


    /**
     * This test is sort of dumb, as it stands. Child groups are the same between users because they follow the
     * inheritance edges, but don't take into account direct admin edges
     */
    @Test
    public void testGetChildrenMultiUser() throws VertexNotFoundException, UserNotFoundException, AccessDeniedException, VertexExistsException, InvalidVertexTypeException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";
        String groupName4 = "mygroup4";

        String expectedGroup1 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1);
        String expectedGroup2 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName2);
        String expectedGroup3 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName3);
        String expectedGroup4 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName4);

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Vertex user2 = graph.addUser(type, id+"2", "User Two").asVertex();

        Group g1 = graph.addGroup(type, id, groupName1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2, expectedGroup1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g3 = graph.addGroup(type, id, groupName3, expectedGroup1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        // Add a group not owned by the other user, to test admin_read permission
        graph.assignEdges(user2, g1.asVertex(), false, true, false, false, true);
        Group g4 = graph.addGroup(type, id+"2", groupName4, expectedGroup1,
                new GroupInheritancePermissions(true, false, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Set<Object> expectedU1 = Sets.newHashSet(g2.asVertex().getId(), g3.asVertex().getId());
        Set<Object> expectedU2 = Sets.newHashSet(g2.asVertex().getId(), g3.asVertex().getId());

        Set<Object> actualU1 = Sets.newHashSet();
        Set<Object> actualU2 = Sets.newHashSet();

        // Get child groups off common
        Set<Group> gsU1 = graph.getGroupChildren(BaseVertex.VertexType.USER, id, expectedGroup1, true);
        for (Group g : gsU1) {
            actualU1.add(g.asVertex().getId());
        }

        Set<Group> gsU2 = graph.getGroupChildren(BaseVertex.VertexType.USER, id, expectedGroup1, true);
        for (Group g : gsU2) {
            actualU2.add(g.asVertex().getId());
        }

        // Make sure users only see children they have admin read on
        Assert.assertEquals(expectedU1, actualU1);
        Assert.assertEquals(expectedU2, actualU2);
    }

    @Test
    public void testAdminReadRequiredToViewChildren() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        String id2 = "User2";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String groupName1 = "mygroup";
        String groupName2 = "mygroup2";
        String groupName3 = "mygroup3";

        String expectedGroup1 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1);
        String expectedGroup2 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName2);
        String expectedGroup3 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1, groupName3);

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Object user2 = graph.addUser(type, id2, "User Two").asVertex().getId();

        Group g1 = graph.addGroup(type, id, groupName1,
                new GroupInheritancePermissions(true, false, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
        Group g2 = graph.addGroup(type, id, groupName2, expectedGroup1,
                new GroupInheritancePermissions(true, true, false, false, false),
                new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);

        Set<Group> gs = graph.getGroupChildren(BaseVertex.VertexType.USER, id2, expectedGroup1);
        Assert.assertTrue(gs.isEmpty());
    }

    @Test
    public void testManageEdges() throws UserNotFoundException, AccessDeniedException, VertexExistsException, InvalidVertexTypeException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String id = "User1";
        String groupName1 = "mygroup";
        BaseVertex.VertexType type = BaseVertex.VertexType.USER;
        String expectedGroup1 = Joiner.on(".").join(Group.COMMON_GROUP, groupName1);

        Object user1 = graph.addUser(type, id, "User One").asVertex().getId();
        Group g1 = graph.addGroup(type, id, groupName1, new GroupInheritancePermissions(true, false, false, false, false), new UserGroupPermissions(), true, false);


        // Right now there is only data access
        Object g1id = g1.asVertex().getId();
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_MANAGE.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_READ.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_WRITE.toString())
                .inV()
                .has("id", g1id).hasNext());

        graph.setGroupInheritance(expectedGroup1, true, true, false, true, false);
        // Now there is only data access, data read and data manage
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_MANAGE.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_READ.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_WRITE.toString())
                .inV()
                .has("id", g1id).hasNext());


        graph.setGroupInheritance(expectedGroup1, true, false, false, true, false);
        // Now there is only data access and data manage
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_MANAGE.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_READ.toString())
                .inV()
                .has("id", g1id).hasNext());
        Assert.assertFalse(new GremlinPipeline(graph.getGraph().getVertex(graph.getCommonGroupId()))
                .outE(BaseEdge.EdgeType.A_WRITE.toString())
                .inV()
                .has("id", g1id).hasNext());
    }


    @Test
    public void userPermissionHelperTest() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        String userId = "User1";

        Vertex user1  = graph.addUser(BaseVertex.VertexType.USER, userId, "User One").asVertex();
        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group1");

        Set<BaseEdge.EdgeType> expected = new HashSet<>(Arrays.asList(BaseEdge.EdgeType.values()));
        expected.remove(BaseEdge.EdgeType.COMPLEX_DATA_ACCESS);

        Set<BaseEdge.EdgeType> edges = graph.userPermissionsOnGroup(BaseVertex.VertexType.USER, userId, "root.group1");

        Assert.assertEquals(expected, edges);
    }



    /**
     * This sort of does an "explicit group membership" lookup... and gets the paths and logs them
     *
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     * @throws VertexExistsException
     * @throws InvalidVertexTypeException
     */
    @Test
    public void testMyLogic() throws UserNotFoundException, AccessDeniedException, VertexExistsException, InvalidVertexTypeException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        final Vertex user1  = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One").asVertex();
        Vertex user2  = graph.addUser(BaseVertex.VertexType.USER, "User2", "User One").asVertex();

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group1");
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group2", g1);
        Group g4 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group4", g2);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, "User1", "group3");

        graph.addUserToGroup(BaseVertex.VertexType.USER, "User2", g2, true, true, true, true, true);
        Group g5 = graph.addGroup(BaseVertex.VertexType.USER, "User2", "group5", "root.group1.group2", new GroupInheritancePermissions(true, true, true, true, true), new UserGroupPermissionsWrapper(), true, false);

        graph.setGroupInheritance(g4.asVertex(), true, false, false, false, false);
        graph.assignEdges(user1, g4.asVertex(), true, false, false, false, false);


        logger.debug("Common: {}, g1: {}, g2: {}, g3: {}, g4: {}, g5: {}", graph.commonGroupId, g1, g2, g3, g4, g5);

        final FramedGraph<TitanGraph> fg = graph.getFramedGraph();

        Set<Vertex> finalVerts = Sets.newHashSet();
        GremlinPipeline<Vertex, Vertex> groupPath = new GremlinPipeline<Vertex, Vertex>(graph.getGraph().getVertex(graph.commonGroupId))

                // Traverse all the child groups, and emit any that are active
                .as("find_the_groups")
                .outE(Group.CHILD_GROUP)
                .gather().scatter()
                .inV()
                .loop("find_the_groups", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        return true;
                    }
                }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        Group g = fg.frame(vertexLoopBundle.getObject(), Group.class);
                        return g.isActive();
                    }
                }).dedup()
                .back("find_the_groups").cast(Vertex.class)
                .enablePath()

//                // Find the path from the child group to the common group
//                .as("the_groups")
//                .enablePath()
//                .inE(Group.CHILD_GROUP)
//                .outV()
//                .loop("the_groups", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                    @Override
//                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
//                        return vertexLoopBundle.getObject().getId() != graph.commonGroupId;
//                    }
//                })

                // Find a path from the vertex to the user, along admin read
//                .back("the_groups")
//                .as("the_loop")
//                .inE(BaseEdge.EdgeType.A_READ.toString())
//                .outV()
//                //.store(finaIlVerts)
//                .loop("the_loop", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                    @Override
//                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
//                        logger.debug(":(");
//                        return !vertexLoopBundle.getObject().equals(user1);
//                    }
//                })
//                .back("the_loop").cast(Vertex.class)
//                .enablePath()
                ;

//        logger.debug("pipe has next: {}", groupPath.hasNext());
//        for (Vertex v : groupPath) {
//            logger.debug("V: {} {}", v, groupPath.getCurrentPath());
//        }

        GremlinPipeline<Vertex, List> s = groupPath.path(new PipeFunction() {
            @Override
            public Object compute(Object o) {
                return ((Vertex) o).getProperty("groupName");
            }
        }, new PipeFunction() {
            @Override
            public Object compute(Object o) {
                return ".";
            }
        });

        for (List x : s) {
            logger.debug("path: {}", x);
        }

    }


    @Test(expected=VertexNotFoundException.class)
    public void testChangeGroupNameNoChildren() throws Exception {
        try {
            String userId = "User1";
            Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, userId, "User One").asVertex();
            Group g1 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group1");

            // Change group name
            graph.changeGroupName("root.group1", "group2");

            // This should exist
            graph.getGroup("root.group2");
        } catch (VertexNotFoundException e) {
            // This exception thrown here is another error
            throw new Exception("Did not expect this error at this part of the test", e);
        }

        // This group shouldn't exist
        graph.getGroup("root.group1");
    }

    @Test(expected=VertexNotFoundException.class)
    public void testChangeGroupNameOneChild() throws Exception {
        try {
            String userId = "User1";
            Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, userId, "User One").asVertex();
            Group g1 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group1");
            Group g2 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group12", "root.group1");

            // Change group name
            graph.changeGroupName("root.group1", "group2");

            // This should exist
            Group g1p = graph.getGroup("root.group2");
            Assert.assertEquals("root.group2", g1p.getGroupName());
            Assert.assertEquals("group2", g1p.getGroupFriendlyName());

            Group g2p = graph.getGroup("root.group2.group12");
            Assert.assertEquals("root.group2.group12", g2p.getGroupName());
            Assert.assertEquals("group12", g2p.getGroupFriendlyName());

        } catch (VertexNotFoundException e) {
            // This exception thrown here is another error
            throw new Exception("Did not expect this error at this part of the test", e);
        }

        // This group shouldn't exist
        graph.getGroup("root.group1");
        graph.getGroup("root.group1.group12");
    }

    @Test(expected=VertexNotFoundException.class)
    public void testChangeGroupNameMultChild() throws Exception {
        try {
            String userId = "User1";
            Vertex user1 = graph.addUser(BaseVertex.VertexType.USER, userId, "User One").asVertex();
            Group g1 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group1");
            Group g2 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group12", "root.group1");
            Group g3 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group123", "root.group1.group12");
            Group g4 = graph.addGroup(BaseVertex.VertexType.USER, userId, "group13", "root.group1");

            // Change group name
            graph.changeGroupName("root.group1", "group2");

            // This should exist
            Group g1p = graph.getGroup("root.group2");
            Assert.assertEquals("root.group2", g1p.getGroupName());
            Assert.assertEquals("group2", g1p.getGroupFriendlyName());

            Group g2p = graph.getGroup("root.group2.group12");
            Assert.assertEquals("root.group2.group12", g2p.getGroupName());
            Assert.assertEquals("group12", g2p.getGroupFriendlyName());

            Group g3p = graph.getGroup("root.group2.group12.group123");
            Assert.assertEquals("root.group2.group12.group123", g3p.getGroupName());
            Assert.assertEquals("group123", g3p.getGroupFriendlyName());

            Group g4p = graph.getGroup("root.group2.group13");
            Assert.assertEquals("root.group2.group13", g4p.getGroupName());
            Assert.assertEquals("group13", g4p.getGroupFriendlyName());

        } catch (VertexNotFoundException e) {
            // This exception thrown here is another error
            throw new Exception("Did not expect this error at this part of the test", e);
        }

        // This group shouldn't exist
        graph.getGroup("root.group1");
        graph.getGroup("root.group1.group12");
        graph.getGroup("root.group1.group12.group123");
        graph.getGroup("root.group1.group13");
    }

}
