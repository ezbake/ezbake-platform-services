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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.*;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.TitanGraphConfiguration;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.AccessDeniedException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.*;
import ezbake.security.test.MockEzSecurityToken;

import org.apache.thrift.TException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * User: jhastings
 * Date: 6/16/14
 * Time: 11:56 AM
 */
public class EzGroupsServiceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    static String adminId = "Jeff";

    EzGroupsService service;
    EzSecurityToken adminToken;
    EzSecurityToken adminAppToken;

    @Before
    public void setUpService() throws StorageException, EzConfigurationLoaderException, AuthorizationException, EzGroupOperationException, EzSecurityTokenException {
        Properties p = new EzConfiguration(new ClasspathConfigurationLoader("/test.properties", "/graphconfig.properties")).getProperties();
        p.setProperty("storage.directory", folder.getRoot().toString());
        p.setProperty(EzGroupsService.X509_RESTRICT, Boolean.FALSE.toString());

        EzGroupsTestModule module = new EzGroupsTestModule(p);

        GraphDatabaseConfiguration conf = new GraphDatabaseConfiguration(new TitanGraphConfiguration(p));
        conf.getBackend().clearStorage();

        Injector injector = Guice.createInjector(module);
        service = injector.getInstance(EzGroupsService.class);

        // Create the admin user and token
        adminToken = MockEzSecurityToken.getMockUserToken(adminId, "", Sets.<String>newHashSet(),
                Maps.<String, List<String>>newHashMap(), true);
        adminAppToken = MockEzSecurityToken.getBlankToken(null, null, 0);
        adminAppToken.setType(TokenType.APP);
        MockEzSecurityToken.populateAppInfo(adminAppToken, null, null);

        service.createUser(adminToken, adminToken.getTokenPrincipal().getPrincipal(), adminId);
        service.createAppUser(adminToken, adminAppToken.getTokenPrincipal().getPrincipal(), adminAppToken.getTokenPrincipal().getPrincipal());
    }
    @After
    public void shutDown() throws IOException {
        if (service != null) {
            service.getGraph().close();
        }
    }


    @Test
    public void testCreateGroup() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException {
        service.createGroup(adminAppToken, "", "jeffs_cool_group", new GroupInheritancePermissions());
    }

    /******************       User Tests          *************************/
    @Test
    public void testAddUserAsAdmin() throws TException {
        service.createUser(adminToken, "Glenn", "Glenn Rhee");
    }

    @Test
    public void testAddUserAndGetAuths() throws TException {
        String user = "Hershel";
        Set<Long> auths = service.createUserAndGetAuthorizations(adminToken, null, user, null);
        // size of 2 - root and user's id
        Assert.assertEquals(2, auths.size());
    }

    @Test
    public void testAddUserAndGetAuthsUserExists() throws TException {
        String user = "Hershel";
        service.createUser(adminToken, user, null);
        Set<Long> auths = service.createUserAndGetAuthorizations(adminToken, null, user, null);
        // size of 2 - root and user's id
        Assert.assertEquals(2, auths.size());
    }

    @Test
    public void testModifyUserPrincipal() throws TException {
        String user = "Matthew";
        String user2 = "Matthew2";

        service.createUser(adminToken, user, "");
        service.modifyUser(adminToken, user, user2);

        // Assert old user doesn't exist
        Assert.assertFalse(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, user)
                .vertices().iterator().hasNext());

        // Assert new user does exist
        Assert.assertTrue(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, user2)
                .vertices().iterator().hasNext());
    }

    @Test
    public void testDeactivateUser() throws TException {
        String user = "Matthew";
        service.createUser(adminToken, user, "");
        service.deactivateUser(adminToken, user);

        // Make sure user is not active
        Iterator<Vertex> iv = service.getGraph().getGraph().query().has(User.PRINCIPAL, user).limit(1).has(BaseVertex.ACTIVE, false).vertices().iterator();
        Assert.assertTrue(iv.hasNext());
    }

    @Test
    public void testActivateUser() throws TException {
        String user = "Matthew";
        service.createUser(adminToken, user, "");
        service.deactivateUser(adminToken, user);

        // Make sure user is not active
        Iterator<Vertex> iv = service.getGraph().getGraph().query().has(User.PRINCIPAL, user).limit(1).has(BaseVertex.ACTIVE, false).vertices().iterator();
        Assert.assertTrue(iv.hasNext());

        // Activate and verify
        service.activateUser(adminToken, user);
        iv = service.getGraph().getGraph().query().has(User.PRINCIPAL, user).limit(1).has(BaseVertex.ACTIVE, true).vertices().iterator();
        Assert.assertTrue(iv.hasNext());
    }

    @Test
    public void testDeleteUser() throws TException {
        String user = "Matthew";
        service.createUser(adminToken, user, "");

        // Make sure user exists
        Iterator<Vertex> iv = service.getGraph().getGraph().query().has(User.PRINCIPAL, user).limit(1).vertices().iterator();
        Assert.assertTrue(iv.hasNext());

        service.deleteUser(adminToken, user);

        // Make sure user does not exists
        iv = service.getGraph().getGraph().query().has(User.PRINCIPAL, user).limit(1).vertices().iterator();
        Assert.assertFalse(iv.hasNext());
    }

    @Test
    public void checkUserAccessToGroup() throws TException {
        String user = "Rick";
        EzSecurityToken userToken = MockEzSecurityToken.getBlankToken(null, null, 0);
        MockEzSecurityToken.populateUserInfo(userToken, user, null, null);
        service.createUser(adminToken, user, "");

        String otherGroup1 = "other1";
        String otherGroup2 = "other2";
        String groupWithAccess = "group1";
        String onlyRequireAppGroup = "onlyrequireapp";
        String onlyRequireUserGroup = "onlyrequireuser";

        // Create these groups as the admin app so the app has access but the user does not
        service.createGroup(adminAppToken, null, otherGroup1, new GroupInheritancePermissions());
        Assert.assertFalse(service.checkUserAccessToGroup(userToken, otherGroup1));
        service.createGroup(adminAppToken, null, otherGroup2, new GroupInheritancePermissions());
        Assert.assertFalse(service.checkUserAccessToGroup(userToken, otherGroup2));

        // Create this group as the app user and only require app so the user gets it
        service.createGroupWithInclusion(adminAppToken, null, onlyRequireAppGroup, new GroupInheritancePermissions(), false, true);
        Assert.assertTrue(service.checkUserAccessToGroup(userToken, onlyRequireAppGroup));

        // Create this group as the app user so it has access, then add the user to it
        service.createGroup(adminAppToken, null, groupWithAccess, new GroupInheritancePermissions());
        service.addUserToGroup(adminAppToken, groupWithAccess, user, new UserGroupPermissions());
        Assert.assertTrue(service.checkUserAccessToGroup(userToken, groupWithAccess));

        // Create this group as the regular admin with only require user and then add the user to it
        service.createGroupWithInclusion(adminToken, null, onlyRequireUserGroup, new GroupInheritancePermissions(), true, false);
        service.addUserToGroup(adminToken, onlyRequireUserGroup, user, new UserGroupPermissions());
        Assert.assertTrue(service.checkUserAccessToGroup(userToken, onlyRequireUserGroup));
        Assert.assertFalse(service.checkUserAccessToGroup(adminAppToken, onlyRequireUserGroup));

        String og1g2 = "group2";
        service.createGroup(adminAppToken, otherGroup1, og1g2, new GroupInheritancePermissions());
        Assert.assertTrue(!service.checkUserAccessToGroup(userToken, otherGroup1+EzGroupsConstants.GROUP_NAME_SEP+og1g2));
    }

    @Test(expected=GroupQueryException.class)
    public void checkUserAccessGroupDoesntExist() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException, GroupQueryException {
        String user = "Rick";
        EzSecurityToken usertoken = MockEzSecurityToken.getMockUserToken(user);
        service.createUser(adminToken, user, "");

        String groupName = "GROUP";
        // Check access on group that does not exist
        service.checkUserAccessToGroup(usertoken, groupName);
    }


    /******************          App User Tests          *************************/

    @Test
    public void testCreateAppUser() throws TException {
        // Create a regular app user
        String securityId = "938522";
        String appName = "App 1";
        String expectedAppGroupName = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, Group.APP_GROUP, appName);
        String expectedAppAccessGroupName = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, Group.APP_ACCESS_GROUP, appName);

        service.createAppUser(adminToken, securityId, appName);

        Graph g = service.getGraph().getGraph();
        // Very naieve tests
        // Get the app user's vertex
        Iterator<Vertex> appUserVertices = g.query().has(User.PRINCIPAL, securityId).vertices().iterator();
        Vertex appUser = appUserVertices.next();
        Assert.assertFalse(appUserVertices.hasNext());

        // Get the app group vertex
        Iterator<Vertex> appGroupVertices = g.query().has(Group.GROUP_NAME, expectedAppGroupName).vertices().iterator();
        Assert.assertTrue(appGroupVertices.hasNext());
        Vertex appGroup = appGroupVertices.next();

        // Get the app access group vertex
        Iterator<Vertex> appAccessGroupVertices = g.query().has(Group.GROUP_NAME, expectedAppAccessGroupName).vertices().iterator();
        Assert.assertTrue(appAccessGroupVertices.hasNext());
        Vertex appAccessGroup = appAccessGroupVertices.next();

        // Check for the app group Data Access edges
        GremlinPipeline<Object, Vertex> appGroupPipe = new GremlinPipeline<>(appUser).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().as("ag").has(Group.GROUP_NAME, expectedAppGroupName).back("ag").cast(Vertex.class);
        Assert.assertTrue(appGroupPipe.hasNext());
        Assert.assertEquals(expectedAppGroupName, appGroupPipe.next().getProperty(Group.GROUP_NAME));

        GremlinPipeline<Object, Vertex> appAccessGroupPipe = new GremlinPipeline<>(appUser).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().as("ag").has(Group.GROUP_NAME, expectedAppAccessGroupName).back("ag").cast(Vertex.class);
        Assert.assertTrue(appAccessGroupPipe.hasNext());
        Assert.assertEquals(expectedAppAccessGroupName, appAccessGroupPipe.next().getProperty(Group.GROUP_NAME));
    }

    @Test
    public void appUserGetsAuditsGroup() throws TException {
        String appId = "app1";
        String appName = "Application One";
        service.createAppUser(adminToken, appId, appName);

        String appGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP, appName);
        String auditsGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP,
                appName, EzGroupsConstants.AUDIT_GROUP);

        // Get the audits group
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(appId);
        Set<ezbake.groups.thrift.Group> groups = service.getChildGroups(appToken, appGroup, false);

        // Assert that the audits group is in the list
        Set<String> receivedGroupNames = Sets.newHashSet();
        for (ezbake.groups.thrift.Group g : groups) {
            receivedGroupNames.add(g.getGroupName());
        }
        Assert.assertTrue(receivedGroupNames.contains(auditsGroup));


        // Now make sure the app user only has admin permissions on it

        Graph graph = service.getGraph().getGraph();
        Vertex auditsGroupV = graph.query()
                .has(Group.GROUP_NAME, Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                        .join(EzGroupsConstants.ROOT, auditsGroup))
                .vertices()
                .iterator().next();

        // Make sure audit group's parent is the apps group
        Iterator<Edge> e = graph.getVertex(auditsGroupV).query()
                .direction(Direction.IN)
                .labels(Group.CHILD_GROUP)
                .edges().iterator();
        if (e.hasNext()) {
            Vertex parent = e.next().getVertex(Direction.OUT);
            Assert.assertEquals(
                    Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, appGroup),
                    parent.getProperty(Group.GROUP_NAME));
        } else {
            Assert.fail();
        }

        // Make sure app user has all the edges on the app group
        Object auditsGroupId = auditsGroupV.getId();
        Vertex app = graph.query()
                .has(User.PRINCIPAL, appId)
                .has(User.TYPE, BaseVertex.VertexType.APP_USER.toString())
                .vertices().iterator().next();

        Assert.assertFalse(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", auditsGroupId).hasNext());
    }

    @Test
    public void appUserGetsMetricsGroup() throws TException {
        String appId = "app1";
        String appName = "Application One";
        service.createAppUser(adminToken, appId, appName);

        String appGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP, appName);
        String metricsGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP,
                appName, EzGroupsConstants.METRICS_GROUP);

        // Get the metrics group
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(appId);
        Set<ezbake.groups.thrift.Group> groups = service.getChildGroups(appToken, appGroup, false);

        // Assert that the audits group is in the list
        Set<String> receivedGroupNames = Sets.newHashSet();
        for (ezbake.groups.thrift.Group g : groups) {
            receivedGroupNames.add(g.getGroupName());
        }
        Assert.assertTrue(receivedGroupNames.contains(metricsGroup));


        // Now make sure the app user has the right permissions on it
        Graph graph = service.getGraph().getGraph();
        Vertex auditsGroupV = graph.query()
                .has(Group.GROUP_NAME, Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                        .join(EzGroupsConstants.ROOT, metricsGroup))
                .vertices()
                .iterator().next();

        // Make sure audit group's parent is the apps group
        Iterator<Edge> e = graph.getVertex(auditsGroupV).query()
                .direction(Direction.IN)
                .labels(Group.CHILD_GROUP)
                .edges().iterator();
        if (e.hasNext()) {
            Vertex parent = e.next().getVertex(Direction.OUT);
            Assert.assertEquals(
                    Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, appGroup),
                    parent.getProperty(Group.GROUP_NAME));
        } else {
            Assert.fail();
        }

        // Make sure app user has all the edges on the app group
        Object auditsGroupId = auditsGroupV.getId();
        Vertex app = graph.query()
                .has(User.PRINCIPAL, appId)
                .has(User.TYPE, BaseVertex.VertexType.APP_USER.toString())
                .vertices().iterator().next();

        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", auditsGroupId).hasNext());
    }

    @Test
    public void appUserGetsDiagnosticGroup() throws TException {
        String appId = "app1";
        String appName = "Application One";
        service.createAppUser(adminToken, appId, appName);

        String appGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP, appName);
        String diagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP,
                appName, EzGroupsConstants.DIAGNOSTICS_GROUP);

        // Get the metrics group
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(appId);
        Set<ezbake.groups.thrift.Group> groups = service.getChildGroups(appToken, appGroup, false);

        // Assert that the audits group is in the list
        Set<String> receivedGroupNames = Sets.newHashSet();
        for (ezbake.groups.thrift.Group g : groups) {
            receivedGroupNames.add(g.getGroupName());
        }
        Assert.assertTrue(receivedGroupNames.contains(diagnosticGroup));


        // Now make sure the app user has the right permissions on it
        Graph graph = service.getGraph().getGraph();
        Vertex auditsGroupV = graph.query()
                .has(Group.GROUP_NAME, Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                        .join(EzGroupsConstants.ROOT, diagnosticGroup))
                .vertices()
                .iterator().next();

        // Make sure audit group's parent is the apps group
        Iterator<Edge> e = graph.getVertex(auditsGroupV).query()
                .direction(Direction.IN)
                .labels(Group.CHILD_GROUP)
                .edges().iterator();
        if (e.hasNext()) {
            Vertex parent = e.next().getVertex(Direction.OUT);
            Assert.assertEquals(
                    Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, appGroup),
                    parent.getProperty(Group.GROUP_NAME));
        } else {
            Assert.fail();
        }

        // Make sure app user has all the edges on the app group
        Object auditsGroupId = auditsGroupV.getId();
        Vertex app = graph.query()
                .has(User.PRINCIPAL, appId)
                .has(User.TYPE, BaseVertex.VertexType.APP_USER.toString())
                .vertices().iterator().next();

        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", auditsGroupId).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getVertex(app)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", auditsGroupId).hasNext());
    }

    @Test
    public void testModifyAppUserPrincipal() throws TException {
        String securityId1 = "938522";
        String securityId2 = "938524";

        service.createAppUser(adminToken, securityId1, securityId1);

        // Make sure user exists
        Assert.assertTrue(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId1)
                .vertices().iterator().hasNext());

        service.modifyAppUser(adminToken, securityId1, securityId2, null);

        // Assert old user doesn't exist
        Assert.assertFalse(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId1)
                .vertices().iterator().hasNext());

        // Assert new user does exist
        Assert.assertTrue(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId2)
                .vertices().iterator().hasNext());
    }

    @Test
    public void testModifyAppUserName() throws TException, VertexNotFoundException {
        String securityId = "938522";
        String name = "NAME";
        String name2 = "name";

        service.createAppUser(adminToken, securityId, name);

        // Make sure user exists
        Assert.assertTrue(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .vertices().iterator().hasNext());

        service.modifyAppUser(adminToken, securityId, securityId, name2);

        // Assert new user does exist
        Assert.assertTrue(service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .vertices().iterator().hasNext());

        // Make sure group names changed
        GroupNameHelper gnh = new GroupNameHelper();
        Group a = service.getGraph().getGroup(gnh.getNamespacedAppGroup(name2));
        Assert.assertEquals(gnh.getNamespacedAppGroup(name2), a.getGroupName());
        Assert.assertEquals(name2, a.getGroupFriendlyName());

        Group aa = service.getGraph().getGroup(gnh.getNamespacedAppAccessGroup(name2));
        Assert.assertEquals(gnh.getNamespacedAppAccessGroup(name2), aa.getGroupName());
        Assert.assertEquals(name2, aa.getGroupFriendlyName());
    }

    @Test
    public void testDeactivateAppUser() throws TException {
        String securityID = "12345";
        service.createAppUser(adminToken, securityID, securityID);
        service.deactivateAppUser(adminToken, securityID);

        // Make sure user is not active
        Iterator<Vertex> iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityID)
                .has(BaseVertex.ACTIVE, false)
                .limit(1)
                .vertices().iterator();
        Assert.assertTrue(iv.hasNext());
    }

    @Test
    public void testActivateAppUser() throws TException {
        String securityId = "App";
        service.createAppUser(adminToken, securityId, securityId);
        service.deactivateAppUser(adminToken, securityId);

        // Make sure user is not active
        Iterator<Vertex> iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .has(BaseVertex.ACTIVE, false)
                .limit(1)
                .vertices().iterator();
        Assert.assertTrue(iv.hasNext());

        // Activate and verify
        service.activateAppUser(adminToken, securityId);
        iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .has(BaseVertex.ACTIVE, true)
                .limit(1)
                .vertices().iterator();
        Assert.assertTrue(iv.hasNext());
    }


    @Test
     public void testDeleteAppUser() throws TException, VertexNotFoundException {
        String securityId = "GIMML";
        service.createAppUser(adminToken, securityId, "GIMML");

        // Make sure user exists
        Iterator<Vertex> iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .limit(1)
                .vertices().iterator();
        Assert.assertTrue(iv.hasNext());

        service.deleteAppUser(adminToken, securityId);

        // Make sure user does not exists
        iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .limit(1)
                .vertices().iterator();
        Assert.assertFalse(iv.hasNext());


        // Lookup the renamed app groups
        Group appGroup = service.getGraph()
                .getGroup(new GroupNameHelper().getNamespacedAppGroup("_DELETED_APP_" + securityId));
        Group accessGroup = service.getGraph()
                .getGroup(new GroupNameHelper().getNamespacedAppAccessGroup("_DELETED_APP_" + securityId));
    }

    @Test
    public void testDeleteAppUserByAdmin() throws TException, VertexNotFoundException {
        String securityId = "GIMML";
        service.createAppUser(adminToken, securityId, securityId);

        // Add an admin
        String user = "Hershel";
        service.createUser(adminToken, user, null);
        service.addUserToGroup(adminToken, new GroupNameHelper().getNamespacedAppGroup(securityId), user,
                new UserGroupPermissions().setDataAccess(true).setAdminManage(true));

        service.deleteAppUser(MockEzSecurityToken.getMockUserToken(user), securityId);

        // Make sure user does not exists
        Iterator<Vertex> iv = service.getGraph().getGraph().query()
                .has(User.PRINCIPAL, securityId)
                .limit(1)
                .vertices().iterator();
        Assert.assertFalse(iv.hasNext());


        // Lookup the renamed app groups
        Group appGroup = service.getGraph()
                .getGroup(new GroupNameHelper().getNamespacedAppGroup("_DELETED_APP_" + securityId));
        Group accessGroup = service.getGraph()
                .getGroup(new GroupNameHelper().getNamespacedAppAccessGroup("_DELETED_APP_" + securityId));
    }

    /******************          Create Group Tests              *************************/
    @Test
    public void testCreateNoParent() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        String groupName = "Jeff's Group";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        service.createGroup(userToken, null, groupName, new GroupInheritancePermissions(true, true, true, true, true));

        // Look for the group
        String expectedGroupName = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName);
        FramedGraph<TitanGraph> g = service.getGraph().getFramedGraph();
        Group groupV = g.query().has(Group.GROUP_NAME, expectedGroupName).vertices(Group.class).iterator().next();

        Assert.assertEquals(expectedGroupName, groupV.getGroupName());

        // TODO: verify edges
    }

    @Test
    public void testCreateWithParent() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        String groupName1 = "Jeff's Group";
        String groupName2 = "The Subgroup";

        String expectedGroupName1 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName1);
        String expectedGroupName2 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName1, groupName2);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName1, groupName2, new GroupInheritancePermissions(true, true, true, true, true));

        // Look for the groups - RAW graph query
        FramedGraph<TitanGraph> g = service.getGraph().getFramedGraph();
        Group groupV1 = g.query().has(Group.GROUP_NAME, expectedGroupName1).vertices(Group.class).iterator().next();
        Group groupV2= g.query().has(Group.GROUP_NAME, expectedGroupName2).vertices(Group.class).iterator().next();

        Assert.assertEquals(expectedGroupName1, groupV1.getGroupName());
        Assert.assertEquals(expectedGroupName2, groupV2.getGroupName());

        // TODO: verify edges
    }

    @Test
    public void testDeactivateGroup() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        String groupName1 = "Jeff's Group";

        String expectedGroupName1 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName1);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));

        service.deactivateGroup(userToken, groupName1, false);

        // Get the group vertex and check it's deactivated
        Vertex v = service.getGraph().getGraph().query().has(Group.GROUP_NAME, expectedGroupName1).vertices().iterator().next();
        Assert.assertFalse((Boolean) v.getProperty(Group.ACTIVE));
    }

    @Test(expected=AuthorizationException.class)
    public void testDeactivateGroupAdminManage() throws TException {
        // Create a regular user
        String userId = "Bryan";
        String userId2 = "Stan";
        service.createUser(adminToken, userId, "");
        service.createUser(adminToken, userId2, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        String groupName1 = "Jeff's Group";

        String expectedGroupName1 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName1);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        userToken = MockEzSecurityToken.getMockUserToken(userId2);
        service.deactivateGroup(userToken, groupName1, false);

        // Get the group vertex and check it's deactivated
        Vertex v = service.getGraph().getGraph().query().has(Group.GROUP_NAME, expectedGroupName1).vertices().iterator().next();
        Assert.assertFalse((Boolean)v.getProperty(Group.ACTIVE));
    }

    @Test
    public void testActivateGroup() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        String groupName1 = "Jeff's Group";

        String expectedGroupName1 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(Group.COMMON_GROUP, groupName1);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));

        service.deactivateGroup(userToken, groupName1, false);
        service.activateGroup(userToken, groupName1, false);

        // Get the group vertex and check it's deactivated
        Vertex v = service.getGraph().getGraph().query().has(Group.GROUP_NAME, expectedGroupName1).vertices().iterator().next();
        Assert.assertTrue((Boolean) v.getProperty(Group.ACTIVE));
    }

    @Test
    public void testGetGroupsMask() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        String groupName1 = "Group1";
        String groupName2 = "Group2";
        String groupName21 = "Group2_1";
        String groupName22 = "Group2_2";
        String groupName3 = "Group3";

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, null, groupName2, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName2, groupName21, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName2, groupName22, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, null, groupName3, new GroupInheritancePermissions(true, true, true, true, true));

        String realG1 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, groupName1);
        String realG2 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, groupName2);
        String realG21 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, groupName2, groupName21);
        String realG22 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, groupName2, groupName22);
        String realG3 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, groupName3);

        // Get the group indices
        Long g1Index = service.getGraph().getGraph().query().has(Group.GROUP_NAME, realG1).vertices().iterator().next().getProperty(Group.INDEX);
        Long g2Index = service.getGraph().getGraph().query().has(Group.GROUP_NAME, realG2).vertices().iterator().next().getProperty(Group.INDEX);
        Long g21Index = service.getGraph().getGraph().query().has(Group.GROUP_NAME, realG21).vertices().iterator().next().getProperty(Group.INDEX);
        Long g22Index = service.getGraph().getGraph().query().has(Group.GROUP_NAME, realG22).vertices().iterator().next().getProperty(Group.INDEX);
        Long g3Index = service.getGraph().getGraph().query().has(Group.GROUP_NAME, realG3).vertices().iterator().next().getProperty(Group.INDEX);

        Set<Long> expectedIndices = Sets.newTreeSet(Lists.newArrayList(g2Index, g21Index, g22Index));
        Set<Long> receivedIndices = service.getGroupsMask(userToken, Sets.newHashSet(
                groupName2,
                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName2, groupName21),
                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName2, groupName22)));

        Assert.assertEquals(expectedIndices, receivedIndices);
    }

    /******************       Get Child Group Members Tests       *************************/
    @Test
    public void testGetChildGroups() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";
        String groupName2 = "The First Subgroup";
        String groupName3 = "The Second Subgroup";
        String groupName4 = "The Third Subgroup";

        String expectedGroupName2 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName2);
        String expectedGroupName3 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName3);
        String expectedGroupName4 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(expectedGroupName3, groupName4);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName1, groupName2, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName1, groupName3, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, expectedGroupName3, groupName4, new GroupInheritancePermissions(true, true, true, true, true));

        Set<String> expected = Sets.newHashSet(expectedGroupName2, expectedGroupName3);
        Set<ezbake.groups.thrift.Group> childGroups = service.getChildGroups(userToken, groupName1, false);
        Set<String> receivedGroups = Sets.newHashSet();
        for (ezbake.groups.thrift.Group r : childGroups) {
            receivedGroups.add(r.getGroupName());
        }

        Assert.assertFalse(receivedGroups.contains(expectedGroupName4));
        Assert.assertEquals(expected, receivedGroups);
    }


    @Test
    public void testGetChildrenOfRoot() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";
        String groupName2 = "The First Subgroup";
        String groupName3 = "The Second Subgroup";
        String groupName4 = "The Third Subgroup";

        String expectedGroupName2 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName2);
        String expectedGroupName3 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName3);
        String expectedGroupName4 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(expectedGroupName3, groupName4);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, null, groupName2, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, null, groupName3, new GroupInheritancePermissions(true, true, true, true, true));

        Set<String> expected = Sets.newHashSet(groupName1, groupName2, groupName3);
        Set<ezbake.groups.thrift.Group> childGroups = service.getChildGroups(userToken, "", false);
        Set<String> receivedGroups = Sets.newHashSet();
        for (ezbake.groups.thrift.Group r : childGroups) {
            receivedGroups.add(r.getGroupName());
        }

        Assert.assertFalse(receivedGroups.contains(expectedGroupName4));
        Assert.assertEquals(expected, receivedGroups);
    }


    @Test
    public void testGetChildGroupsRecurse() throws TException {
        // Create a regular user
        String userId = "Bryan";
        service.createUser(adminToken, userId, "");

        // Create a group as a regular user, with the default inheritance
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";
        String groupName2 = "The First Subgroup";
        String groupName3 = "The Second Subgroup";
        String groupName4 = "The Third Subgroup";

        String expectedGroupName2 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName2);
        String expectedGroupName3 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName3);
        String expectedGroupName4 = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(groupName1, groupName3, groupName4);

        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions(true, true, true, true, true));
        service.createGroup(userToken, groupName1, groupName2, new GroupInheritancePermissions(true, true, false, false, false));
        service.createGroup(userToken, groupName1, groupName3, new GroupInheritancePermissions(true, false, true, true, true));
        service.createGroup(userToken, expectedGroupName3, groupName4, new GroupInheritancePermissions(true, true, false, true, true));

        Set<String> expected = Sets.newHashSet(expectedGroupName2, expectedGroupName3, expectedGroupName4);
        Set<ezbake.groups.thrift.Group> childGroups = service.getChildGroups(userToken, groupName1, true);
        Set<String> received = Sets.newHashSet();
        for (ezbake.groups.thrift.Group g : childGroups) {
            received.add(g.getGroupName());
        }
        Assert.assertEquals(expected, received);
    }


    /******************          Add Group Members Tests          *************************/
    @Test
    public void testAddUsers() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");

        // user 1 create some groups
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";

        GroupInheritancePermissions ih = new GroupInheritancePermissions();
        service.createGroup(userToken, null, groupName1, ih);

        Set<String> expected = Sets.newHashSet(userId1);
        Set<String> users = service.getGroupUsers(userToken, groupName1, false);
        Assert.assertEquals(expected, users);

        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        Set<String> expectedAfterAdd = Sets.newHashSet(userId1, userId2);
        Set<String> usersAfterAdd = service.getGroupUsers(userToken, groupName1, false);
        Assert.assertEquals(expectedAfterAdd, usersAfterAdd);

        // Enforce that users can't be same before and after adding users
        Assert.assertNotEquals(users, usersAfterAdd);
    }

    @Test
    public void testRemoveUsers() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");

        // user 1 create some groups
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";

        GroupInheritancePermissions ih = new GroupInheritancePermissions();
        service.createGroup(userToken, null, groupName1, ih);

        Set<String> expected = Sets.newHashSet(userId1);
        Set<String> users = service.getGroupUsers(userToken, groupName1, false);
        Assert.assertEquals(expected, users);

        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        Set<String> expectedAfterAdd = Sets.newHashSet(userId1, userId2);
        Set<String> usersAfterAdd = service.getGroupUsers(userToken, groupName1, false);
        Assert.assertEquals(expectedAfterAdd, usersAfterAdd);

        // Enforce that users can't be same before and after adding users
        Assert.assertNotEquals(users, usersAfterAdd);

        // Remove
        service.removeUserFromGroup(userToken, groupName1, userId2);
        expected = Sets.newHashSet(userId1);
        users = service.getGroupUsers(userToken, groupName1, false);
        Assert.assertEquals(expected, users);
    }

    @Test
    public void addAppUserToGroupCreatingIfNeeded() throws TException {
        String userId1 = "Bryan";
        service.createUser(adminToken, userId1, "");
        // user 1 create some groups
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);
        String groupName1 = "Jeff's Group";
        GroupInheritancePermissions ih = new GroupInheritancePermissions();
        service.createGroup(userToken, null, groupName1, ih);


        String userId2 = "Ryan";
        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());
        // would throw if it didn't create Ryan
    }

    @Test
    public void addAppUserToRegularGroup() throws TException {
        // Create users
        String userId1 = "Bryan";
        String appId = "App1234";
        String appName = "App1234 Name";
        service.createUser(adminToken, userId1, "");
        service.createAppUser(adminToken, appId, appName);

        // user 1 create some groups
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        // Add the app user to the group
        service.addAppUserToGroup(userToken, groupName1, appId, new UserGroupPermissions());

        AllGroupMembers groupMembers = service.getGroupMembers(userToken, groupName1, false);
        Assert.assertEquals(1, groupMembers.getUsersSize());
        Assert.assertEquals(1, groupMembers.getAppsSize());

        Assert.assertTrue(groupMembers.getUsers().contains(userId1));
        Assert.assertTrue(groupMembers.getApps().contains(appId));
    }

    @Test
    public void removeAppUserFromRegularGroup() throws TException {
        // Create users
        String userId1 = "Bryan";
        String appId = "App1234";
        String appName = "App1234 Name";
        service.createUser(adminToken, userId1, "");
        service.createAppUser(adminToken, appId, appName);

        // user 1 create some groups
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);

        String groupName1 = "Jeff's Group";
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        // Add the app user to the group
        service.addAppUserToGroup(userToken, groupName1, appId, new UserGroupPermissions());

        AllGroupMembers groupMembers = service.getGroupMembers(userToken, groupName1, false);
        Assert.assertEquals(1, groupMembers.getUsersSize());
        Assert.assertEquals(1, groupMembers.getAppsSize());

        Assert.assertTrue(groupMembers.getUsers().contains(userId1));
        Assert.assertTrue(groupMembers.getApps().contains(appId));

        service.removeAppUserFromGroup(userToken, groupName1, appId);
        groupMembers = service.getGroupMembers(userToken, groupName1, false);
        Assert.assertEquals(1, groupMembers.getUsersSize());
        Assert.assertEquals(0, groupMembers.getAppsSize());
    }


    @Test(expected=AuthorizationException.class)
    @Ignore("Ignoring this for now. When not using x509 restrict all users can have this permission... bad I know")
    public void addUserToGroupAdderDoesntExist() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException {
        String groupName1 = "Jeff's Group";
        service.createGroup(adminToken, null, groupName1, new GroupInheritancePermissions());

        String notInGroups = "Userrrr";
        String addToGroupUser = "AddMePlease";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(notInGroups);

        service.addUserToGroup(userToken, groupName1, addToGroupUser, new UserGroupPermissions());
    }

    @Test(expected=AuthorizationException.class)
    public void addAppUserToGroupAdderDoesntExist() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException {
        String groupName1 = "Jeff's Group";
        service.createGroup(adminToken, null, groupName1, new GroupInheritancePermissions());

        String notInGroups = "Userrrr";
        String addToGroupUser = "AddMePlease";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(notInGroups);

        service.addAppUserToGroup(userToken, groupName1, addToGroupUser, new UserGroupPermissions());
    }


    @Test(expected=AuthorizationException.class)
    @Ignore("Ignoring this for now. When not using x509 restrict all users can have this permission... bad I know")
    public void addUsersRequiresAdminWrite() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        String userId3 = "Cyan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");
        service.createUser(adminToken, userId3, "");

        // user 1 create some groups
        String groupName1 = "Jeff's Group";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        // Add one user to it
        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        // second user try to add a third
        userToken = MockEzSecurityToken.getMockUserToken(userId2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.addUserToGroup(userToken, groupName1, userId3, new UserGroupPermissions());
    }

    @Test
    public void addUsersRequiresWorksWithAdminWrite() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        String userId3 = "Cyan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");
        service.createUser(adminToken, userId3, "");

        // user 1 create some groups
        String groupName1 = "Jeff's Group";
        EzSecurityToken userToken1 = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken1, null, groupName1, new GroupInheritancePermissions());

        // Add one user to it
        service.addUserToGroup(userToken1, groupName1, userId2, new UserGroupPermissions().setAdminWrite(true));

        // second user try to add a third
        EzSecurityToken userToken2 = MockEzSecurityToken.getMockUserToken(userId2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.addUserToGroup(userToken2, groupName1, userId3, new UserGroupPermissions());

        AllGroupMembers members = service.getGroupMembers(userToken1, groupName1, true);
        System.out.println(members);
    }

    @Test(expected=AuthorizationException.class)
    public void removeUsersRequiresAdminWrite() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        String userId3 = "Cyan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");
        service.createUser(adminToken, userId3, "");

        // user 1 create some groups
        String groupName1 = "Jeff's Group";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        // Add one user to it
        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        // second user try to add a third
        userToken = MockEzSecurityToken.getMockUserToken(userId2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.removeUserFromGroup(userToken, groupName1, userId1);
    }

    @Test(expected=AuthorizationException.class)
    public void addAppUsersRequiresAdminWrite() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        String securityId = "Cyan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");
        service.createAppUser(adminToken, securityId, securityId);

        // user 1 create some groups
        String groupName1 = "Jeff's Group";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        // Add one user to it
        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());

        // second user try to add a third
        userToken = MockEzSecurityToken.getMockUserToken(userId2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.addAppUserToGroup(userToken, groupName1, securityId, new UserGroupPermissions());
    }

    @Test(expected=AuthorizationException.class)
    public void removeAppUsersRequiresAdminWrite() throws TException {
        // Create users
        String userId1 = "Bryan";
        String userId2 = "Ryan";
        String securityId = "Cyan";
        service.createUser(adminToken, userId1, "");
        service.createUser(adminToken, userId2, "");
        service.createAppUser(adminToken, securityId, securityId);

        // user 1 create some groups
        String groupName1 = "Jeff's Group";
        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId1, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.createGroup(userToken, null, groupName1, new GroupInheritancePermissions());

        service.addUserToGroup(userToken, groupName1, userId2, new UserGroupPermissions());
        service.addAppUserToGroup(userToken, groupName1, securityId, new UserGroupPermissions());

        // second user try to add a third
        userToken = MockEzSecurityToken.getMockUserToken(userId2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        service.removeAppUserFromGroup(userToken, groupName1, securityId);
    }


    /******************          Get Group Members Tests          *************************/
    @Test
    public void testGetGroupUsers() throws TException {
        // Add a couple users
        service.createUser(adminToken, "Jim", "Jim's Name");
        service.createUser(adminToken, "Jerry", "Jerry's Name");

        // Create a new group
        GroupInheritancePermissions gi = new GroupInheritancePermissions(true, true, true, true, true);
        service.createGroup(adminToken, null, "Jeff's Group", gi);

        Set<String> users = service.getGroupUsers(adminToken, "Jeff's Group", false);
        Set<String> expectedUsers = Sets.newHashSet(adminId, "Jim", "Jerry");
        Assert.assertEquals(users, expectedUsers);
    }

    @Test
    public void testGetGroupUsersNoInheritance() throws TException {
        // Add a couple users
        service.createUser(adminToken, "Jim", "Jim's Name");
        service.createUser(adminToken, "Jerry", "Jerry's Name");

        // Create a new group
        GroupInheritancePermissions gi = new GroupInheritancePermissions(false, true, true, true, true);
        service.createGroup(adminToken, null, "Jeff's Group", gi);

        Set<String> users = service.getGroupUsers(adminToken, "Jeff's Group", false);
        Set<String> expectedUsers = Sets.newHashSet(adminId);

        Assert.assertEquals(users, expectedUsers);
    }

    @Test
    public void testGetGroupUsersMatchesGetGroupMembers() throws TException {
        // Add a couple users
        service.createUser(adminToken, "Jim", "Jim's Name");
        service.createUser(adminToken, "Jerry", "Jerry's Name");

        // Create a new group
        GroupInheritancePermissions gi = new GroupInheritancePermissions(false, true, true, true, true);
        service.createGroup(adminToken, null, "Jeff's Group", gi);

        Set<String> usersCall = service.getGroupUsers(adminToken, "Jeff's Group", false);
        Set<String> membersCall = service.getGroupMembers(adminToken, "Jeff's Group", false).getUsers();
        Assert.assertEquals(usersCall, membersCall);
    }

    @Test
    public void getGroupMembersRequiresAdminRead() throws TException {
        String user2 = "Jim";

        // Add a couple users
        service.createUser(adminToken, user2, "Jim's Name");

        // Create a new group
        String groupId = "Jeff's Group";
        GroupInheritancePermissions gi = new GroupInheritancePermissions(true, false, true, true, true);
        service.createGroup(adminToken, null, groupId, gi);

        service.addUserToGroup(adminToken, groupId, user2, new UserGroupPermissions());

        EzSecurityToken user2Token = MockEzSecurityToken.getMockUserToken(user2, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);

        Set<String> membersCall = service.getGroupMembers(user2Token, groupId, false).getUsers();
        Assert.assertEquals(0, membersCall.size());
    }

    @Test
    public void getUserGroups() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));

        // Get the users groups
        Set<UserGroup> groups = service.getUserGroups(userToken, false);
        Set<String> groupNames = Sets.newHashSet();
        for (UserGroup g : groups) {
            groupNames.add(g.getGroup().getGroupName());
        }
        Assert.assertEquals(Sets.newHashSet("root", "group1", "group2", "group3"), groupNames);
    }

    @Test
    public void getUserGroupsWithPermissions() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException, GroupQueryException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);
        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(adminToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(adminToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(adminToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));

        service.addUserToGroup(adminToken, "group1", user, new UserGroupPermissions().setAdminCreateChild(true));
        service.addUserToGroup(adminToken, "group2", user, new UserGroupPermissions().setAdminCreateChild(true).setAdminRead(true));

        // Get the users groups
        Set<UserGroup> groups = service.getUserGroups(userToken, false);
        Set<String> groupNames = Sets.newHashSet();
        for (UserGroup g : groups) {
            groupNames.add(g.getGroup().getGroupName());
            if (g.getGroup().getGroupName().equals("group1")) {
                Assert.assertTrue(g.getPermissions().isDataAccess());
                Assert.assertTrue(g.getPermissions().isAdminCreateChild());
                Assert.assertTrue(!g.getPermissions().isAdminManage());
                Assert.assertTrue(!g.getPermissions().isAdminRead());
                Assert.assertTrue(!g.getPermissions().isAdminWrite());
            } else if (g.getGroup().getGroupName().equals("group2")) {
                Assert.assertTrue(g.getPermissions().isDataAccess());
                Assert.assertTrue(g.getPermissions().isAdminCreateChild());
                Assert.assertTrue(!g.getPermissions().isAdminManage());
                Assert.assertTrue(g.getPermissions().isAdminRead());
                Assert.assertTrue(!g.getPermissions().isAdminWrite());
            } else if (g.getGroup().getGroupName().equals("root")) {
                Assert.assertTrue(g.getPermissions().isDataAccess());
                Assert.assertTrue(g.getPermissions().isAdminCreateChild());
                Assert.assertTrue(!g.getPermissions().isAdminManage());
                Assert.assertTrue(!g.getPermissions().isAdminRead());
                Assert.assertTrue(!g.getPermissions().isAdminWrite());
            } else {
                Assert.assertTrue(g.getPermissions().isDataAccess());
                Assert.assertTrue(!g.getPermissions().isAdminCreateChild());
                Assert.assertTrue(!g.getPermissions().isAdminManage());
                Assert.assertTrue(!g.getPermissions().isAdminRead());
                Assert.assertTrue(!g.getPermissions().isAdminWrite());
            }
        }
        Assert.assertEquals(Sets.newHashSet("root", "group1", "group2", "group3"), groupNames);
    }

    @Test
    public void getUserGroupsAppDontMatchDefInclusion() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(false, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(false, false, false, false, false));

        // Get the users groups
        Set<UserGroup> groups = service.getUserGroups(userToken, false);
        Set<String> groupNames = Sets.newHashSet();
        for (UserGroup g : groups) {
            groupNames.add(g.getGroup().getGroupName());
        }
        Assert.assertEquals(Sets.newHashSet("root", "group1", "group2", "group3"), groupNames);
    }

    @Test
    public void getUserGroupsAppDontMatchInclusion() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(false, false, false, false, false));
        service.createGroupWithInclusion(userToken, null, "group3", new GroupInheritancePermissions(false, false, false, false, false), false, false);

        // Get the users groups
        Set<UserGroup> groups = service.getUserGroups(userToken, false);
        Set<String> groupNames = Sets.newHashSet();
        for (UserGroup g : groups) {
            groupNames.add(g.getGroup().getGroupName());
        }
        Assert.assertEquals(Sets.newHashSet("root", "group1", "group2"), groupNames);
    }

    @Test
    public void getUserGroupsAppDontMatchAppInclusion() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(false, false, false, false, false));

        // App needs to create this group
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(app);
        service.createGroupWithInclusion(appToken, null, "group3", new GroupInheritancePermissions(false, false, false, false, false), false, true);

        // Get the users groups
        Set<UserGroup> groups = service.getUserGroups(userToken, false);
        Set<String> groupNames = Sets.newHashSet();
        for (UserGroup g : groups) {
            groupNames.add(g.getGroup().getGroupName());
        }
        Assert.assertEquals(Sets.newHashSet("root", "group1", "group2", "group3"), groupNames);
    }

    @Test
    public void getUserAuths() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        long jared = service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));

        // Get the users groups
        Set<Long> groups = service.getAuthorizations(userToken);

        // Look up the indices for the groups
        FramedGraph<TitanGraph> graph = service.getGraph().getFramedGraph();
        long root = graph.query().has(Group.GROUP_NAME, "root").vertices(Group.class).iterator().next().getIndex();
        long g1 = graph.query().has(Group.GROUP_NAME, "root.group1").vertices(Group.class).iterator().next().getIndex();
        long g2 = graph.query().has(Group.GROUP_NAME, "root.group2").vertices(Group.class).iterator().next().getIndex();
        long g3 = graph.query().has(Group.GROUP_NAME, "root.group3").vertices(Group.class).iterator().next().getIndex();


        Assert.assertEquals(Sets.newTreeSet(Sets.newHashSet(root, jared, g1, g2, g3)), groups);
    }

    @Test
    public void getUserAuthsPrivileged() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException, GroupQueryException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        long jared = service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroupWithInclusion(userToken, null, "no app", new GroupInheritancePermissions(false, false, false, false, false), false, false);
        service.createGroupWithInclusion(appToken, null, "App GROUP", new GroupInheritancePermissions(false, false, false, false, false), false, true);

        // Get the users groups
        Set<Long> groups = service.getUserAuthorizations(adminToken, TokenType.USER, user, Lists.newArrayList(app));

        // Look up the indices for the groups
        FramedGraph<TitanGraph> graph = service.getGraph().getFramedGraph();
        long root = graph.query().has(Group.GROUP_NAME, "root").vertices(Group.class).iterator().next().getIndex();
        long g1 = graph.query().has(Group.GROUP_NAME, "root.group1").vertices(Group.class).iterator().next().getIndex();
        long g2 = graph.query().has(Group.GROUP_NAME, "root.group2").vertices(Group.class).iterator().next().getIndex();
        long g3 = graph.query().has(Group.GROUP_NAME, "root.group3").vertices(Group.class).iterator().next().getIndex();
        long g4 = graph.query().has(Group.GROUP_NAME, "root.no app").vertices(Group.class).iterator().next().getIndex(); // don't expect to see this
        long a1 = graph.query().has(Group.GROUP_NAME, "root.App GROUP").vertices(Group.class).iterator().next().getIndex();

        Assert.assertEquals(Sets.newTreeSet(Sets.newHashSet(root, g1, g2, g3, a1, jared)), groups);
    }

    @Test
    public void getUserAuthsDeactivatedUser() throws AuthorizationException, GroupQueryException, EzSecurityTokenException, EzGroupOperationException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));

        // Deactivate user
        service.deactivateUser(adminToken, user);

        // Get the users groups
        Set<Long> groups = service.getAuthorizations(userToken);

        Assert.assertEquals(0, groups.size());
    }


    @Test
    public void getUserAuthsPrivilegedDeactivatedUser() throws AuthorizationException, EzGroupOperationException, EzSecurityTokenException, GroupQueryException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroupWithInclusion(userToken, null, "no app", new GroupInheritancePermissions(false, false, false, false, false), false, false);
        service.createGroupWithInclusion(appToken, null, "App GROUP", new GroupInheritancePermissions(false, false, false, false, false), false, true);

        // Deactivate user
        service.deactivateUser(adminToken, user);

        // Get the users groups
        Set<Long> groups = service.getUserAuthorizations(adminToken, TokenType.USER, user, Lists.newArrayList(app));

        Assert.assertEquals(0, groups.size());
    }



    @Test
    public void getUserAuthsInactiveGroups() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        long jared = service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false));
        service.deactivateGroup(userToken, "group3", false);

        // Get the users groups
        Set<Long> groups = service.getAuthorizations(userToken);

        // Look up the indices for the groups
        FramedGraph<TitanGraph> graph = service.getGraph().getFramedGraph();
        long root = graph.query().has(Group.GROUP_NAME, "root").vertices(Group.class).iterator().next().getIndex();
        long g1 = graph.query().has(Group.GROUP_NAME, "root.group1").vertices(Group.class).iterator().next().getIndex();
        long g2 = graph.query().has(Group.GROUP_NAME, "root.group2").vertices(Group.class).iterator().next().getIndex();


        Assert.assertEquals(Sets.newTreeSet(Sets.newHashSet(root, jared, g1, g2)), groups);
    }


    @Test
    public void getUserAuthsInclusionFalse() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        long jared = service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroupWithInclusion(userToken, null, "group3", new GroupInheritancePermissions(true, false, false, false, false), false, false);

        // Get the users groups
        Set<Long> groups = service.getAuthorizations(userToken);

        // Look up the indices for the groups
        FramedGraph<TitanGraph> graph = service.getGraph().getFramedGraph();
        long root = graph.query().has(Group.GROUP_NAME, "root").vertices(Group.class).iterator().next().getIndex();
        long g1 = graph.query().has(Group.GROUP_NAME, "root.group1").vertices(Group.class).iterator().next().getIndex();
        long g2 = graph.query().has(Group.GROUP_NAME, "root.group2").vertices(Group.class).iterator().next().getIndex();


        Assert.assertEquals(Sets.newTreeSet(Sets.newHashSet(root, jared, g1, g2)), groups);
    }

    @Test
    public void getUserAuthsAppInclusion() throws TException {
        String app = "APP";
        service.createAppUser(adminToken, app, app);

        String user = "Jared";
        long jared = service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(true, false, false, false, false));

        // App needs to create this group
        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(app);
        service.createGroupWithInclusion(appToken, null, "group3", new GroupInheritancePermissions(false, false, false, false, false), false, true);

        // Get the users groups
        Set<Long> groups = service.getAuthorizations(userToken);

        // Look up the indices for the groups
        FramedGraph<TitanGraph> graph = service.getGraph().getFramedGraph();
        long root = graph.query().has(Group.GROUP_NAME, "root").vertices(Group.class).iterator().next().getIndex();
        long g1 = graph.query().has(Group.GROUP_NAME, "root.group1").vertices(Group.class).iterator().next().getIndex();
        long g2 = graph.query().has(Group.GROUP_NAME, "root.group2").vertices(Group.class).iterator().next().getIndex();
        long g3 = graph.query().has(Group.GROUP_NAME, "root.group3").vertices(Group.class).iterator().next().getIndex();


        Assert.assertEquals(Sets.newTreeSet(Sets.newHashSet(root, jared, g1, g2, g3)), groups);
    }



    @Test(expected=GroupQueryException.class)
    public void getUserGroupsAppDontExist() throws TException {
        String app = "APP";
        String user = "Jared";
        service.createUser(adminToken, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        service.createGroup(userToken, null, "group1", new GroupInheritancePermissions(true, false, false, false, false));
        service.createGroup(userToken, null, "group2", new GroupInheritancePermissions(false, false, false, false, false));
        service.createGroup(userToken, null, "group3", new GroupInheritancePermissions(false, false, false, false, false));

        // Get the users groups
        service.getUserGroups(userToken, false);
    }

    /**************************************************************************************/
    /**                      Special App group methods                                   **/
    /**************************************************************************************/

    @Test
    public void getDiagnosticUsers() throws TException {
        String groupOwner = "Jeff";
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(groupOwner, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);

        String securityId = "app1";
        service.createAppUser(adminToken, securityId, securityId);

        String user1 = "Bill";
        service.createUser(adminToken, user1, user1);

        String diagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                .join(EzGroupsConstants.APP_GROUP, "app1", EzGroupsConstants.DIAGNOSTICS_GROUP);

        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(securityId);
        service.addUserToGroup(appToken, diagnosticGroup, user1, new UserGroupPermissions());


        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user1);
        Set<String> apps = service.getUserDiagnosticApps(userToken);

        Assert.assertEquals(Sets.newHashSet(securityId), apps);
    }

    @Test
    public void getDiagnosticUsersManualCreateGroup() throws TException {
        service.addUserToGroup(adminToken, "root.app", adminToken.getTokenPrincipal().getPrincipal(), UserGroupPermissionsWrapper.ownerPermissions());

        String securityId = "app1";
        String app1Group = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.APP_GROUP, securityId);
        String app1DiagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(app1Group, EzGroupsConstants.DIAGNOSTICS_GROUP);

        service.createGroup(adminToken, EzGroupsConstants.APP_GROUP, securityId, new GroupInheritancePermissions());
        service.createGroup(adminToken, app1Group, EzGroupsConstants.DIAGNOSTICS_GROUP, new GroupInheritancePermissions());

        String user1 = "Bill";
        service.addUserToGroup(adminToken, app1DiagnosticGroup, user1, new UserGroupPermissions());

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user1);
        Set<String> apps = service.getUserDiagnosticApps(userToken);
        Assert.assertEquals(Sets.newHashSet(securityId), apps);
    }

    @Test
    public void getMetricsUsers() throws TException {
        String groupOwner = "Jeff";
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(groupOwner, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);

        String securityId = "app1";
        service.createAppUser(adminToken, securityId, securityId);

        String user1 = "Bill";
        service.createUser(adminToken, user1, user1);

        String diagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                .join(EzGroupsConstants.APP_GROUP, "app1", EzGroupsConstants.METRICS_GROUP);

        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(securityId);
        service.addUserToGroup(appToken, diagnosticGroup, user1, new UserGroupPermissions());


        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user1);
        Set<String> apps = service.getUserMetricsApps(userToken);

        Assert.assertEquals(Sets.newHashSet(securityId), apps);
    }

    @Test
    public void getAuditUsers() throws TException {
        String groupOwner = "Jeff";
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(groupOwner, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);

        String securityId = "app1";
        service.createAppUser(adminToken, securityId, securityId);

        String user1 = "Bill";
        service.createUser(adminToken, user1, user1);

        String diagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                .join(EzGroupsConstants.APP_GROUP, "app1", EzGroupsConstants.AUDIT_GROUP);

        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(securityId);
        service.addUserToGroup(appToken, diagnosticGroup, user1, new UserGroupPermissions());


        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user1);
        Set<String> apps = service.getUserAuditorApps(userToken);

        Assert.assertEquals(Sets.newHashSet(securityId), apps);
    }

    @Test
    public void appNotInAuditUsers() throws TException {
        String groupOwner = "Jeff";
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(groupOwner, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), true);

        String securityId = "app1";
        service.createAppUser(adminToken, securityId, securityId);

        String user1 = "Bill";
        service.createUser(adminToken, user1, user1);

        String diagnosticGroup = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                .join(EzGroupsConstants.APP_GROUP, "app1", EzGroupsConstants.AUDIT_GROUP);

        EzSecurityToken appToken = MockEzSecurityToken.getMockAppToken(securityId);
        service.addUserToGroup(appToken, diagnosticGroup, user1, new UserGroupPermissions());


        Set<String> apps = service.getUserAuditorApps(appToken);

        Assert.assertTrue(apps.isEmpty());
    }


    /**************************************************************************************/
    /**                        Test Methods Requiring EzAdmin                            **/
    /**************************************************************************************/
    @Test(expected=EzSecurityTokenException.class)
    public void testAddUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.createUser(adminToken, "Jeff", "Jeff's Name");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testModifyUserPrincipalNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.modifyUser(adminToken, "Doesn't Matter", "Should Throw Exception");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testActivateUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.activateUser(adminToken, "Doesn't Matter");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testDeactivateUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.deactivateUser(adminToken, "Doesn't Matter");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testDeleteUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.deleteUser(adminToken, "Doesn't Matter");
    }

    @Test(expected=EzSecurityTokenException.class)
    public void testAddAppUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.createAppUser(adminToken, "Jeff", "Jeff's Name");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testActivateAppUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.activateAppUser(adminToken, "Doesn't Matter");
    }
    @Test(expected=EzSecurityTokenException.class)
    public void testDeactivateAppUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.deactivateAppUser(adminToken, "Doesn't Matter");
    }
    @Test(expected=EzGroupOperationException.class)
    public void testDeleteAppUserNotAdmin() throws TException {
        EzSecurityToken adminToken = MockEzSecurityToken.getMockUserToken(false);
        service.deleteAppUser(adminToken, "Doesn't Matter");
    }



    /**************************************************************************************/
    /**                             Test Specfic workflows                               **/
    /**************************************************************************************/


    @Test
    public void testRegisterPromoteWorkflow() throws TException {
        String securityId = "App";
        try {
            service.activateAppUser(adminToken, securityId);
        } catch (EzGroupOperationException e) {
            if (e.getOperation() == OperationError.USER_NOT_FOUND) {
                service.createAppUser(adminToken, securityId, securityId);
            }
        }
    }

    @Test
    public void testEzSecurityAppUserCreationAndAddDiagnosticUser() throws AuthorizationException,
            EzGroupOperationException, EzSecurityTokenException, GroupQueryException, UserNotFoundException,
            VertexNotFoundException, AccessDeniedException {
        String securityId = "EzBakeFrontEnd";
        service.createAppUserAndGetAuthorizations(adminToken, Lists.<String>newArrayList(), securityId, securityId);
        Set<Group> groups = service.getGraph().getGroupChildren(BaseVertex.VertexType.APP_USER, securityId, "root.app." + securityId);

        Set<String> groupNames = Sets.newHashSet();
        for (Group g : groups) {
            groupNames.add(g.getGroupFriendlyName());
        }
        Assert.assertEquals(Sets.newHashSet(EzGroupsConstants.DIAGNOSTICS_GROUP, EzGroupsConstants.AUDIT_GROUP, EzGroupsConstants.METRICS_GROUP), groupNames);

        service.addUserToGroup(adminToken, "app."+securityId+EzGroupsConstants.GROUP_NAME_SEP+EzGroupsConstants.DIAGNOSTICS_GROUP, "BILLY", new UserGroupPermissions());
        Assert.assertEquals(Sets.newHashSet(securityId), service.getUserDiagnosticApps(MockEzSecurityToken.getMockUserToken("BILLY")));
    }

    @Test(expected=AuthorizationException.class)
    public void testManualAppGroupsCreationAndAddDiagnosticUser() throws AuthorizationException,
            EzGroupOperationException, EzSecurityTokenException, GroupQueryException, UserNotFoundException,
            VertexNotFoundException, AccessDeniedException {
        String securityId = "EzBakeFrontEnd";
        service.createGroup(adminToken, "app", securityId, new GroupInheritancePermissions());
    }

    @Test
    public void testChangeAppUserNameAndAddDiagnosticUser() throws AuthorizationException,
            EzGroupOperationException, EzSecurityTokenException, GroupQueryException, UserNotFoundException,
            VertexNotFoundException, AccessDeniedException {
        String securityId = "_Ez_EFE";
        String name = "EzBakeFrontEnd";
        service.createAppUser(adminToken, securityId, securityId);
        service.modifyAppUser(adminToken, securityId, securityId, name);
        service.addUserToGroup(adminToken, "app."+name+EzGroupsConstants.GROUP_NAME_SEP+EzGroupsConstants.DIAGNOSTICS_GROUP, "BILLY", new UserGroupPermissions());
        Assert.assertEquals(Sets.newHashSet(name), service.getUserDiagnosticApps(MockEzSecurityToken.getMockUserToken("BILLY")));
    }

    @Test
    public void testGetUser() throws TException {
        String securityId = "appUserSecId";
        String name = "appUserName";
        
        ezbake.groups.thrift.User expected = new ezbake.groups.thrift.User().setIsActive(true).setName(name).setPrincipal(securityId);
        service.createAppUser(adminToken, securityId, name);
        ezbake.groups.thrift.User actual = service.getUser(adminAppToken, UserType.APP_USER, securityId); 
        Assert.assertEquals(expected, actual);
        
        expected.setIsActive(false);
        service.deactivateAppUser(adminToken, securityId);
        actual = service.getUser(adminAppToken, UserType.APP_USER, securityId);
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testGetGroup() throws GroupQueryException, TException {
        String userId = "CN=Anakin Skywalker";
        String name = "Anakin Skywalker";
        service.createUser(adminToken, userId, name);

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(userId, "",
                Collections.<String>emptySet(), Collections.<String, List<String>>emptyMap(), false);
        String groupName = "Star Wars";
        service.createGroup(userToken, null, groupName, new GroupInheritancePermissions(true, true, true, true, true));
        
        ezbake.groups.thrift.Group expected = new ezbake.groups.thrift.Group()
        .setId(11)
        .setGroupName(groupName)
        .setInheritancePermissions(new GroupInheritancePermissions(true, true, true, true, true))
        .setRequireOnlyUser(true)
        .setRequireOnlyAPP(false)
        .setIsActive(true);
        
        ezbake.groups.thrift.Group actual = service.getGroup(userToken, groupName);

        Assert.assertEquals(expected, actual);
    }
}
