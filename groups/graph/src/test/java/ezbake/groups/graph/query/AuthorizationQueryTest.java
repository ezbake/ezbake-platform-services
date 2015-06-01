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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.*;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.*;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbake.groups.thrift.*;
import ezbake.security.test.MockEzSecurityToken;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AuthorizationQueryTest extends GraphCommonSetup {

    EzGroupsGraphImpl graph;
    AuthorizationQuery authQuery;

    @Before
    public void setUpTests() {
        graph = new EzGroupsGraphImpl(titanGraph, new NaiveIDProvider(), new GroupQuery(framedTitanGraph));
        authQuery = new AuthorizationQuery(graph);
    }

    @Test
    public void getUserGroups() throws TException, VertexExistsException, IndexUnavailableException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, InvalidGroupNameException, VertexNotFoundException {
        String app = "APP";
        graph.addUser(BaseVertex.VertexType.APP_USER, app, app);

        String user = "Jared";
        graph.addUser(BaseVertex.VertexType.USER, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, user, "group1",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, user, "group2",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, user, "group3",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);

        // Get the users groups
        Set<Group> groups = authQuery.getUserGroups(BaseVertex.VertexType.USER, user, false);
        Set <String> groupNames = Sets.newHashSet();
        for (Group g : groups) {
            groupNames.add(g.getGroupName());
        }
        Assert.assertEquals(Sets.newHashSet("root", g1.getGroupName(), g2.getGroupName(), g3.getGroupName()), groupNames);
    }

    @Test
    public void getUserGroupsError() {
        try {
            authQuery.getUserGroups(BaseVertex.VertexType.USER, "does not exist", false);
            Assert.fail("Should have thrown exception");
        } catch (GroupQueryException e) {
            Assert.assertEquals(GroupQueryError.USER_NOT_FOUND, e.getErrorType());
        }

        try {
            authQuery.getUserGroups(BaseVertex.VertexType.GROUP, "does not matter", false);
            Assert.fail("Should have thrown exception");
        } catch (GroupQueryException e) {
            Assert.assertEquals(GroupQueryError.USER_NOT_FOUND, e.getErrorType());
        }
    }

    @Test
    public void getUserGroupsMultiError() {
        Map<String, BaseVertex.VertexType> users = Maps.newHashMap();
        users.put("not a user", BaseVertex.VertexType.USER);
        Collection<Set<Group>> groups = authQuery.getUserGroups(users, false);
        Assert.assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetAuthorizationSetInvalidType() throws GroupQueryException {
        Assert.assertTrue(
                authQuery.getAuthorizationSet(BaseVertex.VertexType.GROUP, null, null).isEmpty());
    }

    @Test
    public void testGetAuthorizationSetNoUser() {
        try {
            authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, "timmy", null);
            Assert.fail("Should have thrown exception");
        } catch (GroupQueryException e) {
            Assert.assertEquals(GroupQueryError.USER_NOT_FOUND, e.getErrorType());
        }
    }

    @Test
    public void testGetAuthorizationSet() throws VertexExistsException, IndexUnavailableException, UserNotFoundException, InvalidGroupNameException, AccessDeniedException, VertexNotFoundException, InvalidVertexTypeException, GroupQueryException {
        String app = "APP";
        graph.addUser(BaseVertex.VertexType.APP_USER, app, app);

        String user = "Jared";
        ezbake.groups.graph.frames.vertex.User jared = graph.addUser(BaseVertex.VertexType.USER, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, user, "group1",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, user, "group2",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, user, "group3",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g4 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group4", "root", new GroupInheritancePermissions(false, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);

        // Get the users groups
        Set<Long> groups = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, Lists.newArrayList(app));
        Set<Long> groupsNull = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, null);

        Assert.assertEquals(groups, groupsNull);
        Assert.assertEquals(Sets.newHashSet(0l, jared.getIndex(), g1.getIndex(), g2.getIndex(), g3.getIndex()), groups);

        graph.setUserActiveOrNot(BaseVertex.VertexType.USER, user, false);
        Set<Long> groupsDeactivated = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, null);
        Assert.assertEquals(0, groupsDeactivated.size());
    }

    @Test
    public void testGetAuthorizationSetAppUser() throws VertexExistsException, IndexUnavailableException, UserNotFoundException, InvalidGroupNameException, AccessDeniedException, VertexNotFoundException, InvalidVertexTypeException, GroupQueryException {
        String app = "APP";
        User appUser = graph.addUser(BaseVertex.VertexType.APP_USER, app, app);
        Group appGroup = graph.getGroup("root.app." + app);
        Group appaccessGroup = graph.getGroup("root.appaccess."+app);

        EzSecurityToken userToken = MockEzSecurityToken.getMockAppToken(app);
        userToken.getValidity().setIssuedTo(app);

        Group g1 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group1",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group2",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g3 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group3",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g4 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group4", "root", new GroupInheritancePermissions(false, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);

        // Get the users groups
        Set<Long> groups = authQuery.getAuthorizationSet(BaseVertex.VertexType.APP_USER, app, null);
        Set<Long> groupsNull = authQuery.getAuthorizationSet(BaseVertex.VertexType.APP_USER, app, null);

        Assert.assertEquals(groups, groupsNull);
        Assert.assertEquals(
                Sets.newHashSet(0l, 2l, appUser.getIndex(), g1.getIndex(), g2.getIndex(), g3.getIndex(), g4.getIndex(),
                        appGroup.getIndex(), appaccessGroup.getIndex()),
                groups);

        graph.setUserActiveOrNot(BaseVertex.VertexType.APP_USER, app, false);
        Set<Long> groupsDeactivated = authQuery.getAuthorizationSet(BaseVertex.VertexType.APP_USER, app, null);
        Assert.assertEquals(0, groupsDeactivated.size());
    }

    @Test
    public void testGetAuthorizationSetApp() throws VertexExistsException, IndexUnavailableException, UserNotFoundException, InvalidGroupNameException, AccessDeniedException, VertexNotFoundException, InvalidVertexTypeException, GroupQueryException {
        String app = "APP";
        graph.addUser(BaseVertex.VertexType.APP_USER, app, app);

        String user = "Jared";
        ezbake.groups.graph.frames.vertex.User jared = graph.addUser(BaseVertex.VertexType.USER, user, "");

        EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);
        userToken.getValidity().setIssuedTo(app);

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, user, "group1",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, user, "group2",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, user, "group3",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);

        Group g4 = graph.addGroup(BaseVertex.VertexType.APP_USER, app, "group4", "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), false, true);

        // Get the users groups
        Set<Long> groups = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, Lists.newArrayList(app));

        Assert.assertEquals(
                Sets.newHashSet(
                        0l,
                        jared.getIndex(),
                        g1.getIndex(),
                        g2.getIndex(),
                        g3.getIndex(),
                        g4.getIndex()
                ),
                groups);

        graph.setUserActiveOrNot(BaseVertex.VertexType.APP_USER, app, false);
        Set<Long> groupsAppDeactivated = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, Lists.newArrayList(app));
        Assert.assertEquals(Sets.newHashSet(0l, jared.getIndex(), g1.getIndex(), g2.getIndex(), g3.getIndex()), groupsAppDeactivated);
    }

    @Test
    public void testExecute() throws VertexExistsException, IndexUnavailableException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, InvalidGroupNameException, VertexNotFoundException, GroupQueryException {
        String user = "Jared";
        ezbake.groups.graph.frames.vertex.User jared = graph.addUser(BaseVertex.VertexType.USER, user, "");

        Group g1 = graph.addGroup(BaseVertex.VertexType.USER, user, "group1",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g2 = graph.addGroup(BaseVertex.VertexType.USER, user, "group2",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);
        Group g3 = graph.addGroup(BaseVertex.VertexType.USER, user, "group3",  "root", new GroupInheritancePermissions(true, false, false, false, false), UserGroupPermissionsWrapper.ownerPermissions(), true, false);

        // Get the users groups
        Set<Long> userAuths = authQuery.getAuthorizationSet(BaseVertex.VertexType.USER, user, null);
        Set<Long> userAuthsExecute = authQuery.execute(BaseVertex.VertexType.USER, user, null);
        Assert.assertEquals(userAuths, userAuthsExecute);
    }

}
