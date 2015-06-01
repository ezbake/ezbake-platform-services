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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.query.GroupQuery;
import ezbake.groups.thrift.GroupInheritancePermissions;

public class PermissionEnforcerTest extends GraphCommonSetup {

    private EzGroupsGraphImpl graph;
    private GroupQuery query;
    private PermissionEnforcer enforcer;

    @Before
    public void setUpTests() {
        query = new GroupQuery(framedTitanGraph);
        graph = new EzGroupsGraphImpl(titanGraph, new NaiveIDProvider(), query);
        enforcer = query.getPermissionEnforcer();
    }

    @Test
    public void testHasAnyPermissionWithoutPermission() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(false, false, false, false, false));
        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermission1() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(true, false, false, false, false));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermission2() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(false, true, false, false, false));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermission3() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(false, false, true, false, false));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermission4() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(false, false, false, true, false));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermission5() throws Exception {
        Group endGroup = buildGroupsAndGetTestGroup(new GroupInheritancePermissions(false, false, false, false, true));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), endGroup));
    }

    @Test
    public void testHasAnyPermissionCheckAllGroups() throws Exception {
        final Group group123 =
                buildGroupsAndGetTestGroup(new GroupInheritancePermissions(true, false, false, false, false));

        final Group group1 = graph.getGroup(GROUP1_FQNAME);
        final Group group12 = graph.getGroup(GROUP12_FQNAME);
        final Group group13 = graph.getGroup(GROUP13_FQNAME);

        final String newUserId = "bob";
        graph.addUser(BaseVertex.VertexType.USER, newUserId, newUserId);

        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), group1));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), group12));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), group123));
        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER2_ID), group13));

        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER1_ID), group1));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER1_ID), group12));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER1_ID), group123));
        assertTrue(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, USER1_ID), group13));

        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, newUserId), group1));
        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, newUserId), group12));
        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, newUserId), group123));
        assertFalse(enforcer.hasAnyPermission(graph.getUser(BaseVertex.VertexType.USER, newUserId), group13));
    }

    /**
     * Adds some users and groups to EzGroups and returns a grandchild group that can be used for testing.
     *
     * @param testGroupInheritancePermissions group permissions that are inherited by the returned group; this allows an
     * user to have access to a group without having a direct edge to that group
     * @return a Group with some properties that are useful for testing
     */
    private Group buildGroupsAndGetTestGroup(GroupInheritancePermissions testGroupInheritancePermissions)
            throws Exception {
        final UserGroupPermissionsWrapper userGroupPermissionsWrapper =
                new UserGroupPermissionsWrapper(true, true, true, true, true);

        // Add an user
        graph.addUser(BaseVertex.VertexType.USER, USER1_ID, "User One");
        graph.addUser(BaseVertex.VertexType.USER, USER2_ID, "User Two");

        // Add groups with multiple levels of children
        graph.addGroup(BaseVertex.VertexType.USER, USER1_ID, GROUP1_FRIENDLY_NAME);

        graph.addGroup(
                BaseVertex.VertexType.USER, USER1_ID, GROUP12_FRIENDLY_NAME, GROUP1_FQNAME,
                testGroupInheritancePermissions, userGroupPermissionsWrapper, false, false);

        Group endGroup = graph.addGroup(
                BaseVertex.VertexType.USER, USER1_ID, GROUP123_FRIENDLY_NAME, GROUP12_FQNAME,
                testGroupInheritancePermissions, userGroupPermissionsWrapper, false, false);

        // User 2 should not ever get permissions on this group unless explicitly added to it
        graph.addGroup(
                BaseVertex.VertexType.USER, USER1_ID, GROUP13_FRIENDLY_NAME, GROUP1_FQNAME,
                new GroupInheritancePermissions(false, false, false, false, false),
                new UserGroupPermissionsWrapper(true, false, false, false, false), false, false);

        graph.addUserToGroup(BaseVertex.VertexType.USER, USER2_ID, GROUP1_FQNAME, true, true, true, true, true);

        return endGroup;
    }
}
