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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.Group;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.User;
import ezbake.groups.thrift.UserType;
import ezbake.security.test.MockEzSecurityToken;

public class EzGroupsIT extends GroupsServiceCommonITSetup {

    private EzGroups.Client client;
    private final String adminId = "steve";
    private EzSecurityToken adminToken;

    @Before
    public void setUp() throws TException {
        client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);

        adminToken = MockEzSecurityToken.getMockUserToken(
                adminId, "", Sets.<String>newHashSet(), Maps.<String, List<String>>newHashMap(), true);
    }

    @Test
    public void testPing() throws TException {

        assertTrue(client.ping());
    }

    @Test
    public void testCreateUserAndGetUser() throws TException {
        client.createUser(adminToken, adminId, adminId);

        User user = client.getUser(adminToken, UserType.USER, adminId);

        User expectedUser = new User();
        expectedUser.setPrincipal(adminId);
        expectedUser.setName(adminId);
        expectedUser.setIsActive(true);

        assertEquals(expectedUser, user);
    }

    @Test
    public void testCreateGroup() throws TException {
        client.createUser(adminToken, adminId, adminId);

        // create the group
        final String createGroupId = "someGroup";
        long groupId = client.createGroup(adminToken, null, createGroupId, new GroupInheritancePermissions());

        // get the group
        final Group createGroup = client.getGroup(adminToken, createGroupId);

        // verify it is the group we created
        final Group expectedCreateGroup = getExpectedGroup(groupId, createGroupId, createGroupId);

        assertEquals(createGroup, expectedCreateGroup);
    }

    @Test
    public void testCreateAndGetGroup() throws TException {
        client.createUser(adminToken, adminId, adminId);

        final long expectedIndex = 4; // ?
        final String createAndGetGroupId = "alexGroup";
        final Group myGroup =
                client.createAndGetGroup(adminToken, null, createAndGetGroupId, new GroupInheritancePermissions());

        final Group expectedMyGroup = getExpectedGroup(expectedIndex, createAndGetGroupId, createAndGetGroupId);

        assertEquals(expectedMyGroup, myGroup);
    }

    @Test
    public void testGetGroupsMask() throws TException {
        long userIndex = client.createUser(adminToken, adminId, adminId);

        // IDs referred to when calling getGroupsMask
        final String groupId1 = "aGroup";
        final String groupId3 = "cGroup";
        final String appUserId = "appUser";

        // Create groups, create one in the middle we wont get the index for
        long groupIndex1 = client.createGroup(adminToken, null, groupId1, new GroupInheritancePermissions());
        client.createGroup(adminToken, null, "bGroup", new GroupInheritancePermissions());
        long groupIndex3 = client.createGroup(adminToken, null, groupId3, new GroupInheritancePermissions());

        // create app user we'll get the index for
        long appUserIndex = client.createAppUser(adminToken, appUserId, appUserId);

        Set<Long> expectedIndicess = Sets.newHashSet(userIndex, groupIndex1, groupIndex3, appUserIndex);
        Set<Long> actualIndices = client.getGroupsMask(
                adminToken, Sets.newHashSet(groupId1, groupId3), Sets.newHashSet(adminId), Sets.newHashSet(appUserId));

        assertEquals(expectedIndicess, actualIndices);
    }

    @Test
    public void testRenameGroup() throws TException {
        client.createUser(adminToken, adminId, adminId);

        // create several groups and track their names
        final String renameThisGroupName = "renameMe";
        final String childGroupOneFriendlyName = "c1";
        final String childGroupOneFullyQualifiedName =
                String.format("%s.%s", renameThisGroupName, childGroupOneFriendlyName);
        final String childGroupTwoName = "c2";

        client.createGroup(adminToken, null, renameThisGroupName, new GroupInheritancePermissions());

        client.createGroup(
                adminToken, renameThisGroupName, childGroupOneFriendlyName, new GroupInheritancePermissions());

        final long expectedId = client.createGroup(
                adminToken, childGroupOneFullyQualifiedName, childGroupTwoName, new GroupInheritancePermissions());

        // use the client to change the group name of the parent of the groups created in this section
        final String myNewName = "myNewName";
        client.changeGroupName(adminToken, renameThisGroupName, myNewName);
        final String expectedNewName =
                childGroupOneFullyQualifiedName.replace(renameThisGroupName, myNewName) + '.' + childGroupTwoName;

        // verify it is the renamed group we expected
        final Group renamedGrandChild = client.getGroup(adminToken, expectedNewName);

        final Group expectedRenamedGroup = getExpectedGroup(expectedId, expectedNewName, childGroupTwoName);

        assertEquals(expectedRenamedGroup, renamedGrandChild);
    }

    /**
     * Add some groups, users, and app users.  Test the different combinations of app and user return the expected values
     * based on their users' permissions on the groups.
     */
    @Test
    public void testGetGroupNamesByIndices() throws Exception{
        // mock token validity.issuedTo() field comes populated with 'client' EzGroups looks for a group name of
        // 'client' if we don't add this we get an exception due to group not found
        client.createAppUser(adminToken, "client", "client");

        final Long userId1 = client.createUser(adminToken, adminId, adminId);

        final String gropu1Id = "group1";
        final String gropu2Id = "group2";
        final String user = "jar_jar";
        final Long userId2 = client.createUser(adminToken, user, user);

        final String appUser = "AnAppUser";
        client.createAppUser(adminToken, appUser, appUser);

        final EzSecurityToken userToken = MockEzSecurityToken.getMockUserToken(user);

        // create two groups we will use in tests
        final Group group1 = client.createAndGetGroup(adminToken, null, gropu1Id, new GroupInheritancePermissions());
        final Group group2 = client.createAndGetGroup(
                adminToken, gropu1Id, gropu2Id, new GroupInheritancePermissions(true, true, false, false, false));

        // Group two inherits from group one. If we add the user to group one then the user will have permission on both
        // group one and group two
        client.addUserToGroup(adminToken, group1.getGroupName(), user, UserGroupPermissionsWrapper.ownerPermissions());

        // We only add the app to group two, since group two does not inherit from group one, the app will only have
        // permissions on group two.
        client.addAppUserToGroup(
                adminToken, group2.getGroupName(), appUser, UserGroupPermissionsWrapper.ownerPermissions());

        // EzGroups indices we are going to request.  We add in the indices Users here which, while map to something
        // in EzGroups, they do not map to a group in EzGroups so should be ignored.
        final Set<Long> requested = Sets.newHashSet(Sets.newHashSet(group1.getId(), group2.getId(), userId1, userId2));

        final Map<Long, String> expected = EzGroupsService.getUnloadedGroupIndexToNameMap(requested);

        // shouldn't get any groups back without the issuedTo field set on the token (identifies requesting app)
        assertEquals(expected, client.getGroupNamesByIndices(userToken, requested));

        // set token so that is issued to the app we created, this means that both the app
        // and the user will have permission on group two, but only the user will have permission on group one.
        // we should only get group two back.
        userToken.getValidity().setIssuedTo(appUser);
        expected.put(group2.getId(), group2.getGroupName());
        assertEquals(expected, client.getGroupNamesByIndices(userToken, requested));

        // If we add the app to the group its missing, then we can expect to get both groups back.
        client.addAppUserToGroup(
                adminToken, group1.getGroupName(), appUser, UserGroupPermissionsWrapper.ownerPermissions());

        expected.put(group1.getId(), group1.getGroupName());
        assertEquals(expected, client.getGroupNamesByIndices(userToken, requested));

        // use the admin token to get all groups.
        assertEquals(expected, client.getGroupNamesByIndices(adminToken, requested));
    }


    /**
     * Builds a group object for comparison in tests.
     *
     * @param expectedIndex expected index of the group
     * @param groupName group name
     * @param friendlyGroupName friendly group name
     * @return a newly build Group object with default values
     */
    private Group getExpectedGroup(long expectedIndex, String groupName, String friendlyGroupName) {
        final Group expectedMyGroup = new Group();
        expectedMyGroup.setGroupName(groupName);
        expectedMyGroup.setInheritancePermissions(new GroupInheritancePermissions());
        expectedMyGroup.setFriendlyName(friendlyGroupName);
        expectedMyGroup.setRequireOnlyUser(true);
        expectedMyGroup.setRequireOnlyAPP(false);
        expectedMyGroup.setIsActive(true);
        expectedMyGroup.setId(expectedIndex);
        return expectedMyGroup;
    }

}
