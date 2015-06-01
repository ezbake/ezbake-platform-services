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

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift.TException;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzSecurityPrincipal;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.ValidityCaveats;
import ezbake.base.thrift.metrics.MetricRegistryThrift;
import ezbake.groups.common.Cachable;
import ezbake.groups.common.Queryable;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.query.GroupMembersQuery;
import ezbake.groups.service.caching.CacheLayer;
import ezbake.groups.service.query.AuthorizationQuery;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.Group;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.groups.thrift.GroupsRequest;
import ezbake.groups.thrift.UserGroupPermissions;
import ezbake.groups.thrift.UserGroupsRequest;
import ezbake.groups.thrift.UserType;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.test.MockEzSecurityToken;

@SuppressWarnings("JUnitTestMethodWithNoAssertions")
public final class CachingGroupsServiceTest extends EasyMockSupport {

    // Commonly used strings
    private static final String TEST_USERID = "userid";
    private static final String TEST_APPID = "appId";
    private static final String TEST_GROUPNAME = "app.myapp";
    private static final String PARENT_GROUP = "group1";
    private static final String CHILD_GROUP = "group2";
    private static final String USER1_ID = "user1";
    private static final String USER_2_ID = "user2";

    private static final EzSecurityToken USER1_TOKEN = MockEzSecurityToken.getMockUserToken(USER1_ID);
    private static final EzSecurityToken TEST_TOKEN =
            new EzSecurityToken(null, TokenType.USER, new EzSecurityPrincipal("test user", null));

    private static final long TEST_INDEX = 4L;
    private static final String APPUSER_ID = "appuser";

    private Properties properties;

    // members refreshed every test; most of them are mocks
    private GroupInheritancePermissions defaultGroupInheritance;
    private EzbakeSecurityClient client;
    private EzGroupsService mockEzGroupsService;
    private GroupsGraph mockGroupsGraph;
    private CacheLayer mockCacheLayer;
    private GroupsGraph niceGroupsGraphMock;
    private GroupMembersQuery mockGroupMemberQuery;
    private CachingEzGroupsService cachingEzGroupsService;

    /**
     * Mocks need to be created before we start recording actual test results. Some mocks in setup are just here for
     * brevity, but not used in all tests.
     */
    @Before
    public void setUp() throws EzSecurityTokenException {
        client = createMock(EzbakeSecurityClient.class);
        client.validateReceivedToken(anyObject(EzSecurityToken.class));
        expectLastCall().anyTimes();

        properties = new Properties();
        properties.setProperty(BaseGroupsService.X509_RESTRICT, Boolean.FALSE.toString());

        defaultGroupInheritance = new GroupInheritancePermissions();
        mockEzGroupsService = createMock(EzGroupsService.class);
        mockGroupsGraph = createMock(GroupsGraph.class);
        mockCacheLayer = createMock(CacheLayer.class);
        niceGroupsGraphMock = createNiceMock(GroupsGraph.class);
        mockGroupMemberQuery = createMock(GroupMembersQuery.class);

        cachingEzGroupsService = new CachingEzGroupsService(
                mockEzGroupsService, mockCacheLayer, properties, client, mockGroupMemberQuery);
    }

    @Test
    public void testCreateAndGetGroup() throws Exception {
        final Group returnedGroup = new Group();
        returnedGroup.setGroupName(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(PARENT_GROUP, CHILD_GROUP));
        returnedGroup.setInheritancePermissions(defaultGroupInheritance);

        final Capture<String> updateKey = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        members.add(getUser(BaseVertex.VertexType.USER, USER_2_ID));

        expect(
                mockEzGroupsService.createAndGetGroup(
                        TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance)).andReturn(returnedGroup)
                .once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(
                                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(
                                        EzGroupsConstants.ROOT, PARENT_GROUP, CHILD_GROUP)))).andReturn(members).once();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(updateKey), anyString()));
        expectLastCall().times(2);

        replayAll();

        cachingEzGroupsService.createAndGetGroup(TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID), updateKey.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER_2_ID), updateKey.getValues().get(1));

        verifyAll();
    }

    @Test
    public void testCreateAndGetGroupWithInclusion() throws Exception {
        final Group returnedGroup = new Group();
        returnedGroup.setGroupName(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(PARENT_GROUP, CHILD_GROUP));
        returnedGroup.setInheritancePermissions(defaultGroupInheritance);

        final Capture<String> updateKey = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        members.add(getUser(BaseVertex.VertexType.USER, USER_2_ID));

        expect(
                mockEzGroupsService.createAndGetGroupWithInclusion(
                        TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance, true, false))
                .andReturn(returnedGroup).once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(
                                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(
                                        EzGroupsConstants.ROOT, PARENT_GROUP, CHILD_GROUP)))).andReturn(members).once();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(updateKey), anyString()));
        expectLastCall().times(2);

        replayAll();

        cachingEzGroupsService.createAndGetGroupWithInclusion(
                TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance, true, false);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID), updateKey.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER_2_ID), updateKey.getValues().get(1));

        verifyAll();
    }

    @Test
    public void testCreateGroupWithInclusion() throws Exception {
        final long testId = TEST_INDEX;

        final Group returnedGroup = new Group();
        returnedGroup.setGroupName(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(PARENT_GROUP, CHILD_GROUP));
        returnedGroup.setInheritancePermissions(defaultGroupInheritance);
        returnedGroup.setId(testId);

        final Capture<String> updateKey = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        members.add(getUser(BaseVertex.VertexType.USER, USER_2_ID));

        expect(
                mockEzGroupsService.createAndGetGroupWithInclusion(
                        TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance, true, false))
                .andReturn(returnedGroup).once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(
                                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(
                                        EzGroupsConstants.ROOT, PARENT_GROUP, CHILD_GROUP)))).andReturn(members).once();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(updateKey), anyString()));
        expectLastCall().times(2);

        replayAll();

        final long resultId = cachingEzGroupsService.createGroupWithInclusion(
                TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance, true, false);

        assertEquals(testId, resultId);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID), updateKey.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER_2_ID), updateKey.getValues().get(1));

        verifyAll();
    }

    @Test
    public void testCreateGroup() throws Exception {
        final long testId = TEST_INDEX;

        final Group returnedGroup = new Group();
        returnedGroup.setGroupName(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(PARENT_GROUP, CHILD_GROUP));
        returnedGroup.setInheritancePermissions(defaultGroupInheritance);
        returnedGroup.setId(testId);

        final Capture<String> updateKey = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        members.add(getUser(BaseVertex.VertexType.USER, USER_2_ID));

        expect(
                mockEzGroupsService.createAndGetGroup(
                        TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance)).andReturn(returnedGroup)
                .once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(
                                Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(
                                        EzGroupsConstants.ROOT, PARENT_GROUP, CHILD_GROUP)))).andReturn(members).once();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(updateKey), anyString()));
        expectLastCall().times(2);

        replayAll();

        final long resultId = cachingEzGroupsService.createGroup(
                TEST_TOKEN, PARENT_GROUP, CHILD_GROUP, defaultGroupInheritance);

        assertEquals(testId, resultId);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID), updateKey.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER_2_ID), updateKey.getValues().get(1));

        verifyAll();
    }

    @Test
    public void testCreateGroupNoParent() throws Exception {
        final String parentGroup = null;
        final Group returnedGroup = new Group();
        returnedGroup.setGroupName(CHILD_GROUP);
        returnedGroup.setInheritancePermissions(defaultGroupInheritance);
        returnedGroup.setId(TEST_INDEX);

        final Capture<String> updateKey = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        members.add(getUser(BaseVertex.VertexType.USER, USER_2_ID));

        expect(
                mockEzGroupsService.createAndGetGroup(
                        TEST_TOKEN, parentGroup, CHILD_GROUP, defaultGroupInheritance)).andReturn(returnedGroup).once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, CHILD_GROUP))))
                .andReturn(
                        members).once();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(updateKey), anyString()));
        expectLastCall().times(2);

        replayAll();

        final long resultId =
                cachingEzGroupsService.createGroup(TEST_TOKEN, parentGroup, CHILD_GROUP, defaultGroupInheritance);

        assertEquals(TEST_INDEX, resultId);
        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID), updateKey.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER_2_ID), updateKey.getValues().get(1));

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to actually deactivate the group
     */
    @Test
    public void testDeactivateGroup() throws Exception {
        final Capture<String> authorizationQueryCapture = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();

        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        mockEzGroupsService.deactivateGroup(USER1_TOKEN, TEST_GROUPNAME, true);
        expectLastCall().once();

        mockEzGroupsService.deactivateGroup(USER1_TOKEN, TEST_GROUPNAME, false);
        expectLastCall().once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, TEST_GROUPNAME))))
                .andReturn(members).times(
                2);

        expect(mockEzGroupsService.getGraph()).andReturn(mockGroupsGraph).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(authorizationQueryCapture), anyString()));
        expectLastCall().times(2);

        replayAll();

        cachingEzGroupsService.deactivateGroup(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.deactivateGroup(USER1_TOKEN, TEST_GROUPNAME, false);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID),
                authorizationQueryCapture.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID),
                authorizationQueryCapture.getValues().get(1));

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to actually activate the group
     */
    @Test
    public void testActivateGroup() throws Exception {
        final Capture<String> authorizationQueryCapture = new Capture<>(CaptureType.ALL);
        final Set<User> members = Sets.newLinkedHashSet();

        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        mockEzGroupsService.activateGroup(USER1_TOKEN, TEST_GROUPNAME, true);
        expectLastCall().once();

        mockEzGroupsService.activateGroup(USER1_TOKEN, TEST_GROUPNAME, false);
        expectLastCall().once();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, TEST_GROUPNAME))))
                .andReturn(members).times(
                2);

        expect(mockEzGroupsService.getGraph()).andReturn(mockGroupsGraph).times(2);
        mockCacheLayer.markAllForUpdate(and(capture(authorizationQueryCapture), anyString()));
        expectLastCall().times(2);

        replayAll();

        cachingEzGroupsService.activateGroup(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.activateGroup(USER1_TOKEN, TEST_GROUPNAME, false);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID),
                authorizationQueryCapture.getValues().get(0));

        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID),
                authorizationQueryCapture.getValues().get(1));

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to actually change the inheritance
     */
    @Test
    public void testChangeGroupInheritance() throws Exception {
        final Set<User> members = Sets.newLinkedHashSet();
        members.add(getUser(BaseVertex.VertexType.USER, USER1_ID));
        final Capture<String> authorizationQueryCapture = new Capture<>();

        expect(
                mockGroupMemberQuery.getGroupMembers(
                        eq(Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, TEST_GROUPNAME))))
                .andReturn(members).times(2);

        mockEzGroupsService.changeGroupInheritance(eq(USER1_TOKEN), eq(TEST_GROUPNAME), eq(defaultGroupInheritance));
        expectLastCall().once();

        expect(mockEzGroupsService.getGraph()).andReturn(mockGroupsGraph).once();
        mockCacheLayer.markAllForUpdate(and(capture(authorizationQueryCapture), anyString()));
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.changeGroupInheritance(USER1_TOKEN, TEST_GROUPNAME, defaultGroupInheritance);

        // Make sure it updates the cache for the user(s)
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, USER1_ID),
                authorizationQueryCapture.getValue());

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to actually change the group name
     */
    @Test
    public void testChangeGroupName() throws Exception {
        final String newName = "boohoo";

        mockEzGroupsService.changeGroupName(eq(USER1_TOKEN), eq(TEST_GROUPNAME), eq(newName));
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.changeGroupName(USER1_TOKEN, TEST_GROUPNAME, newName);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to actually get the mask
     */
    @Test
    public void testGetGroupsMask() throws Exception {
        final Set<String> groupNames = Sets.newHashSet("group1", "group2", "group3");
        final Set<String> userPrincipals = Sets.newHashSet(USER1_ID, USER_2_ID, "user3");
        final Set<String> appUserIds = Sets.newHashSet("appId1", "appId2", "appId3");
        final Set<Long> ids = Sets.newHashSet(1L, 2L, 3L);

        expect(
                mockEzGroupsService.getGroupsMask(eq(USER1_TOKEN), eq(groupNames), eq(userPrincipals), eq(appUserIds)))
                .andReturn(
                        ids).once();

        replayAll();

        assertEquals(ids, cachingEzGroupsService.getGroupsMask(USER1_TOKEN, groupNames, userPrincipals, appUserIds));

        verifyAll();
    }

    /**
     * Make sure creating a user does all the right stuff and that it updates the right cache keys
     */
    @Test
    public void testCreateUser() throws Exception {
        final String name = "username";
        final Capture<AuthorizationQuery> updateKey = new Capture<>();

        expect(mockEzGroupsService.createUser(TEST_TOKEN, TEST_USERID, name)).andReturn(TEST_INDEX).once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.updateAll((Queryable) and(capture(updateKey), anyObject()));
        expectLastCall();

        replayAll();

        assertEquals(TEST_INDEX, cachingEzGroupsService.createUser(TEST_TOKEN, TEST_USERID, name));
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue().getWildCardKey());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when modifying a userid, especially invalidating the caches
     */
    @Test
    public void testModifyUser() throws Exception {
        // set a new user ID
        mockEzGroupsService.modifyUser(TEST_TOKEN, TEST_USERID, USER_2_ID);
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.modifyUser(TEST_TOKEN, TEST_USERID, USER_2_ID);

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when deactivating a user, especially invalidating the caches
     */
    @Test
    public void testDeactivateUser() throws Exception {
        final Capture<String> updateKey = new Capture<>();

        mockEzGroupsService.deactivateUser(TEST_TOKEN, TEST_USERID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.invalidateAll(and(capture(updateKey), anyString()));
        expectLastCall();

        replayAll();

        cachingEzGroupsService.deactivateUser(TEST_TOKEN, TEST_USERID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when activating a user, especially rebuilding the cache
     */
    @Test
    public void testActivateUser() throws Exception {
        final Capture<AuthorizationQuery> updateKey = new Capture<>();

        mockEzGroupsService.activateUser(TEST_TOKEN, TEST_USERID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.updateAll(and(capture(updateKey), (AuthorizationQuery) anyObject()));
        expectLastCall();

        replayAll();

        cachingEzGroupsService.activateUser(TEST_TOKEN, TEST_USERID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue().getWildCardKey());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when deleting a user, especially invalidating the caches
     */
    @Test
    public void testDeleteUser() throws Exception {
        final Capture<String> updateKey = new Capture<>();

        mockEzGroupsService.deleteUser(TEST_TOKEN, TEST_USERID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.invalidateAll(and(capture(updateKey), anyString()));
        expectLastCall();

        replayAll();

        cachingEzGroupsService.deleteUser(TEST_TOKEN, TEST_USERID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure creating an app user does all the right stuff and that it updates the right cache keys
     */
    @Test
    public void testCreateAppUser() throws Exception {
        final Capture<AuthorizationQuery> updateKey = new Capture<>();

        expect(mockEzGroupsService.createAppUser(TEST_TOKEN, TEST_APPID, "app name")).andReturn(TEST_INDEX).once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.updateAll((Queryable) and(capture(updateKey), anyObject()));
        expectLastCall();

        replayAll();

        assertEquals(TEST_INDEX, cachingEzGroupsService.createAppUser(TEST_TOKEN, TEST_APPID, "app name"));
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, TEST_APPID), updateKey.getValue().getWildCardKey());

        verifyAll();
    }

    /**
     * Make sure create app user calls down to the right methods both with a cache hit and a miss
     */
    @Test
    public void testCreateAppUserAndGetAuthorizations() throws Exception {
        final EzSecurityToken token = new EzSecurityToken(
                new ValidityCaveats("EzSecurity", "EzGroups", System.currentTimeMillis(), ""), TokenType.USER,
                new EzSecurityPrincipal("test user", null));
        final List<String> chain = Lists.newArrayList();
        final String name = "app name";
        final Set<Long> auths = Sets.newHashSet(TEST_INDEX);

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        expect(mockCacheLayer.get(anyObject(AuthorizationQuery.class))).andThrow(new Exception()).once()
                .andReturn(auths).once();

        expect(mockEzGroupsService.createAppUserAndGetAuthorizations(token, chain, TEST_APPID, name)).andReturn(auths)
                .once();

        replayAll();

        // First time cache miss
        assertEquals(
                auths, cachingEzGroupsService.createAppUserAndGetAuthorizations(token, chain, TEST_APPID, name));
        // Second time cache hit
        assertEquals(
                auths, cachingEzGroupsService.createAppUserAndGetAuthorizations(token, chain, TEST_APPID, name));

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when modifying an app user
     */
    @Test
    public void testModifyAppUser() throws Exception {
        final String newId = "appid2";
        final String newName = "app name2";

        mockEzGroupsService.modifyAppUser(TEST_TOKEN, TEST_APPID, newId, newName);
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.modifyAppUser(TEST_TOKEN, TEST_APPID, newId, newName);

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when deactivating an app user, especially invalidating the caches
     */
    @Test
    public void testDeactivateAppUser() throws Exception {
        final Capture<String> updateKey = new Capture<>();

        mockEzGroupsService.deactivateAppUser(TEST_TOKEN, TEST_APPID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.invalidateAll(and(capture(updateKey), anyString()));
        expectLastCall();

        replayAll();

        cachingEzGroupsService.deactivateAppUser(TEST_TOKEN, TEST_APPID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, TEST_APPID), updateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when activating an app user, especially rebuilding the cache
     */
    @Test
    public void testActivateAppUser() throws Exception {
        final Capture<AuthorizationQuery> updateKey = new Capture<>();

        mockEzGroupsService.activateAppUser(TEST_TOKEN, TEST_APPID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.updateAll(and(capture(updateKey), (AuthorizationQuery) anyObject()));
        expectLastCall();
        replayAll();

        cachingEzGroupsService.activateAppUser(TEST_TOKEN, TEST_APPID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, TEST_APPID), updateKey.getValue().getWildCardKey());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when deleting an app user, especially invalidating the caches
     */
    @Test
    public void testDeleteAppUser() throws Exception {
        final Capture<String> updateKey = new Capture<>();

        mockEzGroupsService.deleteAppUser(TEST_TOKEN, TEST_APPID);
        expectLastCall().once();
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).once();
        mockCacheLayer.invalidateAll(and(capture(updateKey), anyString()));
        expectLastCall();

        replayAll();

        cachingEzGroupsService.deleteAppUser(TEST_TOKEN, TEST_APPID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, TEST_APPID), updateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when getting a user
     */
    @Test
    public void testGetUser() throws Exception {
        final UserType userType = UserType.USER;
        expect(mockEzGroupsService.getUser(TEST_TOKEN, userType, TEST_USERID)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUser(TEST_TOKEN, userType, TEST_USERID);

        verifyAll();
    }

    /**
     * Make sure the right stuff happens when getting an app user
     */
    @Test
    public void testGetAppUser() throws Exception {
        final UserType userType = UserType.APP_USER;
        expect(mockEzGroupsService.getUser(TEST_TOKEN, userType, TEST_APPID)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUser(TEST_TOKEN, userType, TEST_APPID);

        verifyAll();
    }

    /**
     * Make sure adding users to groups does the right stuff
     */
    @Test
    public void testAddUserToGroup() throws Exception {
        final UserGroupPermissions permissions = new UserGroupPermissions();
        final Capture<AuthorizationQuery> updateKey = new Capture<>();
        final Capture<String> invalidateKey = new Capture<>();

        mockEzGroupsService.addUserToGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID, permissions);
        expectLastCall().times(2);
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.updateAll(and(capture(updateKey), anyObject(AuthorizationQuery.class)));
        expectLastCall().once().andThrow(new Exception("test exception")).once();
        mockCacheLayer.invalidateAll(and(capture(invalidateKey), anyString()));
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.addUserToGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID, permissions);
        cachingEzGroupsService.addUserToGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID, permissions);
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue().getWildCardKey());
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), invalidateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure removing users from groups does the right stuff
     */
    @Test
    public void testRemoveUserFromGroup() throws Exception {
        final Capture<AuthorizationQuery> updateKey = new Capture<>();
        final Capture<String> invalidateKey = new Capture<>();

        mockEzGroupsService.removeUserFromGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID);
        expectLastCall().times(2);
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.updateAll(and(capture(updateKey), anyObject(AuthorizationQuery.class)));
        expectLastCall().once().andThrow(new Exception("test exception")).once();
        mockCacheLayer.invalidateAll(and(capture(invalidateKey), anyString()));
        expectLastCall().once();

        replayAll();

        final CachingEzGroupsService cachingService = cachingEzGroupsService;
        cachingService.removeUserFromGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID);
        cachingService.removeUserFromGroup(TEST_TOKEN, PARENT_GROUP, TEST_USERID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), updateKey.getValue().getWildCardKey());
        assertEquals(
                getCacheString(BaseVertex.VertexType.USER, TEST_USERID), invalidateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure adding app users to groups does the right stuff
     */
    @Test
    public void testAddAppUserToGroup() throws Exception {
        final UserGroupPermissions permissions = new UserGroupPermissions();

        final Capture<AuthorizationQuery> updateKey = new Capture<>();
        final Capture<String> invalidateKey = new Capture<>();

        mockEzGroupsService.addAppUserToGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID, permissions);
        expectLastCall().times(2);
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.updateAll(and(capture(updateKey), anyObject(AuthorizationQuery.class)));
        expectLastCall().once().andThrow(new Exception("test exception")).once();
        mockCacheLayer.invalidateAll(and(capture(invalidateKey), anyString()));
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.addAppUserToGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID, permissions);
        cachingEzGroupsService.addAppUserToGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID, permissions);
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, APPUSER_ID), updateKey.getValue().getWildCardKey());

        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, APPUSER_ID), invalidateKey.getValue());

        verifyAll();
    }

    /**
     * Make sure removing app users from groups does the right stuff
     */
    @Test
    public void testRemoveAppUserFromGroup() throws Exception {
        final Capture<AuthorizationQuery> updateKey = new Capture<>();
        final Capture<String> invalidateKey = new Capture<>();

        mockEzGroupsService.removeAppUserFromGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID);
        expectLastCall().times(2);
        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        mockCacheLayer.updateAll(and(capture(updateKey), anyObject(AuthorizationQuery.class)));
        expectLastCall().once().andThrow(new Exception("test exception")).once();
        mockCacheLayer.invalidateAll(and(capture(invalidateKey), anyString()));
        expectLastCall().once();

        replayAll();

        cachingEzGroupsService.removeAppUserFromGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID);
        cachingEzGroupsService.removeAppUserFromGroup(TEST_TOKEN, PARENT_GROUP, APPUSER_ID);
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, APPUSER_ID), updateKey.getValue().getWildCardKey());
        assertEquals(
                getCacheString(BaseVertex.VertexType.APP_USER, APPUSER_ID), invalidateKey.getValue());

        verifyAll();
    }

    @Test
    public void testGetGroups() throws TException {
        final GroupsRequest request = new GroupsRequest();
        expect(mockEzGroupsService.getGroups(TEST_TOKEN, request)).andReturn(null).times(1);

        replayAll();

        cachingEzGroupsService.getGroups(TEST_TOKEN, request);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get child groups
     */
    @Test
    public void testGetChildGroups() throws Exception {
        expect(mockEzGroupsService.getChildGroups(USER1_TOKEN, TEST_GROUPNAME, true)).andReturn(null).once();
        expect(mockEzGroupsService.getChildGroups(USER1_TOKEN, TEST_GROUPNAME, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getChildGroups(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.getChildGroups(USER1_TOKEN, TEST_GROUPNAME, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get group
     */
    @Test
    public void testGetGroup() throws EzSecurityTokenException {
        expect(mockEzGroupsService.getGroup(USER1_TOKEN, TEST_GROUPNAME)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getGroup(USER1_TOKEN, TEST_GROUPNAME);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get group members
     */
    @Test
    public void testGetGroupMembers() throws Exception {
        expect(mockEzGroupsService.getGroupMembers(USER1_TOKEN, TEST_GROUPNAME, true)).andReturn(null).once();
        expect(mockEzGroupsService.getGroupMembers(USER1_TOKEN, TEST_GROUPNAME, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getGroupMembers(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.getGroupMembers(USER1_TOKEN, TEST_GROUPNAME, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get group apps
     */
    @Test
    public void testGetGroupApps() throws Exception {
        expect(mockEzGroupsService.getGroupApps(USER1_TOKEN, TEST_GROUPNAME, true)).andReturn(null).once();
        expect(mockEzGroupsService.getGroupApps(USER1_TOKEN, TEST_GROUPNAME, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getGroupApps(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.getGroupApps(USER1_TOKEN, TEST_GROUPNAME, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get group users
     */
    @Test
    public void testGetGroupUsers() throws Exception {
        expect(mockEzGroupsService.getGroupUsers(USER1_TOKEN, TEST_GROUPNAME, true)).andReturn(null).once();
        expect(mockEzGroupsService.getGroupUsers(USER1_TOKEN, TEST_GROUPNAME, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getGroupUsers(USER1_TOKEN, TEST_GROUPNAME, true);
        cachingEzGroupsService.getGroupUsers(USER1_TOKEN, TEST_GROUPNAME, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service properly queries for authorizations
     */
    @Test
    public void testGetAuthorizations() throws Exception {
        final Capture<AuthorizationQuery> getCapture = new Capture<>();

        expect(mockEzGroupsService.getGraph()).andReturn(niceGroupsGraphMock).times(2);
        expect(
                mockCacheLayer.get(
                        and(
                                capture(getCapture), anyObject(AuthorizationQuery.class)))).andReturn(null).once()
                .andThrow(new Exception()).once();
        expect(mockEzGroupsService.getAuthorizations(USER1_TOKEN)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getAuthorizations(USER1_TOKEN);
        cachingEzGroupsService.getAuthorizations(USER1_TOKEN);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get user groups
     */
    @Test
    public void testGetUserGroups() throws Exception {
        expect(mockEzGroupsService.getUserGroups(USER1_TOKEN, true)).andReturn(null).once();
        expect(mockEzGroupsService.getUserGroups(USER1_TOKEN, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUserGroups(USER1_TOKEN, true);
        cachingEzGroupsService.getUserGroups(USER1_TOKEN, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to {@code requestUserGroups(...)}
     */
    @Test
    public void testRequestUserGroups() throws TException {
        final UserGroupsRequest request = new UserGroupsRequest();
        request.setIdentifier("nottested");

        expect(mockEzGroupsService.requestUserGroups(TEST_TOKEN, request)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.requestUserGroups(TEST_TOKEN, request);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get user diagnostic apps
     */
    @Test
    public void testGetUserDiagnosticApps() throws Exception {
        expect(mockEzGroupsService.getUserDiagnosticApps(USER1_TOKEN)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUserDiagnosticApps(USER1_TOKEN);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get user metrics apps
     */
    @Test
    public void testGetUserMetricsApps() throws Exception {
        expect(mockEzGroupsService.getUserMetricsApps(USER1_TOKEN)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUserMetricsApps(USER1_TOKEN);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get user auditor apps
     */
    @Test
    public void testGetUserAuditorApps() throws Exception {
        expect(mockEzGroupsService.getUserAuditorApps(USER1_TOKEN)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getUserAuditorApps(USER1_TOKEN);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get app user groups
     */
    @Test
    public void testGetAppUserGroups() throws Exception {
        expect(mockEzGroupsService.getAppUserGroups(USER1_TOKEN, true)).andReturn(null).once();
        expect(mockEzGroupsService.getAppUserGroups(USER1_TOKEN, false)).andReturn(null).once();

        replayAll();

        cachingEzGroupsService.getAppUserGroups(USER1_TOKEN, true);
        cachingEzGroupsService.getAppUserGroups(USER1_TOKEN, false);

        verifyAll();
    }

    /**
     * Make sure the caching groups service calls down to get user access to groups
     */
    @Test
    public void testCheckUserAccessToGroup() throws Exception {
        final String groupName = "group123";

        expect(mockEzGroupsService.checkUserAccessToGroup(USER1_TOKEN, groupName)).andReturn(false).once();

        replayAll();

        assertFalse(cachingEzGroupsService.checkUserAccessToGroup(USER1_TOKEN, groupName));

        verifyAll();
    }

    /**
     * Make sure the caching groups service does right stuff
     */
    @Test
    public void testCreateUserAndGetAuthorizations() throws Exception {
        final Set<Long> auths = Sets.newHashSet(1L, 2L);
        final List<String> chain = Lists.newArrayList("c1", "c2");
        final String user = "userrrr";
        final String name = "user name";

        final Capture<AuthorizationQuery> authorizationQueryCapture = new Capture<>();

        expect(mockEzGroupsService.getGraph()).andReturn(mockGroupsGraph).times(2);
        expect(
                mockCacheLayer.get(
                        and(
                                capture(authorizationQueryCapture), anyObject(AuthorizationQuery.class))))
                .andReturn(auths).once().andThrow(new Exception()).once();

        expect(mockEzGroupsService.createUserAndGetAuthorizations(USER1_TOKEN, chain, user, name)).andReturn(auths)
                .once();

        replayAll();

        assertEquals(
                auths, cachingEzGroupsService.createUserAndGetAuthorizations(USER1_TOKEN, chain, user, name));

        assertEquals(
                auths, cachingEzGroupsService.createUserAndGetAuthorizations(USER1_TOKEN, chain, user, name));

        verifyAll();
    }

    /**
     * Make sure the caching groups service does right stuff
     */
    @Test
    public void testGetUserAuthorizations() throws Exception {
        final Set<Long> auths = Sets.newHashSet(1L, 2L);
        final List<String> chain = Lists.newArrayList("c1", "c2");
        final String user = "userrrr";
        final Capture<AuthorizationQuery> authorizationQueryCapture = new Capture<>();

        expect(mockEzGroupsService.getGraph()).andReturn(mockGroupsGraph).times(2);
        expect(
                mockCacheLayer.get(
                        and(
                                capture(authorizationQueryCapture), anyObject(AuthorizationQuery.class))))
                .andReturn(auths).once().andThrow(new Exception()).once();

        expect(mockEzGroupsService.getUserAuthorizations(USER1_TOKEN, TokenType.USER, user, chain)).andReturn(auths)
                .once();

        replayAll();

        assertEquals(
                auths, cachingEzGroupsService.getUserAuthorizations(USER1_TOKEN, TokenType.USER, user, chain));

        assertEquals(
                auths, cachingEzGroupsService.getUserAuthorizations(USER1_TOKEN, TokenType.USER, user, chain));

        verifyAll();
    }

    @Test
    public void testGetGroupNameByIndices() throws EzSecurityTokenException, GroupQueryException {
        final Set<Long> auths = Sets.newHashSet(1L, 2L);
        expect(mockEzGroupsService.getGroupNamesByIndices(USER1_TOKEN, auths)).andReturn(null);

        replayAll();

        cachingEzGroupsService.getGroupNamesByIndices(USER1_TOKEN, auths);

        verifyAll();
    }

    @Test
    public void testPing() throws EzSecurityTokenException {
        expect(mockEzGroupsService.ping()).andReturn(true).once();

        replayAll();

        assertTrue(cachingEzGroupsService.ping());

        verifyAll();
    }

    @Test
    public void testGetMetricsRegistry() throws EzSecurityTokenException {
        expect(mockEzGroupsService.getMetricRegistryThrift()).andReturn(new MetricRegistryThrift()).once();

        replayAll();

        assertNotNull(cachingEzGroupsService.getMetricRegistryThrift());

        verifyAll();
    }

    @Test
    public void testClose() throws Exception {
        mockEzGroupsService.close();
        expectLastCall();
        mockCacheLayer.close();
        expectLastCall();

        replayAll();

        cachingEzGroupsService.close();

        verifyAll();
    }

    private static String getCacheString(BaseVertex.VertexType type, String id) {
        return Joiner.on(Cachable.KEY_SEPARATOR).join(type.toString(), id) + '*';
    }

    /**
     * Do not call this after replayAll() is called!
     */
    private User getUser(BaseVertex.VertexType type, String id) {
        final User user = createNiceMock(User.class);
        expect(user.getType()).andReturn(type).anyTimes();
        expect(user.getPrincipal()).andReturn(id).anyTimes();

        return user;
    }
}
