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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.service.caching.CacheLayer;
import ezbake.groups.service.query.AuthorizationQuery;
import ezbake.groups.thrift.*;
import ezbake.security.test.MockEzSecurityToken;
import org.apache.thrift.TException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.*;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CachingGroupsServiceIT extends GroupsServiceCommonITSetup {

    Jedis jedis;
    @Before
    public void setUpCachingGroupsServiceIT() {
        jedis = new Jedis("localhost", redisServer.getPort());
    }

    EzSecurityToken getAdminToken(String userId) {
        return MockEzSecurityToken.getMockUserToken(
                userId, "A", Collections.<String>emptySet(), Maps.<String, List<String>>newHashMap(), true);
    }

    String getGroupName(String... parts) {
        return Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(parts);
    }

    Iterable<String> stringSetFromLongs(Set<Long> longs) {
        return Iterables.transform(longs, new Function<Long, String>() {
            @Nullable
            @Override
            public String apply(Long aLong) {
                return Long.toString(aLong, 10);
            }
        });
    }

    @Test(expected=EzSecurityTokenException.class)
    public void testCreateUserNonAdmin() throws TException {
        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        String userId = "user_1234";
        String userName = "User 1234";
        client.createUser(MockEzSecurityToken.getMockUserToken("user"), userId, userName);
    }

    @Test
    public void testCreateUser() throws TException {
        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        String userId = "user_1234";
        String userName = "User 1234";

        long id = client.createUser(getAdminToken("admin"), userId, userName);

        // get user's auths from redis
        Set<String> auths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, userId, null).getKey());

        MatcherAssert.assertThat(auths, Matchers.containsInAnyOrder("0", Long.toString(id, 10)));
    }

    @Test(expected=EzSecurityTokenException.class)
    public void testCreateAppUserNonAdmin() throws TException {
        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        String userId = "user_1234";
        String userName = "User 1234";
        client.createAppUser(MockEzSecurityToken.getMockUserToken("user"), userId, userName);
    }

    @Test
    public void testCreateAppUser() throws TException, InterruptedException {
        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        String userId = "app_123";
        String userName = "App 1234";

        long id = client.createAppUser(getAdminToken("admin"), userId, userName);

        ezbake.groups.thrift.Group appAccess = client.getGroup(
                getAdminToken("admin"), getGroupName(EzGroupsConstants.ROOT, EzGroupsConstants.APP_ACCESS_GROUP));
        ezbake.groups.thrift.Group appAccessGroup = client.getGroup(
                getAdminToken("admin"), getGroupName(
                        EzGroupsConstants.ROOT, EzGroupsConstants.APP_ACCESS_GROUP, userName));
        ezbake.groups.thrift.Group appGroup = client.getGroup(
                getAdminToken("admin"), getGroupName(EzGroupsConstants.ROOT, EzGroupsConstants.APP_GROUP, userName));
        ezbake.groups.thrift.Group ezbD = client.getGroup(
                getAdminToken("admin"), getGroupName(
                        EzGroupsConstants.ROOT, EzGroupsConstants.APP_GROUP, userName, EzGroupsConstants.DIAGNOSTICS_GROUP));
        ezbake.groups.thrift.Group ezbM = client.getGroup(
                getAdminToken("admin"), getGroupName(
                        EzGroupsConstants.ROOT, EzGroupsConstants.APP_GROUP, userName, EzGroupsConstants.METRICS_GROUP));

        // get user's auths from redis
        Set<String> auths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.APP_USER, userId, null).getKey());

        MatcherAssert.assertThat(auths, Matchers.containsInAnyOrder(
                "0",
                Long.toString(appAccess.getId(), 10),
                Long.toString(appAccessGroup.getId(), 10),
                Long.toString(appGroup.getId(), 10),
                Long.toString(ezbD.getId(), 10),
                Long.toString(ezbM.getId(), 10),
                Long.toString(id, 10)));
    }

    @Test
    public void testCreateGroupNoParent() throws TException {
        String groupName = "my_group";
        String userId = "admin";
        EzSecurityToken adminToken = getAdminToken(userId);

        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);

        long user = client.createUser(adminToken, userId, userId);
        long groupId = client.createGroup(adminToken, null, groupName, new GroupInheritancePermissions());

        // Create group causes need for update
        Set<String> auths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, "admin", null).getKey());
        MatcherAssert.assertThat(
                auths,
                Matchers.containsInAnyOrder(CacheLayer.CacheStatusCodes.NEEDS_UPDATE.getValue()));
        // Querying for auths causes query to run
        MatcherAssert.assertThat(
                stringSetFromLongs(client.getAuthorizations(adminToken)),
                Matchers.containsInAnyOrder(
                        "0",
                        Long.toString(groupId, 10),
                        Long.toString(user, 10)));
    }

    @Test
    public void testDeactivateActivateGroup() throws TException {
        String groupName = "my_group";
        String userId = "admin";
        EzSecurityToken adminToken = getAdminToken(userId);

        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        long user = client.createUser(adminToken, userId, userId);
        long groupId = client.createGroup(adminToken, null, groupName, new GroupInheritancePermissions());


        // Deactivate causes need for update
        client.deactivateGroup(adminToken, groupName, false);
        MatcherAssert.assertThat(
                jedis.smembers(new AuthorizationQuery(null, BaseVertex.VertexType.USER, "admin", null).getKey()),
                Matchers.containsInAnyOrder(CacheLayer.CacheStatusCodes.NEEDS_UPDATE.getValue()));
        // Running the query returns the actual auths
        MatcherAssert.assertThat(
                stringSetFromLongs(client.getAuthorizations(adminToken)),
                Matchers.containsInAnyOrder(
                        "0",
                        Long.toString(user, 10)));

        // Activate causes need for update
        client.activateGroup(adminToken, groupName, false);
        MatcherAssert.assertThat(jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, "admin", null).getKey()),
                Matchers.containsInAnyOrder(CacheLayer.CacheStatusCodes.NEEDS_UPDATE.getValue()));
        MatcherAssert.assertThat(
                stringSetFromLongs(client.getAuthorizations(adminToken)),
                Matchers.containsInAnyOrder(
                        "0",
                        Long.toString(groupId, 10),
                        Long.toString(user, 10)));

    }

    @Test
    public void testChangeInheritance() throws TException {
        String groupName = "my_group";
        String group2Name = "my_group2";
        String userId = "admin";
        String regularUser = "joe_user";
        EzSecurityToken adminToken = getAdminToken(userId);
        EzSecurityToken usertoken = MockEzSecurityToken.getMockUserToken(userId);
        EzSecurityToken regularToken = MockEzSecurityToken.getMockUserToken(regularUser);

        EzGroups.Client client = clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
        long user = client.createUser(adminToken, userId, userId);
        long user2 = client.createUser(adminToken, regularUser, userId);
        long groupId = client.createGroup(adminToken, null, groupName, new GroupInheritancePermissions());
        long groupId2 = client.createGroup(adminToken, groupName, group2Name, new GroupInheritancePermissions().setDataAccess(true));


        // In cache, user will need update
        Iterable<String> adminAuths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, userId, null).getKey());
        MatcherAssert.assertThat(adminAuths, Matchers.contains(CacheLayer.CacheStatusCodes.NEEDS_UPDATE.getValue()));

        // getAuthorizations will perform update and return full set
        MatcherAssert.assertThat(
                stringSetFromLongs(client.getAuthorizations(usertoken)),
                Matchers.containsInAnyOrder(
                        "0",
                        Long.toString(groupId, 10),
                        Long.toString(groupId2, 10),
                        Long.toString(user, 10)));

        // regular user will not have needed any updates
        Set<String> userAuths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, regularUser, null).getKey());
        MatcherAssert.assertThat(userAuths, Matchers.containsInAnyOrder(
                "0",
                Long.toString(user2, 10)));

        // Adding the user to a group will cause needs_update set
        client.addUserToGroup(adminToken, groupName, regularUser, new UserGroupPermissions().setDataAccess(true));
        userAuths = jedis.smembers(
            new AuthorizationQuery(null, BaseVertex.VertexType.USER, regularUser, null).getKey());
        MatcherAssert.assertThat(userAuths, Matchers.containsInAnyOrder(
                "0",
                Long.toString(groupId, 10),
                Long.toString(groupId2, 10),
                Long.toString(user2, 10)));

        // Change group inheritance causes a need for udpate
        client.changeGroupInheritance(adminToken, groupName + "." + group2Name, new GroupInheritancePermissions().setDataAccess(false));
        userAuths = jedis.smembers(
                new AuthorizationQuery(null, BaseVertex.VertexType.USER, regularUser, null).getKey());
        MatcherAssert.assertThat(
                userAuths,
                Matchers.containsInAnyOrder(CacheLayer.CacheStatusCodes.NEEDS_UPDATE.getValue()));
        // After get authorizations you get the new auths
        MatcherAssert.assertThat(
                stringSetFromLongs(client.getAuthorizations(regularToken)),
                Matchers.containsInAnyOrder(
                        "0",
                        Long.toString(groupId, 10),
                        Long.toString(user2, 10)));
    }
}
