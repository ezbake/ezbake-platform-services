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

package ezbake.groups.service.query;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.common.Cachable;
import ezbake.groups.common.InvalidCacheKeyException;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.graph.TitanGraphConfiguration;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbake.groups.graph.query.GroupQuery;
import ezbake.groups.service.GraphCommonSetup;
import ezbake.groups.thrift.AuthorizationException;
import ezbake.groups.thrift.EzGroupOperationException;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.security.test.MockEzSecurityToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * This test needs to be rewritten to take the caching stuff into consideration
 */
public class AuthorizationQueryTest extends GraphCommonSetup {

    GroupsGraph graph;

    @Before
    public void setUpService() throws StorageException, EzConfigurationLoaderException, AuthorizationException, EzGroupOperationException, EzSecurityTokenException {
        graph = new EzGroupsGraphImpl(titanGraph, new NaiveIDProvider(), new GroupQuery(framedTitanGraph));
    }

    @Test
    public void testGetKey() {
        String user = "Dog";

        List<List<String>> chains = Lists.newArrayList();
        chains.add(Lists.newArrayList("a"));
        chains.add(Lists.newArrayList("a,b"));

        for (BaseVertex.VertexType type : BaseVertex.VertexType.values()) {
            AuthorizationQuery authorizationQuery = new AuthorizationQuery(graph, type, user, null);

            Assert.assertEquals(type.toString()+Cachable.KEY_SEPARATOR+user, authorizationQuery.getKey());
            Assert.assertEquals(type.toString()+Cachable.KEY_SEPARATOR+user+"*", authorizationQuery.getWildCardKey());

            for (List<String> chain : chains) {
                authorizationQuery = new AuthorizationQuery(graph, type, user, chain);
                Assert.assertEquals(
                        type.toString()+ Cachable.KEY_SEPARATOR+user+Cachable.KEY_SEPARATOR+Joiner.on(",").join(chain),
                        authorizationQuery.getKey());
                Assert.assertEquals(
                        type.toString()+ Cachable.KEY_SEPARATOR+user+"*",
                        authorizationQuery.getWildCardKey());
            }
        }
    }

    @Test
    public void testUpdateKey() throws InvalidCacheKeyException {
        AuthorizationQuery authorizationQuery = new AuthorizationQuery(graph, BaseVertex.VertexType.APP_USER, "user", null);

        authorizationQuery.updateInstanceByKey("APP_USER///bob///1,2,3");
        Assert.assertEquals(BaseVertex.VertexType.APP_USER, authorizationQuery.getType());
        Assert.assertEquals("bob", authorizationQuery.getId());
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"), authorizationQuery.getChain());

        authorizationQuery.updateInstanceByKey("APP_USER///bob");
        Assert.assertEquals(BaseVertex.VertexType.APP_USER, authorizationQuery.getType());
        Assert.assertEquals("bob", authorizationQuery.getId());
        Assert.assertNull(authorizationQuery.getChain());

    }

    @Test(expected=InvalidCacheKeyException.class)
    public void testInvalidKeyUpdate() throws InvalidCacheKeyException {
        AuthorizationQuery authorizationQuery = new AuthorizationQuery(graph, BaseVertex.VertexType.APP_USER, "user", null);
        // Partial key update fails
        authorizationQuery.updateInstanceByKey("APP_USER");
        Assert.assertEquals(BaseVertex.VertexType.USER, authorizationQuery.getType());
        Assert.assertEquals("bob", authorizationQuery.getId());
        Assert.assertNull(authorizationQuery.getChain());
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
        AuthorizationQuery authQuery1 = new AuthorizationQuery(graph, BaseVertex.VertexType.USER, user, Lists.newArrayList(app));
        AuthorizationQuery authQuery2 = new AuthorizationQuery(graph, BaseVertex.VertexType.USER, user, null);

        Set<Long> groups = authQuery1.runQuery();
        Set<Long> groupsNull = authQuery2.runQuery();

        Assert.assertEquals(groups, groupsNull);
        Assert.assertEquals(Sets.newHashSet(0l, jared.getIndex(), g1.getIndex(), g2.getIndex(), g3.getIndex()), groups);

        graph.setUserActiveOrNot(BaseVertex.VertexType.USER, user, false);
        AuthorizationQuery authQuery3 = new AuthorizationQuery(graph, BaseVertex.VertexType.USER, user, null);
        Set<Long> groupsDeactivated = authQuery3.runQuery();
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
        Set<Long> groups = new AuthorizationQuery(graph, BaseVertex.VertexType.USER, user, Lists.newArrayList(app)).runQuery();

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
        Set<Long> groupsAppDeactivated = new AuthorizationQuery(graph, BaseVertex.VertexType.USER, user, Lists.newArrayList(app)).runQuery();
        Assert.assertEquals(Sets.newHashSet(0l, jared.getIndex(), g1.getIndex(), g2.getIndex(), g3.getIndex()), groupsAppDeactivated);
    }
}
