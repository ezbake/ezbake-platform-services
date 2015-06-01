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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.query.GroupQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public class AppUserCreationTest extends GraphCommonSetup {

    EzGroupsGraphImpl graph;
    @Before
    public void setUpTests() {
        graph = new EzGroupsGraphImpl(titanGraph, new NaiveIDProvider(), new GroupQuery(framedTitanGraph));
    }

    /************************************/
    /**      App User Creation         **/
    /************************************/

    @Test(expected=VertexExistsException.class)
    public void appUserDuplicationTest() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.APP_USER, "User1", "User One");
        Object user2 = graph.addUser(BaseVertex.VertexType.APP_USER, "User1", "User One");
    }

    @Test
    public void appUserBlankAppName() throws VertexExistsException, IndexUnavailableException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException {
        try {
            Object user1 = graph.addUser(BaseVertex.VertexType.APP_USER, "User1", "");
        } catch (InvalidGroupNameException e) {
            // Make sure the app user doesn't exist
            Iterator<Vertex> user = graph.getGraph().query()
                    .has(User.PRINCIPAL, "User1")
                    .vertices().iterator();
            Assert.assertFalse(user.hasNext());
        }
    }

    @Test
    public void appUserNullAppName() throws VertexExistsException, IndexUnavailableException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException {
        try {
            Object user1 = graph.addUser(BaseVertex.VertexType.APP_USER, "User1", null);
        } catch (InvalidGroupNameException e) {
            // Make sure the app user doesn't exist
            Iterator<Vertex> user = graph.getGraph().query()
                    .has(User.PRINCIPAL, "User1")
                    .vertices().iterator();
            Assert.assertFalse(user.hasNext());
        }
    }


    /*********************************/
    /** App Access Group assertions **/
    /*********************************/

    @Test
    public void appUsersGetAppAccessGroupTest() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex().getId();
        Object appGroup = graph.getGraph().query().has(Group.GROUP_NAME, Group.COMMON_GROUP+"."+Group.APP_ACCESS_GROUP+".Application One").vertices().iterator().next().getId();

        // Make sure app group's parent is the special app group
        Iterator<Edge> e = graph.getGraph().getVertex(appGroup).query().direction(Direction.IN).labels(Group.CHILD_GROUP).edges().iterator();
        if (e.hasNext()) {
            Vertex parent = e.next().getVertex(Direction.OUT);
            Assert.assertEquals(Group.COMMON_GROUP+"."+Group.APP_ACCESS_GROUP, parent.getProperty(Group.GROUP_NAME));
        } else {
            Assert.fail();
        }

        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", appGroup).hasNext());
    }

    @Test
    public void appUserHasDataAccessOnAppAccess() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Vertex app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex();
        Object appAccessId = graph.getGraph().getVertex(graph.appAccessGroupId).getId();

        Assert.assertTrue(new GremlinPipeline(app).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", appAccessId).hasNext());
        Assert.assertFalse(new GremlinPipeline(app).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", appAccessId).hasNext());
        Assert.assertFalse(new GremlinPipeline(app).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", appAccessId).hasNext());
        Assert.assertFalse(new GremlinPipeline(app).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", appAccessId).hasNext());
        Assert.assertFalse(new GremlinPipeline(app).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", appAccessId).hasNext());
    }

    @Test
    public void appAccessGroupInheritsDataRead() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex().getId();
        Object appGroupId = graph.getGraph().query().has(Group.GROUP_NAME, Group.COMMON_GROUP+"."+Group.APP_ACCESS_GROUP+".Application One").vertices().iterator().next().getId();
        Vertex appAccess = graph.getGraph().getVertex(graph.appAccessGroupId);

        Assert.assertTrue(new GremlinPipeline(appAccess).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", appGroupId).hasNext());
        Assert.assertFalse(new GremlinPipeline(appAccess).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", appGroupId).hasNext());
        Assert.assertFalse(new GremlinPipeline(appAccess).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", appGroupId).hasNext());
        Assert.assertFalse(new GremlinPipeline(appAccess).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", appGroupId).hasNext());
        Assert.assertFalse(new GremlinPipeline(appAccess).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", appGroupId).hasNext());
    }

    @Test(expected=AccessDeniedException.class)
    public void addChildGroupToAppAccessGroup() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex().getId();
        Vertex appGroup = graph.getGraph().query().has(Group.GROUP_NAME, Group.COMMON_GROUP+"."+Group.APP_ACCESS_GROUP+".Application One").vertices().iterator().next();

        graph.addGroup(BaseVertex.VertexType.APP_USER, "app1", "group", appGroup);

    }
    @Test(expected=AccessDeniedException.class)
    public void addChildGroupToAppAccessGroupAsRegularUser() throws InvalidVertexTypeException, AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex().getId();
        Vertex appGroup = graph.getGraph().query().has(Group.GROUP_NAME, Group.COMMON_GROUP+"."+Group.APP_ACCESS_GROUP+".Application One").vertices().iterator().next();
        Object user = graph.addUser(BaseVertex.VertexType.USER, "user1", "User One").asVertex().getId();

        graph.addGroup(BaseVertex.VertexType.USER, "user1", "group", appGroup);
    }


    /*******************************************/
    /**        App Group and sub groups       **/
    /*******************************************/

    @Test
    public void appUsersGetAppGroupTest() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object app = graph.addUser(BaseVertex.VertexType.APP_USER, "app1", "Application One").asVertex().getId();
        Object appGroup = graph.getGraph().query().has("groupName", Group.COMMON_GROUP+"."+Group.APP_GROUP+".Application One").vertices().iterator().next().getId();

        // Make sure app group's parent is the special app group
        Iterator<Edge> e = graph.getGraph().getVertex(appGroup).query().direction(Direction.IN).labels(Group.CHILD_GROUP).edges().iterator();
        if (e.hasNext()) {
            Vertex parent = e.next().getVertex(Direction.OUT);
            Assert.assertEquals(Group.COMMON_GROUP+"."+Group.APP_GROUP, parent.getProperty(Group.GROUP_NAME));
        } else {
            Assert.fail();
        }

        // Make sure app user has all the edges on the app group
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_MANAGE.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_READ.toString()).inV().has("id", appGroup).hasNext());
        Assert.assertTrue(new GremlinPipeline(graph.getGraph().getVertex(app)).outE(BaseEdge.EdgeType.A_WRITE.toString()).inV().has("id", appGroup).hasNext());
    }



}
