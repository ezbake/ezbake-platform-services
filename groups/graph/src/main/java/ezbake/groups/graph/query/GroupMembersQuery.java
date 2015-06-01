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
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;

import ezbake.groups.graph.PermissionEnforcer;
import ezbake.groups.graph.exception.AccessDeniedException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;

import javax.inject.Inject;
import java.util.Set;

public class GroupMembersQuery {

    private PermissionEnforcer permissionEnforcer;

    private BaseQuery query;
    @Inject
    public GroupMembersQuery(BaseQuery query, PermissionEnforcer permissionEnforcer) {
        this.query = query;
        this.permissionEnforcer = permissionEnforcer;
    }

    public Set<User> getGroupMembers(String groupName) throws VertexNotFoundException {
        return getGroupMembers(query.getGroup(groupName));
    }

    public Set<User> getGroupMembers(Group group) throws VertexNotFoundException {
        return getAllGroupMembers(group.asVertex());
    }

    public Set<User> getGroupMembers(String groupName, BaseVertex.VertexType ownerType, String ownerId) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException {
        return getGroupMembers(
                query.getGroup(groupName),
                query.findAndRetrieveUserById(ownerType, ownerId));
    }

    public Set<User> getGroupMembers(Group group, User owner) throws VertexNotFoundException, AccessDeniedException {
        final Set<User> users = Sets.newHashSet();

        // Check owner permissions if necessary
        if (owner != null) {
            // TODO: this should maybe happen elsewhere
            permissionEnforcer.validateAuthorized(owner, group, PermissionEnforcer.Permission.READ);
        }
        return getAllGroupMembers(group.asVertex());
    }

    protected Set<User> getAllGroupMembers(Vertex group) {
        final Set<User> users = Sets.newHashSet();
        new GremlinPipeline<Vertex, User>(group)
                .as("group_member_traversal")
                .inE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .outV()
                .loop("group_member_traversal", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        // The graph doesn't have loops
                        return true;
                    }
                }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        String vertexType = vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE);
                        return vertexType.equals(BaseVertex.VertexType.USER.toString()) ||
                                vertexType.equals(BaseVertex.VertexType.APP_USER.toString());
                    }
                })
                .dedup()
                .store(users, new PipeFunction<Vertex, User>() {
                    @Override
                    public User compute(Vertex vertex) {
                        return query.getGraph().frame(vertex, User.class);
                    }
                })
                .iterate();
        query.getGraph().getBaseGraph().commit();
        return users;
    }

}
