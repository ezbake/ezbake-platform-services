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

package ezbake.groups.cli.commands.group;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * User: jhastings
 * Date: 9/23/14
 * Time: 2:52 PM
 */
public class GetGroupUsersCommand extends GroupCommand {

    public GetGroupUsersCommand() { }

    public GetGroupUsersCommand(String groupName, String user, Properties properties) {
        super(properties);
        this.user = user;
        this.groupName = groupName;
    }

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();

        String internalName = nameHelper.addRootGroupPrefix(groupName);
        try {
            Group group = graph.getGroup(internalName);

            List<String> edges = Lists.newArrayList();
            for (BaseEdge.EdgeType edge : BaseEdge.EdgeType.values()) {
                edges.add(edge.toString());
            }

            GremlinPipeline<Object, Vertex> pipe = new GremlinPipeline<>(group.asVertex())
                    .as("group_member_traversal")
                    .inE(edges.toArray(new String[edges.size()]))
                    .outV()
                    .loop("group_member_traversal", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            return true;
                        }
                    }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            boolean isUser = BaseVertex.VertexType.USER.toString().equals(vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE));
                            boolean isApp = BaseVertex.VertexType.APP_USER.toString().equals(vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE));
                            return isUser || isApp;
                        }
                    }).dedup();

            Set<User> users = Sets.newHashSet();
            FramedGraph<TitanGraph> framedGraph = graph.getFramedGraph();
            while(pipe.hasNext()) {
                User member = framedGraph.frame(pipe.next(), User.class);
                users.add(member);
            }

            System.out.println("Group members for group: " + groupName);
            for (User user : users) {
                System.out.println("User principal: " + user.getPrincipal() + ", name: " + user.getName());
            }

        } catch (VertexNotFoundException e) {
            System.err.println("Unable to get group members: " + e.getMessage());
        } finally {
            try {
                graph.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
