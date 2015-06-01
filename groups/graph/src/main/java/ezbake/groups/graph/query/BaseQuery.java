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

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Iterator;

public class BaseQuery {
    private static final Logger logger = LoggerFactory.getLogger(BaseQuery.class);

    protected FramedGraph<TitanGraph> graph;

    @Inject
    public BaseQuery(FramedGraph<TitanGraph> graph) {
        this.graph = graph;
    }

    public FramedGraph<TitanGraph> getGraph() {
        return graph;
    }

    /**
     * Query the graph for the group by name
     *
     * @param groupName fully qualified group name
     * @return the group object
     * @throws ezbake.groups.graph.exception.VertexNotFoundException
     */
    public Group getGroup(String groupName) throws VertexNotFoundException {
        // Get the group vertex
        Iterator<Group> g = graph.query()
                .has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .has(BaseVertex.TYPE, Compare.EQUAL, BaseVertex.VertexType.GROUP.toString())
                .limit(1)
                .vertices(Group.class).iterator();
        if (!g.hasNext()) {
            graph.getBaseGraph().rollback();
            throw new VertexNotFoundException("Group not found: " + groupName);
        }

        return g.next();
    }

    /**
     * Finds and returns an User-vertex of the given type, by the given ID.
     *
     * @param userType type of user, USER or APP_USER
     * @param id principal/id of the user to retrieve
     * @return an User-vertex by the given ID
     * @throws ezbake.groups.graph.exception.UserNotFoundException if the user cannot be found
     */
    public User findAndRetrieveUserById(BaseVertex.VertexType userType, String id) throws UserNotFoundException {
        Iterator<User> ownerit = graph.query()
                .limit(1)
                .has(User.TYPE, userType.toString())
                .has(User.PRINCIPAL, id)
                .vertices(User.class).iterator();

        if (!ownerit.hasNext()) {
            graph.getBaseGraph().rollback();
            final String errMsg = String.format(String.format("User: '%s' of type: '%s' not found.", id, userType));
            logger.error(errMsg);
            throw new UserNotFoundException(errMsg);
        }

        return ownerit.next();
    }

    /**
     * Determine whether or not a path exists between two vertices along a particular edge
     *
     * Breadth first search should be used, to search for a direct edge
     *
     * @param sourceId Vertex ID of the source vertex (has out edge)
     * @param targetId Vertex ID of the destination vertex (has in edge)
     * @param edgeLabel label of the edge to traverse
     * @return true if a path exists
     */
    public boolean pathExists(Object sourceId, final Object targetId, String... edgeLabel) {
        return pathExists(graph.getVertex(sourceId), targetId, edgeLabel);
    }

    /**
     * Determine whether or not a path exists between two vertices along a particular edge
     *
     * @param source the source vertex (has out edge)
     * @param target Vertex ID of the destination vertex (has in edge)
     * @param edgeLabel label of the edge to traverse
     * @return true if a path exists, otherwise false
     */
    public boolean pathExists(Vertex source, final Vertex target, String... edgeLabel) {
        return pathExists(source, target.getId(), edgeLabel);
    }

    /**
     * Determine whether or not an outgoing path exists between a start vertex and target along a particular edge.
     *
     * Breadth first search should be used to search for direct edges.
     *
     * @param source the source vertex (has out edge)
     * @param targetId Vertex ID of the destination vertex (has in edge)
     * @param edgeLabel label of the edge to traverse
     * @return true if a path exists
     */
    public boolean pathExists(Vertex source, final Object targetId, String... edgeLabel) {
        final String findPathByLabel = "findPathByLabel";

        GremlinPipeline<Vertex, Vertex> pip = new GremlinPipeline<Vertex, Vertex>(source)
                .as(findPathByLabel)
                .outE(edgeLabel)
                .gather().scatter()
                .inV()
                .loop(
                        findPathByLabel, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> loopBundle) {
                                return !loopBundle.getObject().getId().equals(targetId);
                            }
                        });
        return pip.hasNext();
    }
}
