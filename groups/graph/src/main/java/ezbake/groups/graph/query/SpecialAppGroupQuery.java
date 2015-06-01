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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.thrift.EzGroupsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class SpecialAppGroupQuery {
    private static final Logger logger = LoggerFactory.getLogger(SpecialAppGroupQuery.class);

    private BaseQuery query;
    private String appGroupName;

    public SpecialAppGroupQuery(BaseQuery query) {
        this.query = query;
        appGroupName = new GroupNameHelper().addRootGroupPrefix(EzGroupsConstants.APP_GROUP);
    }

    /**
     * Queries for all apps for which the user is a member of the special group
     *
     *     appGroupId -> <appName> -> specialGroupName
     *
     * Where app name is the name of the application. User must have access to specialGroupName in order for the app
     * name to be returned
     *
     * @param specialGroupName
     * @param user
     * @return
     */
    public Set<String> specialAppNamesQuery(final String specialGroupName, final Vertex user) throws VertexNotFoundException {
        // This query starts at the apps group
        GremlinPipeline<Vertex, String> verts = new GremlinPipeline<Vertex, String>(
                query.getGroup(appGroupName).asVertex())
                // Go out from the apps group two levels
                .outE(Group.CHILD_GROUP)
                .inV()
                .as("appName")
                .outE(Group.CHILD_GROUP)
                .inV().has(Group.GROUP_NAME, new Predicate() {
                    @Override
                    public boolean evaluate(Object o, Object o2) {
                        return (o instanceof String) && (o2 instanceof String) && ((String) o).endsWith((String) o2);
                    }
                }, specialGroupName)
                .as("specialGroup")
                .inE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .gather()
                .scatter()
                .outV()
                .loop("specialGroup", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        return !vertexLoopBundle.getObject().getId().equals(user.getId());
                    }
                })
                .back("appName").property(Group.GROUP_NAME).transform(new PipeFunction<Object, String>() {
                    @Override
                    public String compute(Object o) {
                        List<String> parts = Splitter
                                .on(EzGroupsConstants.GROUP_NAME_SEP)
                                .omitEmptyStrings()
                                .splitToList((String) o);
                        return parts.get(parts.size() - 1);
                    }
                })
                ;

        return Sets.newHashSet(verts.iterator());
    }
}
