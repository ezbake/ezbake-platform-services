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
import com.tinkerpop.frames.FramedGraph;

import javax.inject.Inject;

import ezbake.groups.graph.PermissionEnforcer;

public class GroupQuery {

    private BaseQuery query;
    private SpecialAppGroupQuery appGroupQuery;
    private GroupMembersQuery groupMembersQuery;
    private PermissionEnforcer permissionEnforcer;

    @Inject
    public GroupQuery(FramedGraph<TitanGraph> graph) {
        this.query = new BaseQuery(graph);
        this.appGroupQuery = new SpecialAppGroupQuery(query);
        this.permissionEnforcer = new PermissionEnforcer(query);
        this.groupMembersQuery = new GroupMembersQuery(query, permissionEnforcer);
    }

    public BaseQuery getBaseQuery() {
        return query;
    }

    public SpecialAppGroupQuery getAppGroupQuery() {
        return appGroupQuery;
    }

    public GroupMembersQuery getGroupMembersQuery() {
        return groupMembersQuery;
    }

    public PermissionEnforcer getPermissionEnforcer() {
        return permissionEnforcer;
    }
}

