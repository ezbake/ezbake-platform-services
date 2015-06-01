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

import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.GraphCommonSetup;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.UserGroupPermissions;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class GroupMembersQueryTest extends GraphCommonSetup {

    EzGroupsGraphImpl graph;
    GroupMembersQuery query;
    @Before
    public void setUpTests() {
        GroupQuery groupQuery = new GroupQuery(framedTitanGraph);
        graph = new EzGroupsGraphImpl(titanGraph, new NaiveIDProvider(), groupQuery);
        query = groupQuery.getGroupMembersQuery();
    }

    @Test
    public void testGetGroupMembers() throws VertexNotFoundException, VertexExistsException, IndexUnavailableException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, InvalidGroupNameException {
        String groupName = "test";

        User user = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        User user2 = graph.addUser(BaseVertex.VertexType.USER, "User2", "User Two");
        User user3 = graph.addUser(BaseVertex.VertexType.USER, "User3", "User Three");
        Group parent = graph.addGroup(BaseVertex.VertexType.USER, user.getPrincipal(), groupName, new GroupInheritancePermissions(false, true, true, true, true), new UserGroupPermissions(), true, false);

        Set<User> users = query.getGroupMembers("root."+groupName);

        System.out.println(users);
        for (User u : users) {
            //User u = graph.getFramedGraph().frame(v, User.class);
            System.out.println(u.getName());
        }

    }
}
