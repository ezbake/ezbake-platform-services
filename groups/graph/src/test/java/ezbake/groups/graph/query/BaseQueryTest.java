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
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphQuery;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.easymock.EasyMock.*;

public class BaseQueryTest {
    public static final String UNEXPECTED_EXCEPTION_MESSAGE = "Caught an unexpected exception";
    public static final String EXPECTED_EXCEPTION_MESSAGE = "Expected exception was not thrown";

    @Test
    @SuppressWarnings("unchecked")
    public void testGetGroup() {
        String groupName = "test.group";

        FramedGraph<TitanGraph> baseGraph = (FramedGraph<TitanGraph>)createMock(FramedGraph.class);
        TitanGraph baseGraph2 = createMock(TitanGraph.class);
        FramedGraphQuery query = createMock(FramedGraphQuery.class);
        Iterable<Group> iterable = (Iterable<Group>)createMock(Iterable.class);
        Iterator<Group> groups = (Iterator<Group>)createMock(Iterator.class);

        expect(baseGraph.query()).andReturn(query).times(2);
        expect(query.has(
                eq(Group.GROUP_NAME),
                anyObject(Predicate.class),
                eq(groupName)
        )).andReturn(query).anyTimes();
        expect(query.has(
                eq(BaseVertex.TYPE),
                anyObject(Predicate.class),
                eq(BaseVertex.VertexType.GROUP.toString())
        )).andReturn(query).anyTimes();
        expect(query.limit(1)).andReturn(query).anyTimes();
        expect(query.vertices(Group.class)).andReturn(iterable).anyTimes();
        expect(iterable.iterator()).andReturn(groups).anyTimes();
        expect(groups.hasNext())
                .andReturn(Boolean.TRUE).once()
                .andReturn(Boolean.FALSE).once();
        // returning null since group is an interface, shouldn't matter
        expect(groups.next()).andReturn(null).once();
        expect(baseGraph.getBaseGraph()).andReturn(baseGraph2).once();
        baseGraph2.rollback();
        expectLastCall().once();

        replay(baseGraph, baseGraph2, query, iterable, groups);

        BaseQuery groupQuery = new BaseQuery(baseGraph);
        try {
            Assert.assertEquals(null, groupQuery.getGroup(groupName));
        } catch (VertexNotFoundException e) {
            Assert.fail(UNEXPECTED_EXCEPTION_MESSAGE);
        }

        try {
            groupQuery.getGroup(groupName);
            Assert.fail(EXPECTED_EXCEPTION_MESSAGE);
        } catch (VertexNotFoundException e) {
            // caught as expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFindUser() {
        String userName = "testuser";
        String userType = BaseVertex.VertexType.USER.toString();

        FramedGraph<TitanGraph> baseGraph = (FramedGraph<TitanGraph>)createMock(FramedGraph.class);
        TitanGraph baseGraph2 = createMock(TitanGraph.class);
        FramedGraphQuery query = createMock(FramedGraphQuery.class);
        Iterable<User> iterable = (Iterable<User>)createMock(Iterable.class);
        Iterator<User> iterator = (Iterator<User>)createMock(Iterator.class);

        expect(baseGraph.query()).andReturn(query).times(2);
        expect(query.limit(1)).andReturn(query).anyTimes();
        expect(query.has(
                eq(User.TYPE),
                eq(userType)
        )).andReturn(query).anyTimes();
        expect(query.has(
                eq(User.PRINCIPAL),
                eq(userName)
        )).andReturn(query).anyTimes();
        expect(query.vertices(User.class)).andReturn(iterable).anyTimes();
        expect(iterable.iterator()).andReturn(iterator).anyTimes();
        expect(iterator.hasNext())
                .andReturn(Boolean.TRUE).once()
                .andReturn(Boolean.FALSE).once();
        // returning null since group is an interface, shouldn't matter
        expect(iterator.next()).andReturn(null).once();
        expect(baseGraph.getBaseGraph()).andReturn(baseGraph2).once();
        baseGraph2.rollback();
        expectLastCall().once();

        replay(baseGraph, baseGraph2, query, iterable, iterator);

        BaseQuery groupQuery = new BaseQuery(baseGraph);
        try {
            Assert.assertEquals(null, groupQuery.findAndRetrieveUserById(BaseVertex.VertexType.USER, userName));
        } catch (UserNotFoundException e) {
            Assert.fail(UNEXPECTED_EXCEPTION_MESSAGE);
        }

        try {
            groupQuery.findAndRetrieveUserById(BaseVertex.VertexType.USER, userName);
            Assert.fail(EXPECTED_EXCEPTION_MESSAGE);
        } catch (UserNotFoundException e) {
            // caught as expected
        }
    }
}
