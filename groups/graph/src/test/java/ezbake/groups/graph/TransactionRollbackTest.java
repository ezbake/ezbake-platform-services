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

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Vertex;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbake.groups.graph.query.GroupQuery;
import ezbake.groups.graph.query.SpecialAppGroupQuery;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;

public class TransactionRollbackTest extends GraphCommonSetup {

    public EzGroupsGraphImpl graph;
    public StubIDProvider idProvider;
    public SpecialAppGroupQuery query;

    @Before
    public void setUpTests() throws EzConfigurationLoaderException {
        idProvider = new StubIDProvider();
        GroupQuery groupQuery = new GroupQuery(framedTitanGraph);
        graph = new EzGroupsGraphImpl(titanGraph, idProvider, groupQuery);
        query = new SpecialAppGroupQuery(groupQuery.getBaseQuery());
    }

    @After
    public void closeGraph() throws StorageException, IOException {
        if (graph != null) {
            graph.close();
        }
    }

    @Test
    public void testRollbackIfIdUnavailableForAddUser() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, InvalidGroupNameException {
        idProvider.id = -1;
        try {
            graph.addUser(BaseVertex.VertexType.USER, "user1", "");
        } catch (IndexUnavailableException e) {
            Iterator<Vertex> v = graph.getGraph().query().has(User.PRINCIPAL, "user1").vertices().iterator();
            Assert.assertFalse(v.hasNext());
        }
    }

    @Test
    public void testRollbackIfAddGroup() throws InvalidVertexTypeException, AccessDeniedException, IndexUnavailableException, UserNotFoundException, VertexExistsException, InvalidGroupNameException {
        graph.addUser(BaseVertex.VertexType.USER, "user1", "");

        idProvider.id = -1;
        try {
            graph.addGroup(BaseVertex.VertexType.USER, "user1", "test");
        } catch (IndexUnavailableException e) {
            Iterator<Vertex> v = graph.getGraph().query().has(Group.GROUP_NAME, "root.test").vertices().iterator();
            Assert.assertFalse(v.hasNext());
        }

    }
}
