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
import ezbake.groups.graph.api.GroupIDProvider;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbake.groups.graph.query.SpecialAppGroupQuery;
import org.easymock.EasyMock;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * User: jhastings
 * Date: 7/29/14
 * Time: 11:58 PM
 */
public class TransactionRollbackTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public final static String graphConfig = "/graphconfig.properties";

    public EzConfiguration ezConfiguration;
    public EzGroupsGraph graph;
    public StubIDProvider idProvider;
    public SpecialAppGroupQuery query;

    @Before
    public void setUp() throws Exception {
        ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader(graphConfig));
        ezConfiguration.getProperties().setProperty("storage.directory", folder.getRoot().toString());

        GraphDatabaseConfiguration conf = new GraphDatabaseConfiguration(new TitanGraphConfiguration(ezConfiguration.getProperties()));
        conf.getBackend().clearStorage();

        idProvider = new StubIDProvider();

        graph = new EzGroupsGraph(ezConfiguration.getProperties(), new TitanGraphProvider(ezConfiguration.getProperties()).get(), idProvider);
        query = new SpecialAppGroupQuery(graph.getFramedGraph(), graph.appGroupId);
    }

    @After
    public void wipeGraph() throws StorageException, IOException {
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
