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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.impl.NaiveIDProvider;
import ezbake.groups.graph.impl.TitanGraphProvider;
import ezbake.groups.graph.query.GroupMembersQuery;
import ezbake.groups.graph.query.GroupQuery;
import ezbake.groups.graph.query.SpecialAppGroupQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;

public class GraphCommonSetup {
    public final static String graphConfig = "/graphconfig.properties";

    /**
     * Fail message if an expection is expected but never thrown.
     */
    protected static final String EXCEPTION_FAIL_MSG = "Exception should have been thrown.";
    // Commonly used group friendly and fully qualified names:
    protected static final String USER1_ID = "User1";
    protected static final String USER2_ID = "User2";
    protected static final String GROUP12_FQNAME = "root.group1.group12";
    protected static final String GROUP1_FQNAME = "root.group1";
    protected static final String GROUP13_FRIENDLY_NAME = "group13";
    protected static final String GROUP123_FRIENDLY_NAME = "group123";
    protected static final String GROUP12_FRIENDLY_NAME = "group12";
    protected static final String GROUP1_FRIENDLY_NAME = "group1";
    protected static final String GROUP13_FQNAME = "root.group1.group13";
    protected static final String GROUP123_FQNAME = "root.group1.group12.group123";
    // Friendly group name to use to rename a group with:
    protected static final String FRIENDLY_GROUPNAME_TO_RENAME_WITH = "group2";
    // Group name change operations tested, results verified using these 'renamed' fully qualified group names:
    protected static final String RENAMED_FQ_GROUP1NAME =
            GROUP1_FQNAME.replace(GROUP1_FRIENDLY_NAME, FRIENDLY_GROUPNAME_TO_RENAME_WITH);
    protected static final String RENAMED_FQ_GROUP12NAME = GROUP12_FQNAME.replace(GROUP1_FQNAME, RENAMED_FQ_GROUP1NAME);
    protected static final String RENAMED_FQ_GROUP123NAME = GROUP123_FQNAME
            .replace(GROUP1_FQNAME, RENAMED_FQ_GROUP1NAME);
    protected static final String RENAMED_FQ_GROUP13NAME = GROUP13_FQNAME.replace(GROUP1_FQNAME, RENAMED_FQ_GROUP1NAME);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public Properties graphConfiguration;
    public TitanGraph titanGraph;
    public FramedGraph<TitanGraph> framedTitanGraph;

    @Before
    public void setUpGraph() throws StorageException, EzConfigurationLoaderException {
        graphConfiguration = new EzConfiguration(new ClasspathConfigurationLoader(graphConfig)).getProperties();
        graphConfiguration.setProperty("storage.directory", folder.getRoot().toString());

        GraphDatabaseConfiguration conf = new GraphDatabaseConfiguration(new TitanGraphConfiguration(graphConfiguration));
        conf.getBackend().clearStorage();

        TitanGraphProvider graphProvider = new TitanGraphProvider(graphConfiguration);

        titanGraph = graphProvider.get();
        framedTitanGraph = new FramedGraphFactory(new JavaHandlerModule()).create(titanGraph);
    }

    @After
    public void tearDownGraph() throws StorageException, IOException {
        if (titanGraph != null) {
            titanGraph.shutdown();
        }
    }
}
