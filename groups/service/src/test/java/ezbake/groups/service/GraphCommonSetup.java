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

package ezbake.groups.service;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.TitanGraphConfiguration;
import ezbake.groups.graph.impl.TitanGraphProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;

public class GraphCommonSetup {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public Properties graphConfiguration;
    public TitanGraph titanGraph;
    public FramedGraph<TitanGraph> framedTitanGraph;

    @Before
    public void setUpGraph() throws StorageException, EzConfigurationLoaderException {
        graphConfiguration = new Properties();
        graphConfiguration.setProperty("storage.directory", folder.getRoot().toString());
        graphConfiguration.setProperty("storage.provider", "berkeleyje");
        graphConfiguration.setProperty("storage.machine-id-appendix", "1");
        graphConfiguration.setProperty("storage.transactions", "false");

        GraphDatabaseConfiguration conf = new GraphDatabaseConfiguration(new TitanGraphConfiguration(graphConfiguration));
        conf.getBackend().clearStorage();

        TitanGraphProvider graphProvider = new TitanGraphProvider(graphConfiguration);
        titanGraph = graphProvider.get();
        framedTitanGraph = new FramedGraphFactory().create(titanGraph);
        updateGraphIdAppendix();
    }

    @After
    public void tearDownGraph() throws StorageException, IOException {
        if (framedTitanGraph != null) {
            framedTitanGraph.shutdown();
        }
        if (titanGraph != null) {
            titanGraph.shutdown();
        }
    }

    public void updateGraphIdAppendix() {
        int currentAppendix = Integer.parseInt(graphConfiguration.getProperty("storage.machine-id-appendix"), 10);
        graphConfiguration.setProperty("storage.machine-id-appendix", Integer.toString(++currentAppendix, 10));
    }
}
