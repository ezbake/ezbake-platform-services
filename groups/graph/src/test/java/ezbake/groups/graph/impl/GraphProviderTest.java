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

package ezbake.groups.graph.impl;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.GraphCommonSetup;
import ezbake.groups.graph.api.GraphProvider;
import ezbake.groups.graph.frames.vertex.Group;
import org.junit.Test;


public class GraphProviderTest extends GraphCommonSetup {

    public GraphProvider<TitanGraph> gp;

    @Test
    public void testGetGraph() {
        TitanGraph g = new TitanGraphProvider(graphConfiguration).getGraph(graphConfiguration);
    }

    @Test
    public void testGraphInit() {
        TitanGraph g = new TitanGraphProvider(graphConfiguration).getGraph(graphConfiguration);

        // Make sure graph uniqueness holds true
        Vertex g1 = g.addVertex(null);
        g1.setProperty(Group.COMMON_GROUP, "unique");

        Vertex g2 = g.addVertex(null);
        g2.setProperty(Group.COMMON_GROUP, "unique");

        g.commit();
    }
}
