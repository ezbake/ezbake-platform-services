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

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.api.GroupIDPublisher;
import ezbake.groups.graph.frames.vertex.BaseVertex;

import java.util.Iterator;

/**
 * User: jhastings
 * Date: 7/30/14
 * Time: 3:45 PM
 */
public class TitanGraphIDPublisher implements GroupIDPublisher {

    private TitanGraph graph;

    @Inject
    public TitanGraphIDPublisher(TitanGraph graph) {
        this.graph = graph;
    }

    @Override
    public long getCurrentId() {
        long id = 0;
        Iterator<Vertex> lastVertex = graph.query()
                .has(BaseVertex.INDEX)
                .orderBy(BaseVertex.INDEX, Order.DESC)
                .limit(1)
                .vertices().iterator();
        if (lastVertex.hasNext()) {
            id = lastVertex.next().getProperty(BaseVertex.INDEX);
        }
        return id;
    }

}
