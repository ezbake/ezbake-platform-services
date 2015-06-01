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

package ezbake.groups.graph.frames.edge;

import com.tinkerpop.frames.EdgeFrame;
import com.tinkerpop.frames.Property;

/**
 * User: jhastings
 * Date: 6/25/14
 * Time: 4:06 PM
 */
public interface BaseEdge extends EdgeFrame {
    public enum EdgeType {
        DATA_ACCESS,
        COMPLEX_DATA_ACCESS,
        A_READ,
        A_WRITE,
        A_MANAGE,
        A_CREATE_CHILD
    }

    @Property("type")
    public void setType(EdgeType type);

    @Property("type")
    public EdgeType getType();
}
