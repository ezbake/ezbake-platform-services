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

package ezbake.groups.graph.frames.vertex;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.Initializer;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerContext;

/**
 * User: jhastings
 * Date: 6/16/14
 * Time: 12:52 PM
 */
public interface BaseVertex extends VertexFrame {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String INDEX = "index";
    public static final String TERMINATOR = "terminator";
    public static final String ACTIVE = "active";


    public enum VertexType {
        GROUP,
        APP_GROUP,
        GROUP_MAPPING,
        USER,
        APP_USER
    }

    @Property(NAME)
    public void setName(String name);

    @Property(NAME)
    public String getName();
    
    @Property(TYPE)
    public void setType(VertexType type);

    @Property(TYPE)
    public VertexType getType();

    @Property(INDEX)
    public void setIndex(Long index);

    @Property(INDEX)
    public Long getIndex();

    @Property(TERMINATOR)
    public void setTerminator(boolean terminator);

    @Property(TERMINATOR)
    public boolean getTerminator();

    @Property(ACTIVE)
    public void setIsActive(boolean active);
    @Property(ACTIVE)
    public boolean isActive();

    public abstract class Impl implements JavaHandlerContext<Vertex>, BaseVertex {
        @Initializer
        public void init() {
            setIsActive(true);
        }
    }

}
