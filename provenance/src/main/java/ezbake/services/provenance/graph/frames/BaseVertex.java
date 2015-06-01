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

package ezbake.services.provenance.graph.frames;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerClass;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerContext;
import ezbake.base.thrift.TokenType;
import ezbake.services.provenance.graph.Utils;

import java.util.Date;

@JavaHandlerClass(BaseVertex.BaseVertexImpl.class)
public interface BaseVertex extends VertexFrame {

    public static final String Application = "Application";
    public static final String User = "User";
    public static final String TimeStamp = "TimeStamp";
    public static final String Type = "Type";

    @Property(Application)
    public void setApplication(String application);

    @Property(Application)
    public String getApplication();

    @Property(User)
    public void setUser(String user);

    @Property(User)
    public String getUser();

    @Property(TimeStamp)
    public void setTimeStamp(Date timeStamp);

    @Property(TimeStamp)
    public Date getTimeStamp();

    @Property(Type)
    public void setType(String user);

    @Property(Type)
    public String getType();

    @JavaHandler
    public void updateCommonProperties(ezbake.base.thrift.EzSecurityToken securityToken, String type);

    public abstract class BaseVertexImpl implements JavaHandlerContext<Vertex>, BaseVertex {

        public void updateCommonProperties(ezbake.base.thrift.EzSecurityToken securityToken, String type) {
            this.setApplication(Utils.getApplication(securityToken));
            this.setTimeStamp(Utils.getCurrentDate());
            this.setType(type);
            this.setUser(Utils.getUser(securityToken));
        }
    }
}
