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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.frames.EdgeFrame;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.javahandler.Initializer;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerClass;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerContext;
import ezbake.base.thrift.TokenType;
import ezbake.services.provenance.graph.Utils;

import java.util.Date;

@JavaHandlerClass(BaseEdge.BaseEdgeImpl.class)
public interface BaseEdge extends EdgeFrame {

    public static final String Application = "Application";
    public static final String User = "User";
    public static final String TimeStamp = "TimeStamp";


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

    @JavaHandler
    public void updateCommonProperties(ezbake.base.thrift.EzSecurityToken securityToken);

    public abstract class BaseEdgeImpl implements JavaHandlerContext<Edge>, BaseEdge {

        public void updateCommonProperties(ezbake.base.thrift.EzSecurityToken securityToken) {
            this.setApplication(Utils.getApplication(securityToken));
            this.setTimeStamp(Utils.getCurrentDate());
            this.setUser(Utils.getUser(securityToken));
        }
    }
}
