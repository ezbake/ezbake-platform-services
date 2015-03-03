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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Adjacency;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.javahandler.Initializer;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerContext;
import ezbake.groups.thrift.EzGroupsConstants;

/**
 * User: jhastings
 * Date: 6/16/14
 * Time: 12:50 PM
 */
public interface Group extends BaseVertex {
    /* Special group names */
    public static final String COMMON_GROUP = EzGroupsConstants.ROOT;
    public static final String APP_GROUP = EzGroupsConstants.APP_GROUP;
    public static final String APP_ACCESS_GROUP = EzGroupsConstants.APP_ACCESS_GROUP;

    /* Property constants */
    public static final String GROUP_NAME = "groupName";
    public static final String FRIENDLY_GROUP_NAME = "groupFriendlyName";
    public static final String CHILD_GROUP = "ChildGroup";
    public static final String REQUIRE_ONLY_USER = "requireOnlyUser";
    public static final String REQUIRE_ONLY_APP = "requireOnlyAPP";

    @Property(GROUP_NAME)
    public void setGroupName(String groupName);

    @Property(GROUP_NAME)
    public String getGroupName();

    @Property(FRIENDLY_GROUP_NAME)
    public void setGroupFriendlyName(String groupName);

    @Property(FRIENDLY_GROUP_NAME)
    public String getGroupFriendlyName();

    @Property(REQUIRE_ONLY_APP)
    public boolean isRequireOnlyApp();

    @Property(REQUIRE_ONLY_APP)
    public void setRequireOnlyApp(boolean isRequireOnlyApp);

    @Property(REQUIRE_ONLY_USER)
    public boolean isRequireOnlyUser();

    @Property(REQUIRE_ONLY_USER)
    public void setRequireOnlyUser(boolean isRequireOnlyUser);

    @Adjacency(label=CHILD_GROUP, direction=Direction.OUT)
    public Iterable<Group> getChildGroups();

    @Adjacency(label=CHILD_GROUP, direction=Direction.OUT)
    public void addChildGroup(Group child);

    @Adjacency(label=CHILD_GROUP, direction=Direction.OUT)
    public void setChildGroups(Iterable<Group> childern);

    public abstract class Impl implements JavaHandlerContext<Vertex>, Group {
        @Initializer
        public void init() {
            setIsActive(true);
            setRequireOnlyUser(true);
            setRequireOnlyApp(false);
        }
    }

}
