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

package ezbake.groups.cli.commands.group;

import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.cli.commands.group.GroupCommand;
import ezbake.groups.graph.EzGroupsGraph;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.thrift.EzGroupsConstants;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/5/14
 * Time: 4:20 PM
 */
public class GetGroupsCommand extends GroupCommand {

    public GetGroupsCommand() { }

    public GetGroupsCommand(String groupName, Properties properties) {
        super(properties);
        this.groupName = groupName;
    }

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraph graph = getGraph();

        if (!groupName.endsWith(EzGroupsConstants.ROOT)) {
            groupName = nameHelper.addRootGroupPrefix(groupName);
        }

        try {
            Group group = graph.getGroup(groupName);
            System.out.println("Group: "+group.getGroupName());
            System.out.println("Index: "+group.getIndex());
            System.out.println("Vertex Id: "+group.asVertex().getId());
            System.out.println("Active: " + group.isActive());
            System.out.println("Requires only user: " + group.isRequireOnlyUser());
            System.out.println("Requires only app: " + group.isRequireOnlyApp());
            for (Group child : group.getChildGroups()) {
                System.out.println("Child group: " + child.getGroupName());
            }
        } catch (VertexNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                graph.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
