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

import com.google.common.base.Strings;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.cli.commands.group.GroupCommand;
import ezbake.groups.graph.EzGroupsGraph;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.thrift.EzGroupsConstants;
import org.kohsuke.args4j.Option;

import java.io.IOException;

/**
 * User: jhastings
 * Date: 10/10/14
 * Time: 12:22 PM
 */
public class UpdateGroupInclusionCommand extends GroupCommand {

    @Option(name="--require-only-user", usage="value to set the group require-only-user flag to")
    String requireOnlyUser;

    @Option(name="--require-only-app", usage="value to set the group require-only-app flag to")
    String requireOnlyApp;

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraph graph = getGraph();

        if (!groupName.endsWith(EzGroupsConstants.ROOT)) {
            groupName = nameHelper.addRootGroupPrefix(groupName);
        }

        try {
            Group group = graph.getGroup(groupName);

            if (!Strings.isNullOrEmpty(requireOnlyUser)) {
                boolean value = Boolean.parseBoolean(requireOnlyUser);
                group.setRequireOnlyUser(value);
                System.out.println("Updated group with require only user: " + value);
            }

            if (!Strings.isNullOrEmpty(requireOnlyApp)) {
                boolean value = Boolean.parseBoolean(requireOnlyApp);
                group.setRequireOnlyApp(value);
                System.out.println("Updated group with require app user: " + value);
            }

        } catch (VertexNotFoundException e) {
            System.err.println("Group not found");
        } finally {
            try {
                graph.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
