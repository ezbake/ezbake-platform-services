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
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.exception.VertexExistsException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import org.kohsuke.args4j.Option;

import java.io.IOException;

/**
 * User: jhastings
 * Date: 11/3/14
 * Time: 12:58 PM
 */
public class ChangeGroupName extends GroupCommand {

    @Option(name="-n", aliases="--new-group-name", required=true)
    String newName;

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();

        groupName = nameHelper.addRootGroupPrefix(groupName);
        try {
            graph.changeGroupName(groupName, newName);
        } catch (VertexNotFoundException e) {
            e.printStackTrace();
        } catch (VertexExistsException e) {
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
