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
import ezbake.groups.graph.exception.*;
import org.kohsuke.args4j.Option;

import java.io.IOException;

/**
 * User: jhastings
 * Date: 11/1/14
 * Time: 7:17 PM
 */
public class GroupInheritanceCommand extends GroupCommand {

    @Option(name="-da", aliases="--data-access")
    boolean dataAccess = false;

    @Option(name="-ar", aliases="--admin-read")
    boolean adminRead = false;

    @Option(name="-aw", aliases="--admin-write")
    boolean adminWrite = false;

    @Option(name="-am", aliases="--admin-manage")
    boolean adminManage = false;

    @Option(name="-ac", aliases="--admin-create-child")
    boolean adminCreateChild = false;

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();

        try {
            String internalName = nameHelper.addRootGroupPrefix(groupName);
            graph.setGroupInheritance(internalName, dataAccess, adminRead, adminWrite, adminManage, adminCreateChild);
            System.out.println("Updating Group inheritance for: " + groupName);
            System.out.println("Data Access: " + dataAccess);
            System.out.println("Admin Read: " + adminRead);
            System.out.println("Admin Write: " + adminWrite);
            System.out.println("Admin Manage: " + adminManage);
            System.out.println("Admin Create Child: " + adminCreateChild);
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
