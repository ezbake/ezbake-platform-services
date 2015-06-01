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
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.*;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 10:53 AM
 */
public class AddUserToGroupCommand extends GroupCommand {
    private static final Logger logger = LoggerFactory.getLogger(AddUserToGroupCommand.class);

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

        groupName = nameHelper.addRootGroupPrefix(groupName);
        try {
            boolean userAdded = false;
            int tries = 0;
            while (!userAdded && tries < MAX_TRIES) {
                try {
                    System.out.println("Adding user to group: user=" + user + ", groupName=" +
                            nameHelper.removeRootGroupPrefix(groupName) + ", permissions=" +
                            new UserGroupPermissionsWrapper(true, adminRead, adminWrite, adminManage, adminCreateChild));
                    graph.addUserToGroup(userType(), user, groupName, true, adminRead, adminWrite, adminManage,
                            adminCreateChild);
                    userAdded = true;
                } catch (UserNotFoundException e) {
                    System.out.println("Cannot add user to group. User does not exist");
                    // Create the user
                    try {
                        System.out.println("Creating user: " + user);
                        graph.addUser(userType(), user, null);
                    } catch (InvalidVertexTypeException | VertexExistsException | AccessDeniedException | UserNotFoundException
                            | IndexUnavailableException | InvalidGroupNameException e1) {
                        System.err.println("Unable to create user: " + e1.getMessage());
                        break;
                    }
                } catch (InvalidVertexTypeException | VertexNotFoundException e) {
                    System.err.println("Unable to add user to group: " + e.getMessage());
                    break;
                }
                tries += 1;
            }
        } finally {
            try {
                graph.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
