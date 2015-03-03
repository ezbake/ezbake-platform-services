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
import ezbake.groups.graph.EzGroupsGraph;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.UserGroupPermissions;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 10:51 AM
 */
public class CreateGroupCommand extends GroupCommand {
    private static final Logger logger = LoggerFactory.getLogger(CreateGroupCommand.class);

    @Option(name="-f", aliases="--force", usage="Create group even if user doesn't not have permission")
    private boolean force = false;


    /**
     * Default constructor. This is required for args4j
     */
    public CreateGroupCommand() { }

    public CreateGroupCommand(String groupName, String parentGroup, String groupOwner, Properties properties) {
        super(properties);
        this.groupName = groupName;
        this.parentGroup = parentGroup;
        this.user = groupOwner;
        this.userType = UserType.USER;
    }

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraph graph = getGraph();

        try {
            boolean groupCreated = false;
            int attempts = 0;
            while (!groupCreated && attempts < MAX_TRIES) {
                try {
                    Group g;
                    UserGroupPermissions ownerPermissions = UserGroupPermissionsWrapper.ownerPermissions();
                    if (parentGroup != null) {
                        // Normalize the group name
                        parentGroup = nameHelper.addRootGroupPrefix(parentGroup);

                        System.out.println("Creating group: groupName=" + groupName + ", parentGroup=" + parentGroup + ", owner=" +
                                user);
                        g = graph.addGroup(userType(), user, groupName, parentGroup, new GroupInheritancePermissions(), ownerPermissions, true, false);
                    } else {
                        System.out.println("Creating group: groupName=" + groupName + ", owner=" + user);
                        g = graph.addGroup(userType(), user, groupName, new GroupInheritancePermissions(), ownerPermissions, true, false);
                    }
                    groupCreated = true;
                } catch (UserNotFoundException e) {
                    System.out.println("Cannot create group. Group owner does not exist");
                    // Create the user
                    try {
                        System.out.println("Creating groupOwner: " + user);
                        graph.addUser(userType(), user, null);
                    } catch (InvalidVertexTypeException | VertexExistsException | AccessDeniedException | UserNotFoundException
                            | IndexUnavailableException | InvalidGroupNameException e1) {
                        System.err.println("Failed creating group owner: " + e.getMessage());
                        break;
                    }
                } catch (AccessDeniedException e) {
                    if (force) {
                        System.out.println("User does not admin create child permission. Forcing group creation");

                    }
                } catch (VertexExistsException | VertexNotFoundException | InvalidGroupNameException | IndexUnavailableException e) {
                    System.err.println("Failed creating group: groupName=" + groupName + ". " + e.getMessage());
                    break;
                }

                if (!groupCreated) {
                    attempts -= 1;
                }
            }

            if (!groupCreated) {
                System.err.println("Group was not created");
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
