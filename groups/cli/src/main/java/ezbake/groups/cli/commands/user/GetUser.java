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

package ezbake.groups.cli.commands.user;

import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.exception.InvalidVertexTypeException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.frames.vertex.User;

/**
 * User: jhastings
 * Date: 9/25/14
 * Time: 10:52 AM
 */
public class GetUser extends UserCommand {

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();

        try {
            User graphUser = graph.getUser(userType(), user);
            System.out.println("User name: " + graphUser.getName());
            System.out.println("     principal: " + graphUser.getPrincipal());
            System.out.println("     index: " + graphUser.getIndex());
        } catch (InvalidVertexTypeException | UserNotFoundException e) {
            e.printStackTrace();
        }
    }
}

