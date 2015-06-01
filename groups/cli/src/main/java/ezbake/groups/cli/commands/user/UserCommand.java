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

import ezbake.groups.cli.commands.EzGroupsCommand;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import org.kohsuke.args4j.Option;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/25/14
 * Time: 11:25 AM
 */
public abstract class UserCommand extends EzGroupsCommand {

    public enum UserType { USER, APP }

    @Option(name="-t", aliases="--user-type", metaVar="USER_TYPE", usage="User userType")
    public UserType userType = UserType.USER;

    @Option(name="-u", aliases="--user", metaVar="USER", required=true)
    public String user;

    public BaseVertex.VertexType userType() {
        return (userType == UserType.USER)? BaseVertex.VertexType.USER : BaseVertex.VertexType.APP_USER;
    }

    public UserCommand() { }

    public UserCommand(Properties properties) {
        super(properties);
    }
}
