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

import ezbake.groups.cli.commands.user.UserCommand;
import org.kohsuke.args4j.Option;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/25/14
 * Time: 11:29 AM
 */
public abstract class GroupCommand extends UserCommand {

    @Option(name="-g", aliases="--group-name", metaVar="GROUP_NAME", required=true, usage="Name of group to act on")
    public String groupName;

    @Option(name="-p", aliases="--parent-group-name", metaVar="PARENT_GROUP_NAME", depends={"-g"},
            usage="Parent group under which to look for group")
    public String parentGroup;

    public GroupCommand() { }

    public GroupCommand(Properties properties) {
        super(properties);
    }
}
