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
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.query.BaseQuery;
import ezbake.groups.graph.query.SpecialAppGroupQuery;
import ezbake.groups.thrift.EzGroupsConstants;
import org.kohsuke.args4j.Option;

import java.util.Set;

/**
 * User: jhastings
 * Date: 11/1/14
 * Time: 7:03 PM
 */
public class GetUserApps extends UserCommand {

    enum AppMembershipType {
        Auditor, Metrics, Diagnostic
    }

    @Option(name="-m", aliases="--membership-type")
    AppMembershipType appMembershipType = AppMembershipType.Diagnostic;



    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();
        SpecialAppGroupQuery query = new SpecialAppGroupQuery(new BaseQuery(graph.getFramedGraph()));

        try {
            Set<String> groups = null;
            switch (appMembershipType) {
                case Auditor:
                    groups = query.specialAppNamesQuery(EzGroupsConstants.AUDIT_GROUP, graph.getUser(userType(), user).asVertex());
                    break;
                case Metrics:
                    groups = query.specialAppNamesQuery(EzGroupsConstants.METRICS_GROUP, graph.getUser(userType(), user).asVertex());
                    break;
                case Diagnostic:
                    groups = query.specialAppNamesQuery(EzGroupsConstants.DIAGNOSTICS_GROUP, graph.getUser(userType(), user).asVertex());
                    break;
            }
            System.out.println("Apps for user: " + user);
            if (groups != null) {
                for (String group : groups) {
                    System.out.println("App: " + group);
                }
            } else {
                System.out.println("None");
            }

        } catch (InvalidVertexTypeException | UserNotFoundException | VertexNotFoundException e) {
            e.printStackTrace();
        }
    }
}
