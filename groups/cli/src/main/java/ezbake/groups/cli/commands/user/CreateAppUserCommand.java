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
import ezbake.groups.cli.commands.EzGroupsCommand;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.GroupInheritancePermissions;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 4:07 PM
 */
public class CreateAppUserCommand extends EzGroupsCommand {
    private static final Logger logger = LoggerFactory.getLogger(CreateAppUserCommand.class);

    @Option(name="-id", aliases="--app-security-id", required=true)
    private String securityId;

    @Option(name="-n", aliases="--app-name", required=true)
    private String appName;

    public CreateAppUserCommand() { }

    public CreateAppUserCommand(String securityId, String appName, Properties configuration) {
        super(configuration);
        this.securityId = securityId;
        this.appName = appName;
    }

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraphImpl graph = getGraph();
        try {
            System.out.println("Creating App User: securityId="+securityId+", appName="+appName);
            graph.addUser(BaseVertex.VertexType.APP_USER, securityId, appName);

            String appGroup = nameHelper.getNamespacedAppGroup(appName, null);
            try {
                // Also create root.app.name.ezbAudits, root.app.name.ezbMetrics, root.app.name.ezbDiagnostics
                System.out.println("Creating audit group. "+nameHelper.removeRootGroupPrefix(appGroup)+"."+
                        EzGroupsConstants.AUDIT_GROUP);
                graph.addGroup(BaseVertex.VertexType.APP_USER, securityId, EzGroupsConstants.AUDIT_GROUP, appGroup,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        new UserGroupPermissionsWrapper(false, true, true, true, true), true, false);

                System.out.println("Creating metrics group. "+nameHelper.removeRootGroupPrefix(appGroup)+"."+
                        EzGroupsConstants.METRICS_GROUP);
                graph.addGroup(BaseVertex.VertexType.APP_USER, securityId, EzGroupsConstants.METRICS_GROUP, appGroup,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        UserGroupPermissionsWrapper.ownerPermissions(), true, false);

                System.out.println("Creating diagnostics group. "+nameHelper.removeRootGroupPrefix(appGroup)+"."+
                        EzGroupsConstants.DIAGNOSTICS_GROUP);
                graph.addGroup(BaseVertex.VertexType.APP_USER, securityId, EzGroupsConstants.DIAGNOSTICS_GROUP,
                        appGroup, new GroupInheritancePermissions(false, false, false, false, false),
                        UserGroupPermissionsWrapper.ownerPermissions(), true, false);

            } catch (VertexNotFoundException e) {
                System.err.println("Parent group not found: " + e.getMessage());
            }
        } catch (VertexExistsException e) {
            System.err.println("App User already exists: securityId="+securityId+", appName="+appName);
        } catch (InvalidVertexTypeException|AccessDeniedException|UserNotFoundException e) {
            System.err.println("Failed to create App User: "+e.getMessage());
        } catch (IndexUnavailableException e) {
            System.err.println("Unable to create App User. Failed to allocate a new unique index. Is redis running " +
                    "and properly configured? "+e.getMessage());
        } catch (InvalidGroupNameException e) {
            System.err.println("Application name must not be blank. " + e.getMessage());
        } finally {
            try {
                graph.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
