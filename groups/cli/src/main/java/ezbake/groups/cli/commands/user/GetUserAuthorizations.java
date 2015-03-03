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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.groups.graph.EzGroupsGraph;
import ezbake.groups.graph.exception.InvalidVertexTypeException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.GroupQueryException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 10/10/14
 * Time: 10:04 AM
 */
public class GetUserAuthorizations extends UserCommand {
    private static Logger logger = LoggerFactory.getLogger(GetUserAuthorizations.class);

    @Option(name="-e", aliases="--explicit-groups", usage="Only display groups that the user has direct access to")
    boolean explicit = false;

    @Option(name="-a", aliases="--apps", usage="Perform request as if these apps were part of the chain")
    String apps = "";

    @Option(name="-i", aliases="--include-inactive", usage="Include inactive groups in the results")
    boolean inactive = false;

    @Override
    public void runCommand() throws EzConfigurationLoaderException {
        EzGroupsGraph graph = getGraph();

        List<String> appChain = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(apps);





        try {
            Set<Group> userGroups = getUserGroups(graph, userType(), user, explicit, inactive);
            System.out.println("Groups for user: " + user);
            for (Group group : userGroups) {
                System.out.println("Group name: " + group.getGroupName() + ", index: " + group.getIndex());
            }

            Set<Long> authorizations = getAuthorizations(graph, userType(), user, appChain);
            System.out.println("User authorization set: " + authorizations.toString());
        } catch (UserNotFoundException e) {
            System.err.println("User does not exist");
            e.printStackTrace();
        } catch (GroupQueryException e) {
            System.err.println("Failure getting authorizations");
            e.printStackTrace();
        }
    }

    private static Set<Group> getUserGroups(EzGroupsGraph graph, BaseVertex.VertexType userType, String id, boolean explicit, boolean includeInactive) throws UserNotFoundException {
        return graph.userGroups(userType, id, explicit, includeInactive);
    }

    private static Set<Long> getAuthorizations(EzGroupsGraph graph, BaseVertex.VertexType userType, String userId, List<String> appFilterChain) throws GroupQueryException, UserNotFoundException {
        Set<Long> auths = Sets.newHashSet();

        // Only get auths if the user exists
        User user;
        try {
            user = graph.getUser(userType, userId);
            if (!user.isActive()) {
                return auths; // just don't get groups
            }
        } catch (UserNotFoundException|InvalidVertexTypeException e) {
            return auths; // just don't get groups
        }

        // Add the user's own index
        auths.add(user.getIndex());

        // This can sometimes be null
        if (appFilterChain == null) {
            appFilterChain = Collections.emptyList();
        }

        // These are the groups the user has on their own
        Set<Group> userGroups = getUserGroups(graph, userType, userId, false, false);
        logger.debug("Initial user groups: {}", userGroups);

        // These are the groups the apps always include, even if the user doesn't have access
        List<Set<Group>> appsGroups = getAuthorizationsForApps(graph, appFilterChain, false);
        Set<Long> appsFilter = Sets.newHashSet(); // This is the intersection of all app auths
        Set<Long> groupsAppsAlwaysInclude = Sets.newTreeSet(); // This is all the groups the apps include anyways
        for (Set<Group> appGroup : appsGroups) {
            Set<Long> indices = Sets.newTreeSet();
            for (Group group : appGroup) {
                indices.add(group.getIndex());
                if (group.isRequireOnlyApp()) {
                    groupsAppsAlwaysInclude.add(group.getIndex());
                }
            }
            appsFilter.retainAll(indices);
        }
        logger.debug("Apps filter: {}", appsFilter);


        if (userType == BaseVertex.VertexType.USER) {
            // Split groups into 2 sets - those that users always have (even if app doesn't) and those that users only have if app has too
            Set<Long> groupsUserHasRegardless = Sets.newHashSet(auths);
            Set<Long> groupsDependingOnApp = Sets.newHashSet();
            for (Group g : userGroups) {
                if (g.isRequireOnlyUser()) {
                    groupsUserHasRegardless.add(g.getIndex());
                } else {
                    groupsDependingOnApp.add(g.getIndex());
                }
            }

            // Filter the groups that depend on the app
            if (!groupsDependingOnApp.isEmpty()) {
                logger.debug("Groups depending on app: {}", groupsDependingOnApp);
                groupsDependingOnApp = Sets.intersection(groupsDependingOnApp, appsFilter);
            }

            logger.debug("Groups user has regardless: {}", groupsUserHasRegardless);
            // Now union the sets to get the users final list
            auths = Sets.union(groupsUserHasRegardless, groupsDependingOnApp);
        } else if (userType == BaseVertex.VertexType.APP_USER) {
            // What to do here?
            Set<Long> appAuths = Sets.newHashSet(auths);
            for (Group g : userGroups) {
                appAuths.add(g.getIndex());
            }
            auths = appAuths;
        }

        logger.debug("User auths before union: {}", auths);
        return Sets.union(auths, groupsAppsAlwaysInclude);
    }

    private static List<Set<Group>> getAuthorizationsForApps(EzGroupsGraph graph, List<String> apps, boolean includeInactive) {
        List<Set<Group>> appAuthorizations = Lists.newArrayList();

        for (String securityId : apps) {
            try {
                appAuthorizations.add(getUserGroups(graph, BaseVertex.VertexType.APP_USER, securityId, false, includeInactive));
            } catch (UserNotFoundException e) {
                System.out.println("No groups returned for app: " + securityId);
            }
        }
        return appAuthorizations;
    }

}
