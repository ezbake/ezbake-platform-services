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

package ezbake.groups.graph.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.graph.exception.InvalidVertexTypeException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.GroupQueryError;
import ezbake.groups.thrift.GroupQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 */
public class AuthorizationQuery {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizationQuery.class);

    private GroupsGraph graph;

    public AuthorizationQuery(GroupsGraph graph) {
        this.graph = graph;
    }

    protected Set<Group> getUserGroups(BaseVertex.VertexType type, String id, boolean includeInactive) throws GroupQueryException {
        Set<Group> gs;
        try {
            User u = graph.getUser(type, id);
            // Only return groups for active users
            if (!u.isActive()) {
                return Collections.emptySet();
            }
            // Get the groups from the graph and add them to the set
            gs = graph.userGroups(type, id, false, includeInactive);
        } catch (UserNotFoundException|InvalidVertexTypeException e) {
            logger.error("Cannot get user groups for {}. User Not found", id);
            throw new GroupQueryException("User "+id+" not found. Cannot compute group user groups",
                    GroupQueryError.USER_NOT_FOUND);
        }
        return gs;
    }

    protected Collection<Set<Group>> getUserGroups(Map<String, BaseVertex.VertexType> users, boolean includeInactive) {
        List<Set<Group>> groupList = Lists.newArrayList();
        for (Map.Entry<String, BaseVertex.VertexType> user : users.entrySet()) {
            try {
                groupList.add(getUserGroups(user.getValue(), user.getKey(), includeInactive));
            } catch (GroupQueryException e) {
                logger.debug("No groups returned for user: {} ({})", user.getValue(), user.getKey());
            }
        }
        return groupList;
    }

    private Map<String, BaseVertex.VertexType> userListToMap(BaseVertex.VertexType allUsersType, Collection<String> ids) {
        Map<String, BaseVertex.VertexType> map = Maps.newHashMap();
        for (String id: ids) {
            map.put(id, allUsersType);
        }
        return map;
    }

    /**
     * Will get a user's authorizations, filtering by the groups that apps in the filter chain have access to
     *
     * @param type type of user to look for
     * @param id user id
     * @param appFilterChain
     * @return the user's set of group authorizations
     */
    public Set<Long> getAuthorizationSet(BaseVertex.VertexType type, String id, List<String> appFilterChain) throws GroupQueryException {
        Set<Long> auths = Sets.newHashSet();

        // Only get auths if the user exists
        User user;
        try {
            user = graph.getUser(type, id);
            if (!user.isActive()) {
                logger.debug("User was inactive, returning empty set");
                return auths; // just don't get groups
            }
        } catch (InvalidVertexTypeException e) {
            logger.debug("Invalid request, returning empty set");
            return auths; // just don't get groups
        } catch (UserNotFoundException e) {
            throw new GroupQueryException("No user found: "+type+":"+id, GroupQueryError.USER_NOT_FOUND);
        }

        // Add the user's own index
        logger.debug("Adding user's id to the auths: {}", user.getIndex());
        auths.add(user.getIndex());

        // This can sometimes be null
        if (appFilterChain == null) {
            appFilterChain = Collections.emptyList();
        }

        // These are the groups the user has on their own
        Set<Group> userGroups = getUserGroups(type, id, false);
        for (Group g : userGroups) {
            logger.debug("Group -> {} {}", g.getGroupName(), g.getIndex());
        }
        logger.debug("getAuthorizations User: {} has groups: {}", user, userGroups);

        // These are the groups the apps always include, even if the user doesn't have access
        Collection<Set<Group>> appsGroups = getUserGroups(userListToMap(BaseVertex.VertexType.APP_USER, appFilterChain),
                false);
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


        if (type == BaseVertex.VertexType.USER) {
            // Split groups into 2 sets - those that users always have (even if app doesn't) and those that users only have if app has too
            Set<Long> groupsUserHasRegardless = Sets.newHashSet(auths);
            Set<Long> groupsDependingOnApp = Sets.newHashSet();
            for (Group g : userGroups) {
                if (g.isRequireOnlyUser()) {
                    logger.debug("User should have group: {} regardless", g);
                    groupsUserHasRegardless.add(g.getIndex());
                } else {
                    logger.debug("Will check app access to group: {}", g);
                    groupsDependingOnApp.add(g.getIndex());
                }
            }

            // Filter the groups that depend on the app
            if (!groupsDependingOnApp.isEmpty()) {
                logger.debug("Filtering groups depending on app: {} -> {}", groupsDependingOnApp, appsFilter);
                groupsDependingOnApp = Sets.intersection(groupsDependingOnApp, appsFilter);
                logger.debug("Filter result: {}", groupsDependingOnApp);
            }

            // Now union the sets to get the users final list
            auths = Sets.union(groupsUserHasRegardless, groupsDependingOnApp);
            logger.debug("Auths after taking intersection: {}", auths);
        } else if (type == BaseVertex.VertexType.APP_USER) {
            // What to do here?
            Set<Long> appAuths = Sets.newHashSet(auths);
            for (Group g : userGroups) {
                appAuths.add(g.getIndex());
            }
            auths = appAuths;
        }

        graph.commitTransaction();
        return Sets.union(auths, groupsAppsAlwaysInclude);
    }

    public Set<Long> execute(BaseVertex.VertexType type, String id, List<String> appFilter) throws GroupQueryException {
        return getAuthorizationSet(type, id, appFilter);
    }
}
