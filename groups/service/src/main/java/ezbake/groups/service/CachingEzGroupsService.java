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

package ezbake.groups.service;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.metrics.MetricRegistryThrift;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.vertex.*;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.query.GroupMembersQuery;
import ezbake.groups.service.caching.CacheLayer;
import ezbake.groups.service.query.AuthorizationQuery;
import ezbake.groups.thrift.*;
import ezbake.groups.thrift.Group;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.authentication.EzX509;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This implementation of the EzGroups Service implements a caching layer in front of the basic implementation
 */
public class CachingEzGroupsService extends BaseGroupsService {
    private static final Logger logger = LoggerFactory.getLogger(CachingEzGroupsService.class);

    private EzGroupsService impl;
    private CacheLayer<Set<Long>> cache;
    private GroupMembersQuery groupMembersQuery;
    private boolean logTimer;


    public CachingEzGroupsService(EzGroupsService impl, CacheLayer<Set<Long>> cache, Properties configuration,
                                  EzbakeSecurityClient securityClient, GroupMembersQuery groupMembersQuery) {
        this(impl, cache, configuration, securityClient, groupMembersQuery, false);
    }

    @Inject
    public CachingEzGroupsService(EzGroupsService impl, CacheLayer<Set<Long>> cache, Properties configuration,
                                  EzbakeSecurityClient securityClient, GroupMembersQuery groupMembersQuery,
                                  @Named(GroupsServiceModule.LOG_TIMER_STATS_NAME) Boolean shouldLogTimers) {
        super(configuration, securityClient);
        this.impl = impl;
        this.cache = cache;
        this.groupMembersQuery = groupMembersQuery;
        this.logTimer = shouldLogTimers;
    }

    @Override
    public long createGroup(EzSecurityToken ezSecurityToken, String parent, String name,
                            GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        return createAndGetGroup(ezSecurityToken,parent,name,inheritance).getId();
    }

    @Override
    public Group createAndGetGroup(EzSecurityToken ezSecurityToken, String parent, String name,
            GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        // Create the group then perform the actual update
        final Group group = impl.createAndGetGroup(ezSecurityToken, parent, name, inheritance);

        // Rebuild cache for members of the group
        markCacheNeedsUpdateForGroupMembers(group.getGroupName());

        return group;
    }

    @Override
    public long createGroupWithInclusion(EzSecurityToken ezSecurityToken, String parent, String name,
                                         GroupInheritancePermissions inheritance, boolean includeOnlyRequiresUser,
                                         boolean includeOnlyRequiresApp)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        return createAndGetGroupWithInclusion(
                ezSecurityToken, parent, name, inheritance, includeOnlyRequiresUser, includeOnlyRequiresApp).getId();
    }

    @Override
    public Group createAndGetGroupWithInclusion(EzSecurityToken ezSecurityToken, String parent, String name,
            GroupInheritancePermissions inheritance, boolean includeOnlyRequiresUser, boolean includeOnlyRequiresApp)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        // Create the group then perform the actual update
        final Group group = impl.createAndGetGroupWithInclusion(
                ezSecurityToken, parent, name, inheritance, includeOnlyRequiresUser, includeOnlyRequiresApp);

        // Rebuild cache for members of the group
        markCacheNeedsUpdateForGroupMembers(group.getGroupName());

        return group;
    }

    @Override
    public void deactivateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.deactivateGroup(ezSecurityToken, groupName, andChildren);

        // Rebuild cache for users
        markCacheNeedsUpdateForGroupMembers(groupName);
    }

    @Override
    public void activateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.activateGroup(ezSecurityToken, groupName, andChildren);

        // Rebuild cache for users
        markCacheNeedsUpdateForGroupMembers(groupName);
    }

    /**
     * Change which permissions are inherited from the parent to the given group
     *
     * This must also update caches for members who were members of the group, or are now members of it
     *
     * @param ezSecurityToken security token to validate query
     * @param groupName name of group who's inheritance to change
     * @param inheritance new inheritance permissions
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws EzGroupOperationException
     */
    @Override
    public void changeGroupInheritance(EzSecurityToken ezSecurityToken, String groupName,
                                       GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        // Get group members before changing inheritance. These are the users who's caches need to be updated
        Set<User> oldMembers;
        try {
            oldMembers = getGroupMembers(groupName);
        } catch (VertexNotFoundException e) {
            logger.error("Failed to get group members, unable to change group inheritance. Group Name: {}", groupName);
            throw new EzGroupOperationException("Unable to get group members", OperationError.UNRECOVERABLE_ERROR);
        }

        // update inheritance
        impl.changeGroupInheritance(ezSecurityToken, groupName, inheritance);

        Set<User> newMembers;
        try {
            newMembers = getGroupMembers(groupName);
        } catch (VertexNotFoundException e) {
            logger.error("Failed to get group members, unable to change group inheritance. Group Name: {}", groupName);
            throw new EzGroupOperationException("Unable to get group members", OperationError.UNRECOVERABLE_ERROR);
        }

        // Rebuild cache for members
        markCacheNeedsUpdateForGroupMembers(Sets.union(oldMembers, newMembers));
    }

    /**
     * Doesn't have any effect on group id numbers, so no cache update
     *
     * @param ezSecurityToken token
     * @param oldName old group name
     * @param newName new group name
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws EzGroupOperationException
     */
    @Override
    public void changeGroupName(EzSecurityToken ezSecurityToken, String oldName, String newName)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {

        impl.changeGroupName(ezSecurityToken, oldName, newName);
    }

    /**
     * This currently has no caching. It appears to perform pretty well
     *
     * @param ezSecurityToken calling token
     * @param groupNames name of groups requested
     * @param userIds name of users requested
     * @param appIds name of app users requested
     *
     * @return the set of group and/or user index numbers
     * @throws EzSecurityTokenException
     * @throws EzGroupOperationException
     */
    @Override
    public Set<Long> getGroupsMask(EzSecurityToken ezSecurityToken, Set<String> groupNames, Set<String> userIds,
                                   Set<String> appIds) throws EzSecurityTokenException, EzGroupOperationException {
        return impl.getGroupsMask(ezSecurityToken, groupNames,userIds,appIds);
    }

    /**
     * Create a new User with the given id and name
     *
     * This method just calls down to the base implementation to create the User. It then runs a query to get the
     * user's authorizations and set them in the cache
     *
     * @param ezSecurityToken user of token performing the action
     * @param id security id of the new user. This will be their user id in EzGroups
     * @param name the user's name for display purposes. can be blank
     * @return the group index number of the new app user
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws EzGroupOperationException
     */
    @Override
    public long createUser(EzSecurityToken ezSecurityToken, String id, String name)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        long index = impl.createUser(ezSecurityToken, id, name);

        try {
            // Run a query to populate the cache for the new user
            cache.updateAll(buildQueryForUser(BaseVertex.VertexType.USER, id, null));
        } catch (Exception e) {
            // TODO?
        }

        return index;
    }

    @Override
    public void modifyUser(EzSecurityToken ezSecurityToken, String id, String newId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.modifyUser(ezSecurityToken, id, newId);
    }

    @Override
    public void deactivateUser(EzSecurityToken ezSecurityToken, String id)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.deactivateUser(ezSecurityToken, id);

        // Invalidate caches, user should have no cached values anymore
        cache.invalidateAll(buildQueryForUser(BaseVertex.VertexType.USER, id, null).getWildCardKey());
    }

    @Override
    public void activateUser(EzSecurityToken ezSecurityToken, String id)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.activateUser(ezSecurityToken, id);

        // Rebuild caches
        try {
            cache.updateAll(buildQueryForUser(BaseVertex.VertexType.USER, id, null));
        } catch (Exception e) {
            // TODO: Do something about it - probably just del the key
        }
    }

    @Override
    public void deleteUser(EzSecurityToken ezSecurityToken, String id)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.deleteUser(ezSecurityToken, id);

        // Remove caches
        cache.invalidateAll(buildQueryForUser(BaseVertex.VertexType.USER, id, null).getWildCardKey());
    }

    /**
     * This method should be constant time (titan indices) so not caching
     *
     * @param ezSecurityToken caller token
     * @param type type of user to get
     * @param id the user's EzBake id
     * @return the user object
     * @throws EzSecurityTokenException
     */
    @Override
    public ezbake.groups.thrift.User getUser(EzSecurityToken ezSecurityToken, UserType type, String id) throws EzSecurityTokenException {
        return impl.getUser(ezSecurityToken, type, id);
    }

    /**
     * Create a new App User with the given securityId and name
     *
     * This method just calls down to the base implementation to create the App User. It then runs a query to get the
     * user's authorizations and set them in the cache
     *
     * @param ezSecurityToken user of token performing the action
     * @param securityId security id of the new app. This will be their user id in EzGroups
     * @param name name of the application. Used for the "app.appname" groups
     * @return the group index number of the new app user
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws EzGroupOperationException
     */
    @Override
    public long createAppUser(EzSecurityToken ezSecurityToken, String securityId, String name)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        long id = impl.createAppUser(ezSecurityToken, securityId, name);

        try {
            // Run a query to populate the cache for the new user
            cache.updateAll(buildQueryForUser(BaseVertex.VertexType.APP_USER, securityId, null));
        } catch (Exception e) {
            // TODO?
        }

        return id;
    }

    @Override
    public Set<Long> createAppUserAndGetAuthorizations(EzSecurityToken ezSecurityToken, List<String> chain,
                                                       String securityId, String name)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        validatePrivilegedPeer(ezSecurityToken, new EzX509());

        // Check caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER,
                securityId, chain);
        try {
            return cache.get(authQuery);
        } catch (Exception e) {
            // not in cache, call down to base impl
            logger.error("Failed to get authorizations for app user", e);
            return impl.createAppUserAndGetAuthorizations(ezSecurityToken, chain, securityId, name);
        }
    }

    @Override
    public void modifyAppUser(EzSecurityToken ezSecurityToken, String securityId, String newSecurityId, String newName)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.modifyAppUser(ezSecurityToken, securityId, newSecurityId, newName);
    }

    @Override
    public void deactivateAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.deactivateAppUser(ezSecurityToken, securityId);

        // Delete caches for user
        cache.invalidateAll(new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER, securityId, null)
                .getWildCardKey());
    }

    @Override
    public void activateAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.activateAppUser(ezSecurityToken, securityId);

        // Rebuild caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER,
                securityId, null);
        try {
            cache.updateAll(authQuery);
        } catch (Exception e) {
            // TODO: Do something about it - or just ignore?
        }
    }

    @Override
    public void deleteAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.deleteAppUser(ezSecurityToken, securityId);

        // Delete caches for user
        cache.invalidateAll(new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER, securityId, null)
                .getWildCardKey());
    }

    @Override
    public void addUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String userId,
                               UserGroupPermissions userPermissions)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.addUserToGroup(ezSecurityToken, groupName, userId, userPermissions);

        // Update user caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.USER, userId,
                null);
        try {
            cache.updateAll(authQuery);
        } catch (Exception e) {
            // If the update failed, then invalidate the cache since it's bad now
            cache.invalidateAll(authQuery.getWildCardKey());
        }
    }

    @Override
    public void removeUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String userId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.removeUserFromGroup(ezSecurityToken, groupName, userId);

        // Update user caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.USER, userId,
                null);
        try {
            cache.updateAll(authQuery);
        } catch (Exception e) {
            // If the update failed, then invalidate the cache since it's bad now
            cache.invalidateAll(authQuery.getWildCardKey());
        }
    }

    @Override
    public void addAppUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId,
                                  UserGroupPermissions userPermissions)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.addAppUserToGroup(ezSecurityToken, groupName, securityId, userPermissions);

        // Update user caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER,
                securityId, null);
        try {
            cache.updateAll(authQuery);
        } catch (Exception e) {
            // If the update failed, then invalidate the cache since it's bad now
            cache.invalidateAll(authQuery.getWildCardKey());
        }
    }

    @Override
    public void removeAppUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        impl.removeAppUserFromGroup(ezSecurityToken, groupName, securityId);

        // Update user caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.APP_USER,
                securityId, null);
        try {
            cache.updateAll(authQuery);
        } catch (Exception e) {
            // If the update failed, then invalidate the cache since it's bad now
            cache.invalidateAll(authQuery.getWildCardKey());
        }
    }

    @Override
    public GroupsRequestResponse getGroups(EzSecurityToken token, GroupsRequest groupsRequest)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
       //TODO: Add caching?

        return impl.getGroups(token, groupsRequest);
    }

    @Override
    public Set<Group> getChildGroups(EzSecurityToken ezSecurityToken, String groupName, boolean recursive)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getChildGroups(ezSecurityToken, groupName, recursive);
    }

    /**
     * This method should be constant time (titan indices) so not caching
     *
     * @param ezSecurityToken querying user token
     * @param groupName name of group to query
     * @return the group object
     * @throws EzSecurityTokenException
     */
    @Override
    public Group getGroup(EzSecurityToken ezSecurityToken, String groupName) throws EzSecurityTokenException {
        return impl.getGroup(ezSecurityToken, groupName);
    }

    @Override
    public Map<Long, String> getGroupNamesByIndices(EzSecurityToken ezSecurityToken, Set<Long> ids)
            throws EzSecurityTokenException, GroupQueryException {
        //TODO: Should this cache?

        return impl.getGroupNamesByIndices(ezSecurityToken, ids);
    }

    /**
     * Get all users of the given group, both USER and APP_USER types.
     *
     * TODO: This should cache group members, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @param groupName name of group to query
     * @param explicitMembersOnly if true, return group members who have explicit access to the group (not inherited)
     * @return all group memebrs
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public AllGroupMembers getGroupMembers(EzSecurityToken ezSecurityToken, String groupName,
                                           boolean explicitMembersOnly)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getGroupMembers(ezSecurityToken, groupName, explicitMembersOnly);
    }

    /**
     * Get all APP_USERs of the given group
     *
     * TODO: This should cache group members, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @param groupName name of group to query
     * @param explicitMembersOnly if true, return group members who have explicit access to the group (not inherited)
     * @return all group memebrs
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<String> getGroupApps(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembersOnly)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getGroupApps(ezSecurityToken, groupName, explicitMembersOnly);
    }

    /**
     * Get all USERs of the given group
     *
     * TODO: This should cache group members, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @param groupName name of group to query
     * @param explicitMembersOnly if true, return group members who have explicit access to the group (not inherited)
     * @return all group memebrs
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<String> getGroupUsers(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembersOnly)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getGroupUsers(ezSecurityToken, groupName, explicitMembersOnly);
    }

    /**
     * Return authorizations for the user/app chain given in the token
     *
     * Will hit cache first, building if necessary. If fails at caching layer, will query base impl directly
     *
     * @param ezSecurityToken token to query auths for
     * @return the token user's auths
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<Long> getAuthorizations(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        validateToken(ezSecurityToken);
        try {
            AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), userTypeFromToken(ezSecurityToken),
                    ezSecurityToken.getTokenPrincipal().getPrincipal(), requestChainFromToken(ezSecurityToken));
            return cache.get(authQuery);
        } catch (Exception e) {
            return impl.getAuthorizations(ezSecurityToken);
        }
    }

    /**
     * Get user's Groups
     *
     * TODO: This should cache, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @param explicitGroupsOnly optionally, only return groups user has explicit membership of
     * @return user's groups
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<UserGroup> getUserGroups(EzSecurityToken ezSecurityToken, boolean explicitGroupsOnly)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getUserGroups(ezSecurityToken, explicitGroupsOnly);
    }

    @Override
    public Set<UserGroup> requestUserGroups(EzSecurityToken ezSecurityToken, UserGroupsRequest userGroupsRequest)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException, TException {

        // TODO: Determine if caching would be beneficial
        return impl.requestUserGroups(ezSecurityToken, userGroupsRequest);
    }

    /**
     * Get user's Diagnostic Apps - app.<app name>.ezbDiagnostics
     *
     * TODO: This should cache, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @return the app names of user diagnostic apps
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<String> getUserDiagnosticApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        Stopwatch timer = getStopwatch();
        try {
            return impl.getUserDiagnosticApps(ezSecurityToken);
        } finally {
            logStopwatch(timer, "getUserDiagnosticApps for user: %s type: %s", ezSecurityToken.getType(),
                    ezSecurityToken.getTokenPrincipal().getPrincipal());
        }
    }

    /**
     * Get user's Metrics Apps - app.<app name>.ezbMetrics
     *
     * TODO: This should cache, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @return the app names of user diagnostic apps
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<String> getUserMetricsApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        Stopwatch timer = getStopwatch();
        try {
            return impl.getUserMetricsApps(ezSecurityToken);
        } finally {
            logStopwatch(timer, "getUserMetricsApps user: %s type: %s", ezSecurityToken.getType(),
                    ezSecurityToken.getTokenPrincipal().getPrincipal());
        }
    }

    /**
     * Get user's Auditor Apps - app.<app name>.ezbAudits
     *
     * TODO: This should cache, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @return the app names of user diagnostic apps
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<String> getUserAuditorApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        Stopwatch timer = getStopwatch();
        try {
            return impl.getUserAuditorApps(ezSecurityToken);
        } finally {
            logStopwatch(timer, "getUserAuditorApps user: %s type: %s", ezSecurityToken.getType(),
                    ezSecurityToken.getTokenPrincipal().getPrincipal());
        }
    }

    /**
     * Get app user's Groups
     *
     * TODO: This should cache, because the query could be slow
     *
     * @param ezSecurityToken token of querying user
     * @param explicitGroupsOnly optionally, only return groups app user was explicitly added to
     * @return user's groups
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public Set<UserGroup> getAppUserGroups(EzSecurityToken ezSecurityToken, boolean explicitGroupsOnly)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.getAppUserGroups(ezSecurityToken, explicitGroupsOnly);
    }

    /**
     * Check if a user is a member of a particular group.
     *
     * TODO: this can be done by querying for the group id, and checking if it exists in the user's cache
     *
     * @param ezSecurityToken security token of user being queried
     * @param groupName group name to check user accesss to
     * @return true if the user has data access
     * @throws EzSecurityTokenException
     * @throws AuthorizationException
     * @throws GroupQueryException
     */
    @Override
    public boolean checkUserAccessToGroup(EzSecurityToken ezSecurityToken, String groupName)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        return impl.checkUserAccessToGroup(ezSecurityToken, groupName);
    }

    @Override
    public Set<Long> createUserAndGetAuthorizations(EzSecurityToken ezSecurityToken, List<String> chain, String id,
                                                    String name)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        validatePrivilegedPeer(ezSecurityToken, new EzX509());

        // Check caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), BaseVertex.VertexType.USER,
                id, chain);
        try {
            return cache.get(authQuery);
        } catch (Exception e) {
            // not in cache, call down to base impl (do create)
            return impl.createUserAndGetAuthorizations(ezSecurityToken, chain, id, name);
        }
    }

    @Override
    public Set<Long> getUserAuthorizations(EzSecurityToken ezSecurityToken, TokenType userType, String userId,
                                           List<String> chain)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        validatePrivilegedPeer(ezSecurityToken, new EzX509());

        // Check caches
        AuthorizationQuery authQuery = new AuthorizationQuery(impl.getGraph(), vertexTypeFromTokenType(userType),
                userId, chain);
        try {
            return cache.get(authQuery);
        } catch (Exception e) {
            // not in cache, call down to base impl (do create)
            return impl.getUserAuthorizations(ezSecurityToken, userType, userId, chain);
        }
    }

    @Override
    public boolean ping() {
        return impl.ping();
    }

    @Override
    public MetricRegistryThrift getMetricRegistryThrift() {
        return impl.getMetricRegistryThrift();
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (impl != null) {
            impl.close();
        }
        if (cache != null) {
            cache.close();
        }
    }

    /**
     * Helper method to build an authorization query for different user types
     *
     * @param userType type from user token
     * @param id user identifier
     * @param chain list of applications to include in the query
     * @return an authorization query for the user
     */
    private AuthorizationQuery buildQueryForUser(BaseVertex.VertexType userType, String id, List<String> chain) {
        return new AuthorizationQuery(impl.getGraph(), userType, id, chain);
    }


    /**
     * Get members of the group, given the un-prefixed group name
     *
     * @param group un-prefixed group name
     * @return the set of users
     */
    private Set<User> getGroupMembers(String group) throws VertexNotFoundException {
        return groupMembersQuery.getGroupMembers(new GroupNameHelper().addRootGroupPrefix(group));
    }


    /**
     * Update cache for all group members
     *
     * @param group un-prefixed group name
     */
    private void updateCacheForGroupMembers(String group) {
         try {
            updateCacheForGroupMembers(getGroupMembers(group));
        } catch (VertexNotFoundException e) {
            logger.error("Failed to get members of group: {}. Group not found", group, e);
        }
    }

    /**
     * Update cache for the given group members
     *
     * @param groupMembers users for whom to update the cache
     */
    private void updateCacheForGroupMembers(Set<ezbake.groups.graph.frames.vertex.User> groupMembers) {
        Stopwatch watch = getStopwatch();
        logger.info("Begin update cache for all group members");
        for (ezbake.groups.graph.frames.vertex.User member : groupMembers) {
            try {
                logger.info("Updating cache for: {}:{}", member.getType(), member.getPrincipal());
                cache.updateAll(buildQueryForUser(member.getType(), member.getPrincipal(), null));
            } catch (Exception e) {
                logger.error("Failed to update cache for: {}:{}", member.getType(), member.getPrincipal(), e);
            }
        }
        logStopwatch(watch, "End update cache for all group members");
    }

    private void markCacheNeedsUpdateForGroupMembers(String group) {
        try {
            markCacheNeedsUpdateForGroupMembers(getGroupMembers(group));
        } catch (VertexNotFoundException e) {
            logger.error("Failed to get members of group: {}. Group not found", group, e);
        }
    }

    private void markCacheNeedsUpdateForGroupMembers(Set<ezbake.groups.graph.frames.vertex.User> groupMembers) {
        Stopwatch watch = getStopwatch();
        logger.info("Begin cache mark for update for all group members");
        for (ezbake.groups.graph.frames.vertex.User member : groupMembers) {
            try {
                logger.info("Mark all for update: {}:{}", member.getType(), member.getPrincipal());
                cache.markAllForUpdate(
                        buildQueryForUser(member.getType(), member.getPrincipal(), null).getWildCardKey());
            } catch (Exception e) {
                logger.error("Failed to mark for update: {}:{}", member.getType(), member.getPrincipal(), e);
            }
        }
        logStopwatch(watch, "End mark for update for all group members");
    }


    private Stopwatch getStopwatch() {
        Stopwatch watch = Stopwatch.createUnstarted();
        if (logTimer) {
            watch.start();
        }
        return watch;
    }

    private void logStopwatch(String messageFormat, Object... args) {
        if (logTimer) {
            logger.info(messageFormat, args);
        }

    }


    public void logStopwatch(Stopwatch timer, String message, Object... args) {
        if (logTimer) {
            message = String.format(message, args);
            logger.info("TIMER: {} ----------> {}ms", message, timer.elapsed(TimeUnit.MILLISECONDS));
        }
    }

}
