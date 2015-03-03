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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.common.properties.EzProperties;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.EzGroupsGraph;
import ezbake.groups.graph.EzGroupsGraphModule;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.AccessDeniedException;
import ezbake.groups.graph.exception.IndexUnavailableException;
import ezbake.groups.graph.exception.InvalidGroupNameException;
import ezbake.groups.graph.exception.InvalidVertexTypeException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexExistsException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.edge.BaseEdge.EdgeType;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.query.SpecialAppGroupQuery;
import ezbake.groups.thrift.AllGroupMembers;
import ezbake.groups.thrift.AuthorizationError;
import ezbake.groups.thrift.AuthorizationException;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.EzGroupOperationException;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryError;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.groups.thrift.OperationError;
import ezbake.groups.thrift.UserGroup;
import ezbake.groups.thrift.UserGroupPermissions;
import ezbake.groups.thrift.UserType;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.common.core.SecurityID;
import ezbake.thrift.authentication.EzX509;
import ezbake.thrift.authentication.ThriftPeerUnavailableException;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;

/**
 * This is the implementation of the EzGroups thrift service. It implements the EzBakeBaseThriftService, and can
 * be run with thrift runner if built into a jar with dependencies.
 */
public class EzGroupsService extends EzBakeBaseThriftService implements EzGroups.Iface {
    private static final Logger logger = LoggerFactory.getLogger(EzGroupsService.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(EzGroupsService.class);

    /* Instance variables */
    private final EzProperties ezProperties;
    private EzbakeSecurityClient ezbakeSecurityClient;
    private EzGroupsGraph graph;

    /* Helpers */
    GroupNameHelper nameHelper = new GroupNameHelper();

    /* Special queries */
    SpecialAppGroupQuery specialAppGroupQuery;

    /**
     * Default constructor
     *
     * Should only be used by ThriftRunner to bootstrap creating a real instance
     */
    public EzGroupsService() {
        ezProperties = null;
        ezbakeSecurityClient = null;
        graph = null;
    }

    @Inject
    public EzGroupsService(Properties ezProperties, EzGroupsGraph graph) {
        this.ezProperties = new EzProperties(ezProperties, true);
        this.ezbakeSecurityClient = new EzbakeSecurityClient(ezProperties);
        this.graph = graph;

        specialAppGroupQuery = new SpecialAppGroupQuery(graph.getFramedGraph(), graph.getAppGroupId());
    }

    private List<EzGroupsService> instances = Lists.newArrayList();
    @Override
    public TProcessor getThriftProcessor() {
        Properties configurationProperties = getConfigurationProperties();
        EzProperties p = new EzProperties(getConfigurationProperties(), true);
        p.setTextCryptoProvider(new SystemConfigurationHelper(configurationProperties).getTextCryptoProvider());

        EzGroupsService instance = Guice.createInjector(new EzGroupsGraphModule(p)).getInstance(EzGroupsService.class);
        instances.add(instance);

        return new EzGroups.Processor<>(instance);
    }

    @Override
    public void shutdown() {
        for (EzGroupsService instance : instances) {
            // Right now graph is null because we return a *new* instance from getThriftProcessor, not *this* instance
            try {
                instance.graph.close();
            } catch (IOException e) {
                logger.error("Unable to close the graph cleanly", e);
            }
        }
    }

    public EzGroupsGraph getGraph() {
        return graph;
    }

    @Override
    public boolean ping() {
        logger.info("received ping");
        return true;
    }
    @Override
    public long createGroup(EzSecurityToken ezSecurityToken, String parent, String name,
                            GroupInheritancePermissions inheritance
    ) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        return createGroupWithInclusion(ezSecurityToken, parent, name, inheritance, true, false);
    }

    @Override
    public long createGroupWithInclusion(EzSecurityToken ezSecurityToken, String parent, String name,
                            GroupInheritancePermissions inheritance, boolean includeOnlyRequiresUser,
                            boolean includeOnlyRequiresApp
    ) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        logger.info("createGroup request. Initiator: {}, Parent Group: {}, Group Name: {}, Inheritance options: {}",
                ezSecurityToken.getValidity().getIssuedTo(), parent, name, inheritance);
        validateToken(ezSecurityToken);

        // Information about the user creating the group
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtGroupRoleAdd, ezSecurityToken)
                .arg("Parent group", parent)
                .arg("New group", name)
                .arg("Group inheritance", inheritance)
                .arg("always include group if user has access", includeOnlyRequiresUser)
                .arg("always include group if app has access", includeOnlyRequiresApp);
        try {
            if (parent == null || parent.isEmpty()) {
                parent = Group.COMMON_GROUP;
            } else {
                // Always add root to the parent groups
                parent = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, parent);
            }

            // Create the group
            Group group = graph.addGroup(userTypeFromToken(ezSecurityToken), principal, name, parent, inheritance,
                    UserGroupPermissionsWrapper.ownerPermissions(), includeOnlyRequiresUser, includeOnlyRequiresApp);

            event.arg("Assigned group index", group.getIndex());
            return group.getIndex();
        } catch (VertexNotFoundException e) {
            event.failed();
            logger.error("Cannot create group, the parent vertex doesn't! parent name: {}, group: {}", parent, name);
            throw new EzGroupOperationException("Parent group ("+parent+") can not be found, unable to create child " +
                    "group", OperationError.PARENT_GROUP_NOT_FOUND);
        } catch (VertexExistsException e) {
            event.failed();
            logger.error("Cannot create group, the vertex already exists! group name: {}", name);
            throw new EzGroupOperationException("Group already exists", OperationError.GROUP_EXISTS);
        } catch (UserNotFoundException e) {
            event.failed();
            logger.error("Cannot create group, the owner could not be found! owner name: {}", principal);
            throw new AuthorizationException("Group owner does not exist. The user must be added to EzGroups first",
                    AuthorizationError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            event.failed();
            logger.error("Cannot create group, the owner does not have permission to create a child group! owner " +
                    "name: {}", principal);
            throw new AuthorizationException("User does not have permissions to create groups from parent group: " +
                    parent, AuthorizationError.ACCESS_DENIED);
        } catch (IndexUnavailableException e) {
            event.failed();
            logger.error("Cannot get an index for the group");
            throw new EzGroupOperationException("Unable to create group at this time. No group ID available",
                    OperationError.INDEX_UNAVAILABLE);
        } catch (InvalidGroupNameException e) {
            event.failed();
            logger.error("Create group with invalid group name: {}", e.getMessage());
            throw new EzGroupOperationException("Invalid group name passed to createGroup: "+e.getMessage(),
                    OperationError.UNRECOVERABLE_ERROR);
        } finally {
            auditLogger.logEvent(event);
        }

    }

    @Override
    public long createUser(EzSecurityToken ezSecurityToken, String principal, String name) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                .arg("Create user id", principal)
                .arg("user name", name);

        long userId;
        try {
            User user = graph.addUser(BaseVertex.VertexType.USER, principal, name);
            userId = user.getIndex();
            event.arg("Assigned group index to user", userId);
        } catch (VertexExistsException e) {
            event.arg("User already exists", true);
            event.failed();
            logger.error("Cannot create user, the vertex already exists! user name: {}", principal);
            throw new EzGroupOperationException("User with principal: "+principal+" already exists",
                    OperationError.USER_EXISTS);
        } catch (InvalidVertexTypeException|InvalidGroupNameException e) {
            event.failed();
            logger.error("Unexpected exception: {}", e);
            throw new EzGroupOperationException("Unable to create users at this time", OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            event.failed();
            logger.error("User not found?", principal);
            throw new EzGroupOperationException("User not found creating app group for user: "+principal+"the app " +
                    "group will need to be created manually", OperationError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            event.arg("Creating user does have permission to create groups", true);
            event.failed();
            logger.error("User does not have permission to create groups");
            throw new AuthorizationException("User does not have permissions for creating groups. This usually " +
                    "means that an app group cannot be created", AuthorizationError.ACCESS_DENIED);
        } catch (IndexUnavailableException e) {
            event.arg("Failed setting group index", true);
            event.failed();
            logger.error("Cannot get an index for the user");
            throw new EzGroupOperationException("Unable to create users at this time. No ID available",
                    OperationError.INDEX_UNAVAILABLE);
        } finally {
            auditLogger.logEvent(event);
        }

        return userId;
    }

    @Override
    public void modifyUser(EzSecurityToken ezSecurityToken, String principal, String newPrincipal) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("change user principal", principal)
                .arg("to new principal", newPrincipal);
        try {
            modifyUserOfType(BaseVertex.VertexType.USER, principal, newPrincipal, null);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void deactivateUser(EzSecurityToken ezSecurityToken, String principal) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("deactivate user", principal);
        try {
            deactivateUserOfType(BaseVertex.VertexType.USER, principal);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void activateUser(EzSecurityToken ezSecurityToken, String principal) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("activate user", principal);
        try {
            activateUserOfType(BaseVertex.VertexType.USER, principal);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void deleteUser(EzSecurityToken ezSecurityToken, String principal) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken, true);
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                .arg("delete user", principal);
        try {
            deleteUserOfType(BaseVertex.VertexType.USER, principal);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public long createAppUser(EzSecurityToken ezSecurityToken, String securityID, String name) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                .arg("Create app user id", securityID)
                .arg("app name", name);
        try {
            // Create the app user. root.app.name and root.appaccess.name will be created
            User appUser = graph.addUser(BaseVertex.VertexType.APP_USER, securityID, name);

            String appGroup = nameHelper.getNamespacedAppGroup(name, null);

            // Also create root.app.name.ezbAudits, root.app.name.ezbMetrics, root.app.name.ezbDiagnostics
            graph.addGroup(BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.AUDIT_GROUP, appGroup,
                    new GroupInheritancePermissions(false, false, false, false, false),
                    new UserGroupPermissionsWrapper(false, true, true, true, true), true, false);
            graph.addGroup(BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.METRICS_GROUP, appGroup,
                    new GroupInheritancePermissions(false, false, false, false, false),
                    UserGroupPermissionsWrapper.ownerPermissions(), true, false);
            graph.addGroup(BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.DIAGNOSTICS_GROUP,
                    appGroup, new GroupInheritancePermissions(false, false, false, false, false),
                    UserGroupPermissionsWrapper.ownerPermissions(), true, false);

            event.arg("Assigned App Group Index", appUser.getIndex());
            return appUser.getIndex();
        } catch (VertexExistsException e) {
            event.arg("Already exists", true);
            event.failed();
            logger.error("Cannot create user, the vertex already exists! user name: {}. Original Exception: {}",
                    securityID, e.getMessage());
            throw new EzGroupOperationException("User with principal: "+securityID+" already exists",
                    OperationError.USER_EXISTS);
        } catch (InvalidVertexTypeException e) {
            event.failed();
            logger.error("Invalid vertex type, this should not happen");
            throw new EzGroupOperationException("Unable to create users at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            event.failed();
            logger.error("User not found? Most likely when creating app group", securityID);
            throw new EzGroupOperationException("User not found creating app group for user: "+securityID,
                    OperationError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            event.arg("User does not have permission to create groups", true);
            event.failed();
            logger.error("User does not have permission to create groups");
            throw new AuthorizationException("User does not have permissions for creating groups. This usually " +
                    "means the app group cannot be created", AuthorizationError.ACCESS_DENIED);
        } catch (VertexNotFoundException e) {
            event.arg("Unable to create groups for app user because parent group was not found", true);
            event.failed();
            logger.error("Unable to create groups for app user because parent group was not found");
            throw new EzGroupOperationException("Unable to create groups for app user ("+securityID+") : " +
                    e.getMessage(), OperationError.PARENT_GROUP_NOT_FOUND);
        } catch (IndexUnavailableException e) {
            event.arg("Failed setting group index", true);
            event.failed();
            logger.error("Cannot get an index for the user");
            throw new EzGroupOperationException("Unable to create users at this time. No ID available",
                    OperationError.INDEX_UNAVAILABLE);
        } catch (InvalidGroupNameException e) {
            event.arg("Unable to create app groups with app name", name);
            event.failed();
            throw new EzGroupOperationException("Unable to create app users with no name",
                    OperationError.UNRECOVERABLE_ERROR);
        } finally {
            auditLogger.logEvent(event);
        }
    }



    @Override
    public void modifyAppUser(EzSecurityToken ezSecurityToken, String securityId, String newSecurityID, String newName) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        validateToken(ezSecurityToken);
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("change user principal", securityId)
                .arg("to new principal", newSecurityID)
                .arg("New name", newName);
        try {
            // If not an admin, must have admin manage access to the app group
            if (!EzSecurityTokenUtils.isEzAdmin(ezSecurityToken)) {
                // Query for app_user to determine app name
                User app;
                try {
                    app = getUser(BaseVertex.VertexType.APP_USER, securityId);
                } catch (UserNotFoundException e) {
                    throw new EzGroupOperationException("No app found with security id: " + securityId,
                            OperationError.USER_NOT_FOUND);
                }

                String appGroup = nameHelper.getNamespacedAppGroup(app.getName());
                ensureUserHasAdminGroup(
                        userTypeFromToken(ezSecurityToken),
                        ezSecurityToken.getTokenPrincipal().getPrincipal(),
                        appGroup,
                        BaseEdge.EdgeType.A_MANAGE,
                        false);
            }
            modifyUserOfType(BaseVertex.VertexType.APP_USER, securityId, newSecurityID, newName);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }

    }

    @Override
    public void deactivateAppUser(EzSecurityToken ezSecurityToken, String securityId) throws TException {
        validateToken(ezSecurityToken, true);
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("deactivate app user", securityId);
        try {
            deactivateUserOfType(BaseVertex.VertexType.APP_USER, securityId);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void activateAppUser(EzSecurityToken ezSecurityToken, String securityId) throws TException {
        validateToken(ezSecurityToken, true);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("activate app user", securityId);
        try {
            activateUserOfType(BaseVertex.VertexType.APP_USER, securityId);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }

    }

    @Override
    public void deleteAppUser(EzSecurityToken ezSecurityToken, String securityId) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        validateToken(ezSecurityToken);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                .arg("delete app user", securityId);
        try {
            // First rename the app user, which will rename all of the app groups that were created for it
            modifyAppUser(ezSecurityToken, securityId, securityId, "_DELETED_APP_"+securityId);

            // Now delete the user
            deleteUserOfType(BaseVertex.VertexType.APP_USER, securityId);
        } catch (Exception e) {
            event.arg("error", e.getMessage());
            event.failed();
            throw e;
        } finally {
            auditLogger.logEvent(event);
        }

    }

    /**
     * Get a set of all users who belong to a particular group
     *
     * @param ezSecurityToken token representing the user
     * @param groupName name of the group to query
     * @param explicitMembers only return members who belong to a group directly
     * @return a set of user external IDs
     */
    @Override
    public AllGroupMembers getGroupMembers(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers) throws EzSecurityTokenException, GroupQueryException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        return getGroupMembers(userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                realGroupName, true, true, explicitMembers);
    }

    /**
     * Get a set of all application users who belong to a particular group
     *
     * @param ezSecurityToken token representing the user
     * @param groupName name of the group to query
     * @param explicitMembers only return members who belong to a group directly
     * @return a set of user external IDs
     */
    @Override
    public Set<String> getGroupApps(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers) throws EzSecurityTokenException, GroupQueryException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        return getGroupMembers(userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                realGroupName, false, true, explicitMembers).getApps();
    }

    /**
     * Get a set of all users who belong to a particular group
     *
     * @param ezSecurityToken token representing the user
     * @param groupName name of the group to query
     * @param explicitMembers only return members who belong to a group directly
     * @return a set of user external IDs
     */
    @Override
    public Set<String> getGroupUsers(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers) throws EzSecurityTokenException, GroupQueryException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        return getGroupMembers(userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                realGroupName, true, false, explicitMembers).getUsers();
    }

    @Override
    public Set<Long> getAuthorizations(EzSecurityToken ezSecurityToken) throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        validateToken(ezSecurityToken);

        return getAuthorizations(userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(), requestChainFromToken(ezSecurityToken));
    }


    /**
     * Return a set of UserGroups, which include group information and the user's group permissions
     *
     * @param token a token granting access to EzGroups
     * @param explicitGroups whether or not to fetch all user groups, or just the ones they have direct membership to
     * @return a set of all the user's groups
     */
    @Override
    public Set<UserGroup> getUserGroups(EzSecurityToken token, boolean explicitGroups) throws GroupQueryException, EzSecurityTokenException {
        validateToken(token);
        String userId = token.getTokenPrincipal().getPrincipal();
        String requestApp = token.getValidity().getIssuedTo();

        // First get all user and apps groups
        Set<Group> userGroups = getUserGroups(BaseVertex.VertexType.USER, userId, true);
        Set<Group> appGroups = getUserGroups(BaseVertex.VertexType.APP_USER, requestApp, true);

        // Split groups into 2 sets - those that users always have (even if app doesn't) and those that users only have if app has too
        Set<Group> groupsUserHasRegardless = Sets.newHashSet();
        Set<Group> groupsDependingOnApp = Sets.newHashSet();
        for (Group g : userGroups) {
            if (g.isRequireOnlyUser()) {
                groupsUserHasRegardless.add(g);
            } else {
                groupsDependingOnApp.add(g);
            }
        }

        // Get app groups that go in regardless
        Set<Group> appAlwaysGetsGroups = Sets.newHashSet();
        for (Group g : appGroups) {
            if (g.isRequireOnlyApp()) {
                appAlwaysGetsGroups.add(g);
            }
        }

        // Filter the groups that depend on the app
        if (!groupsDependingOnApp.isEmpty()) {
            groupsDependingOnApp = Sets.intersection(groupsDependingOnApp, appGroups);
        }

        // Now union the sets to get the users final list
        Set<Group> groups = Sets.union(groupsUserHasRegardless, groupsDependingOnApp);
        if (!appAlwaysGetsGroups.isEmpty()) {
            groups = Sets.union(groups, appAlwaysGetsGroups);
        }
        
        // Transform
        Set<UserGroup> ugs = new HashSet<>();
        for (Group g : groups) {
            ezbake.groups.thrift.Group group = new ezbake.groups.thrift.Group(g.getIndex(), g.getGroupName());
            try {
                group.setInheritancePermissions(graph.getGroupInheritancePermissions(g.getGroupName()));
            } catch (VertexNotFoundException e) {
                logger.error(g.getGroupName(), e);
            }
            group.setRequireOnlyUser(g.isRequireOnlyUser());
            group.setRequireOnlyAPP(g.isRequireOnlyApp());
            
            group.setGroupName(nameHelper.removeRootGroupPrefix(g.getGroupName()));

            // Figure out what the users permissions are
            UserGroupPermissions ugp = new UserGroupPermissions();
            try {
                Set<BaseEdge.EdgeType> edges = graph.userPermissionsOnGroup(BaseVertex.VertexType.USER, userId, g.getGroupName());
                for (BaseEdge.EdgeType edge : edges) {
                    switch(edge) {
                        case DATA_ACCESS:
                            ugp.setDataAccess(true);
                            break;
                        case A_CREATE_CHILD:
                            ugp.setAdminCreateChild(true);
                            break;
                        case A_MANAGE:
                            ugp.setAdminManage(true);
                            break;
                        case A_READ:
                            ugp.setAdminRead(true);
                            break;
                        case A_WRITE:
                            ugp.setAdminWrite(true);
                            break;
                        case COMPLEX_DATA_ACCESS:
                            break;
                    default:
                        break;
                    }
                }

            } catch (UserNotFoundException|VertexNotFoundException e) {
                // ignore. We just won't have permissions
            }

            ugs.add(new UserGroup(group, ugp));
        }
        return ugs;
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is a diagnostic user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has diagnostic access to
     */
    @Override
    public Set<String> getUserDiagnosticApps(EzSecurityToken ezSecurityToken) throws EzSecurityTokenException, EzGroupOperationException, GroupQueryException {
        validateToken(ezSecurityToken);
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        return querySpecialGroupForApps(userTypeFromToken(ezSecurityToken), principal,
                EzGroupsConstants.DIAGNOSTICS_GROUP);
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is a metrics user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has metrics access to
     */
    @Override
    public Set<String> getUserMetricsApps(EzSecurityToken ezSecurityToken) throws EzSecurityTokenException, EzGroupOperationException, GroupQueryException {
        validateToken(ezSecurityToken);
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        return querySpecialGroupForApps(userTypeFromToken(ezSecurityToken), principal,
                EzGroupsConstants.METRICS_GROUP);
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is an audit user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has auditor access to
     */
    @Override
    public Set<String> getUserAuditorApps(EzSecurityToken ezSecurityToken) throws EzSecurityTokenException, EzGroupOperationException, GroupQueryException {
        validateToken(ezSecurityToken);
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        return querySpecialGroupForApps(userTypeFromToken(ezSecurityToken), principal,
                EzGroupsConstants.AUDIT_GROUP);
    }

    @Override
    public boolean checkUserAccessToGroup(EzSecurityToken ezSecurityToken, String groupName) throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        validateToken(ezSecurityToken);
        EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        // First get the user and app user vertex
        User user;
        User appUser;
        try {
            user = graph.getUser(userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal());
            appUser = graph.getUser(BaseVertex.VertexType.APP_USER, wrapper.getSecurityId());
        } catch (InvalidVertexTypeException|UserNotFoundException e) {
            logger.info("Unable to get user: {}", e.getMessage());
            throw new GroupQueryException("Unable to get user: "+e.getMessage(), GroupQueryError.USER_NOT_FOUND);
        }

        // Now get the group and check both accesses
        Group group;
        try {
            group = graph.getGroup(realGroupName);
        } catch (VertexNotFoundException e) {
            logger.info("Group does not exist to check access: {}", groupName);
            throw new GroupQueryException("Group not found: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
        }

        // Determine user access. User must have access, and if not require only user check app, give unconditionally
        // if require only app
        boolean userAccess = graph.pathExists(user.asVertex(), group.asVertex(), BaseEdge.EdgeType.DATA_ACCESS.toString());
        if (!group.isRequireOnlyUser() || group.isRequireOnlyApp()) {
            boolean appAccess = graph.pathExists(appUser.asVertex(), group.asVertex(), BaseEdge.EdgeType.DATA_ACCESS.toString());
            if (group.isRequireOnlyApp()) {
                userAccess = appAccess;
            } else if (!group.isRequireOnlyUser()) {
                userAccess = userAccess && appAccess;
            }
        }

        return userAccess;
    }

    /**
     * Return a set of UserGroups, which include group information and the user's group permissions
     *
     * @param token a token granting access to EzGroups
     * @param explicitGroups whether or not to fetch all user groups, or just the ones they have direct membership to
     * @return a set of all the apps groups
     */
    @Override
    public Set<UserGroup> getAppUserGroups(EzSecurityToken token, boolean explicitGroups) throws EzSecurityTokenException, GroupQueryException {
        validateToken(token);
        String securityId = token.getTokenPrincipal().getPrincipal();

        Set<UserGroup> ugs = new HashSet<>();
        Set<Group> groups = getUserGroups(BaseVertex.VertexType.APP_USER, securityId, true);
        for (Group g : groups) {
            ezbake.groups.thrift.Group group = new ezbake.groups.thrift.Group(g.getIndex(), g.getGroupName());
            try {
                group.setInheritancePermissions(graph.getGroupInheritancePermissions(g.getGroupName()));
            } catch (VertexNotFoundException e) {
                logger.error(g.getGroupName(), e);
            }
            group.setRequireOnlyUser(g.isRequireOnlyUser());
            group.setRequireOnlyAPP(g.isRequireOnlyApp());
            
            group.setGroupName(nameHelper.removeRootGroupPrefix(g.getGroupName()));
            // Figure out what the users permissions are
            UserGroupPermissions ugp = new UserGroupPermissions();
            try {
                Set<BaseEdge.EdgeType> edges = graph.userPermissionsOnGroup(BaseVertex.VertexType.APP_USER, securityId, g.getGroupName());
                for (BaseEdge.EdgeType edge : edges) {
                    switch(edge) {
                        case DATA_ACCESS:
                            ugp.setDataAccess(true);
                            break;
                        case A_CREATE_CHILD:
                            ugp.setAdminCreateChild(true);
                            break;
                        case A_MANAGE:
                            ugp.setAdminManage(true);
                            break;
                        case A_READ:
                            ugp.setAdminRead(true);
                            break;
                        case A_WRITE:
                            ugp.setAdminWrite(true);
                            break;
                        case COMPLEX_DATA_ACCESS:
                            break;
                        default:
                            break;
                    }
                }
            } catch (UserNotFoundException|VertexNotFoundException e) {
                // ignore. We just won't have permissions
            }

            ugs.add(new UserGroup(group, ugp));
        }

        return ugs;
    }


    /**
     * List the child groups from the specified parent. If null, all groups a user has access to will be returned
     *
     * @param ezSecurityToken a token granting access to EzGroups
     * @param groupName the group name from where the query should begin
     * @param recurse whether or not to return all children, or just direct child groups
     * @return a set of the child groups
     */
    @Override
    public Set<ezbake.groups.thrift.Group> getChildGroups(EzSecurityToken ezSecurityToken, String groupName, boolean recurse) throws EzSecurityTokenException, GroupQueryException, AuthorizationException {
        logger.info("getChildGroups request. Initiator: {}, Parent Group: {}",
                ezSecurityToken.getValidity().getIssuedTo(), groupName);
        validateToken(ezSecurityToken);

        String realGroupName = nameHelper.addRootGroupPrefix(groupName);
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        Set<ezbake.groups.thrift.Group> groupNames = Sets.newHashSet();
        try {
            Set<Group> groups = graph.getGroupChildren(userTypeFromToken(ezSecurityToken), principal, realGroupName,
                    recurse);
            
            for (Group g : groups) {
                ezbake.groups.thrift.Group group = new ezbake.groups.thrift.Group(g.getIndex(), g.getGroupName());
                try {
                    group.setInheritancePermissions(graph.getGroupInheritancePermissions(g.getGroupName()));
                } catch (VertexNotFoundException e) {
                    logger.error(g.getGroupName(), e);
                }
                group.setRequireOnlyUser(g.isRequireOnlyUser());
                group.setRequireOnlyAPP(g.isRequireOnlyApp());
                group.setIsActive(g.isActive());
                
                group.setGroupName(nameHelper.removeRootGroupPrefix(g.getGroupName()));
                groupNames.add(group);
            }
        } catch (VertexNotFoundException e) {
            logger.error("No group found getting children of {}", groupName);
            throw new GroupQueryException("No group found for name: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            logger.error("No user found getting children of {}, user id: {}", groupName, principal);
            throw new GroupQueryException("No user found for id: " + principal, GroupQueryError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            logger.error("User did not have required permissions for accessing group: {}", groupName);
            throw new AuthorizationException("User does not have access to group: " + groupName,
                    AuthorizationError.ACCESS_DENIED);
        }

        return groupNames;
    }

    /**
     * Add a user to a group, assigning them the specified direct permissions. Other permissions may still be inherited
     *
     * @param ezSecurityToken a token granting access to EzGroups
     * @param groupName the group to add the user to
     * @param securityId the app user's security Id
     * @param permissions the direct permissions the user should have
     */
    @Override
    public void addAppUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId, UserGroupPermissions permissions) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);
        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                .arg("add app user", securityId)
                .arg("to group", groupName)
                .arg("with permissions", permissions);
        try {
            // First need to check the actors permissions on the group
            boolean privileged = EzSecurityTokenUtils.isEzAdmin(ezSecurityToken) &&
                    isPrivilegedPeer(new EzX509(), SecurityID.ReservedSecurityId.INS_REG);
            ensureUserHasAdminGroup(userTypeFromToken(ezSecurityToken), principal, realGroupName,
                    BaseEdge.EdgeType.A_WRITE, privileged);

            graph.addUserToGroup(BaseVertex.VertexType.APP_USER, securityId, realGroupName, permissions.isDataAccess(),
                    permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                    permissions.isAdminCreateChild());

        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot add user to {}", groupName);
            throw new EzGroupOperationException("No group found for name: " + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            event.arg("User does not exist", securityId);
            event.failed();
            logger.error("No user found. Cannot add user {} to {}", securityId, groupName);
            throw new EzGroupOperationException("No user found, " + securityId + " will not be added to the group",
                    OperationError.USER_NOT_FOUND);
        } catch (InvalidVertexTypeException e) {
            event.failed();
            logger.error("Invalid vertex type, this should not happen");
            throw new EzGroupOperationException("Unable to add users to groups at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } finally {
            auditLogger.logEvent(event);
        }

    }

    /**
     * Add a user to a group, assigning them the specified direct permissions. Other permissions may still be inherited
     *
     * @param ezSecurityToken a token granting access to EzGroups
     * @param groupName the group to add the user to
     * @param userId the user's external id
     * @param permissions the direct permissions the user should have
     */
    @Override
    public void addUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String userId, UserGroupPermissions permissions) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken);
        logger.info("addUserToGroup request. token: {}, group: {}, user: {}, permissions: {}",
                ezSecurityToken.getValidity().getIssuedTo(), groupName, userId, permissions);

        String realGroupName = nameHelper.addRootGroupPrefix(groupName);
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                .arg("add user", userId)
                .arg("to group", groupName)
                .arg("with permissions", permissions);
        try {
            // First need to check the actors permissions on the group
            String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            boolean privileged = EzSecurityTokenUtils.isEzAdmin(ezSecurityToken) ||
                    isPrivilegedPeer(new EzX509(), SecurityID.ReservedSecurityId.INS_REG);
            ensureUserHasAdminGroup(userTypeFromToken(ezSecurityToken), principal, realGroupName,
                    BaseEdge.EdgeType.A_WRITE, privileged);

            graph.addUserToGroup(BaseVertex.VertexType.USER, userId, realGroupName, permissions.isDataAccess(),
                    permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                    permissions.isAdminCreateChild());

        } catch (VertexNotFoundException e) {
            event.arg("Group not found", groupName);
            event.failed();
            logger.error("No group found. Cannot add user to {}", groupName);
            throw new EzGroupOperationException("No group found for name: " + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } catch (InvalidVertexTypeException e) {
            event.arg("Invalid vertex type", userTypeFromToken(ezSecurityToken));
            event.failed();
            logger.error("Invalid vertex type, this should not happen");
            throw new EzGroupOperationException("Unable to add users to groupsat this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            // Create the user now, and then add them
            logger.info("User being added does not exist. Creating the user, and then reattempting to add");
            AuditEvent addUserEvent = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                    .arg("Adding user", userId);
            try {
                graph.addUser(BaseVertex.VertexType.USER, userId, "");

                graph.addUserToGroup(BaseVertex.VertexType.USER, userId, realGroupName, permissions.isDataAccess(),
                        permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                        permissions.isAdminCreateChild());
            } catch (AccessDeniedException e1) {
                addUserEvent.failed();
                logger.error("Access denied creating user. This should not have happened because it only happens for " +
                        "APP_USERS, not USERS");
                throw new EzGroupOperationException("Unable to create user: "+userId+" reason: "+e1.getMessage(),
                        OperationError.UNRECOVERABLE_ERROR);
            } catch (InvalidVertexTypeException|UserNotFoundException|VertexExistsException|InvalidGroupNameException e1) {
                addUserEvent.arg("Unable to create user", e1.getMessage());
                addUserEvent.failed();
                logger.error("Unable to create a user: {}", e1.getMessage());
                throw new EzGroupOperationException("Unable to create user: "+userId+" reason: "+e1.getMessage(),
                        OperationError.UNRECOVERABLE_ERROR);
            } catch (IndexUnavailableException e1) {
                addUserEvent.arg("Failed setting group index", true);
                addUserEvent.failed();
                logger.error("Cannot get an index for the user");
                throw new EzGroupOperationException("Unable to create users at this time. No ID available",
                        OperationError.INDEX_UNAVAILABLE);
            } catch (VertexNotFoundException e1) {
                addUserEvent.arg("Group not found", groupName);
                addUserEvent.failed();
                logger.error("No group found. Cannot add user to {}", groupName);
                throw new EzGroupOperationException("No group found for name: " + groupName,
                        OperationError.GROUP_NOT_FOUND);
            } finally {
                auditLogger.logEvent(addUserEvent);
            }
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void deactivateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken);

        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("Deactivate group", groupName)
                .arg("Deactivate child groups, too", andChildren);
        try {
            graph.setGroupActiveOrNot(userTypeFromToken(ezSecurityToken), principal, nameHelper.addRootGroupPrefix(groupName), false, andChildren);
        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot deactivate group {}", groupName);
            throw new EzGroupOperationException("Unable to deactivate group:" + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            event.arg("User does not exist", groupName);
            event.failed();
            logger.error("User not found. Cannot deactivate group {}", groupName);
            throw new EzGroupOperationException("Unable to deactivate group:" + groupName,
                    OperationError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            event.arg("User does not have permission to deactivate", groupName);
            event.failed();
            logger.error("User does not have admin manage permissions on {}", groupName);
            throw new AuthorizationException("User does not have required permissions on group: " + groupName,
                    AuthorizationError.ACCESS_DENIED);
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void activateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren) throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        validateToken(ezSecurityToken);

        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                .arg("Activate group", groupName);
        try {
            graph.setGroupActiveOrNot(userTypeFromToken(ezSecurityToken), principal, nameHelper.addRootGroupPrefix(groupName), true, andChildren);
        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot activate group {}", groupName);
            throw new EzGroupOperationException("Unable to activate group:" + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            event.arg("User does not exist", groupName);
            event.failed();
            logger.error("User not found. Cannot activate group {}", groupName);
            throw new EzGroupOperationException("Unable to activate group:" + groupName,
                    OperationError.USER_NOT_FOUND);
        } catch (AccessDeniedException e) {
            event.arg("User does not have permission to activate", groupName);
            event.failed();
            logger.error("User does not have admin manage permissions on {}", groupName);
            throw new AuthorizationException("User does not have required permissions on group: " + groupName,
                    AuthorizationError.ACCESS_DENIED);
        } finally {
            auditLogger.logEvent(event);
        }

    }

    @Override
    public void removeAppUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                .arg("remove app user", securityId)
                .arg("from group", groupName);
        try {
            // First need to check the actors permissions on the group
            String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            BaseVertex.VertexType ownerType;
            if (ezSecurityToken.getType() == TokenType.USER) {
                ownerType = BaseVertex.VertexType.USER;
            }  else {
                ownerType = BaseVertex.VertexType.APP_USER;
            }

            if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName).contains(BaseEdge.EdgeType.A_WRITE)) {
                event.arg("permissions missing", BaseEdge.EdgeType.A_WRITE.toString());
                event.failed();
                logger.error("Bad access. User: {} tried removing {} to group {}, but is lacking admin manage permissions",
                        principal, securityId, realGroupName);
                throw new AuthorizationException("This user cannot add other users to the group",
                        AuthorizationError.ACCESS_DENIED);
            }

            graph.removeUserFromGroup(BaseVertex.VertexType.APP_USER, securityId, realGroupName);

        } catch (UserNotFoundException e) {
            event.arg("User does not exist", securityId);
            event.failed();
            logger.error("No user found. Cannot remove user {} from {}", securityId, realGroupName);
            throw new EzGroupOperationException("No app user found for id: " + securityId,
                    OperationError.USER_NOT_FOUND);
        } catch (InvalidVertexTypeException e) {
            event.failed();
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to modify users at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot remove user {} from {}", securityId, groupName);
            throw new EzGroupOperationException("No group found for name: " + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } finally {
            auditLogger.logEvent(event);
        }

    }

    @Override
    public void removeUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String userId) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                .arg("remove user", userId)
                .arg("from group", groupName);
        try {
            // First need to check the actors permissions on the group
            String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            BaseVertex.VertexType ownerType;
            if (ezSecurityToken.getType() == TokenType.USER) {
                ownerType = BaseVertex.VertexType.USER;
            }  else {
                ownerType = BaseVertex.VertexType.APP_USER;
            }

            if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName).contains(BaseEdge.EdgeType.A_WRITE)) {
                event.arg("permissions missing", BaseEdge.EdgeType.A_WRITE.toString());
                event.failed();
                logger.error("Bad access. User: {} tried removing {} from group {}, but is lacking admin manage permissions",
                        principal, userId, realGroupName);
                throw new AuthorizationException("This user cannot add other users to the group",
                        AuthorizationError.ACCESS_DENIED);
            }

            graph.removeUserFromGroup(BaseVertex.VertexType.USER, userId, realGroupName);

        } catch (UserNotFoundException e) {
            event.arg("User does not exist", userId);
            event.failed();
            logger.error("No user found. Cannot remove user {} from {}", userId, realGroupName);
            throw new EzGroupOperationException("No user found for id: " + userId,
                    OperationError.USER_NOT_FOUND);
        } catch (InvalidVertexTypeException e) {
            event.failed();
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to modify users at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot remove user {} from {}", userId, groupName);
            throw new EzGroupOperationException("No group found for name: " + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void changeGroupInheritance(EzSecurityToken ezSecurityToken, String groupName, GroupInheritancePermissions inheritance) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        validateToken(ezSecurityToken);
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);

        String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
        AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtGroupRoleModify, ezSecurityToken)
                .arg("Group name", groupName)
                .arg("Set Group Ineritance", inheritance);
        try {
            // First need to check the actors permissions on the group
            BaseVertex.VertexType ownerType;
            if (ezSecurityToken.getType() == TokenType.USER) {
                ownerType = BaseVertex.VertexType.USER;
            }  else {
                ownerType = BaseVertex.VertexType.APP_USER;
            }

            if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName).contains(BaseEdge.EdgeType.A_MANAGE)) {
                event.arg("permissions missing", BaseEdge.EdgeType.A_MANAGE.toString());
                event.failed();
                logger.error("Bad access. User: {} tried removing {} from group {}, but is lacking admin manage permissions",
                        principal, principal, realGroupName);
                throw new AuthorizationException("This user cannot add other users to the group",
                        AuthorizationError.ACCESS_DENIED);
            }

            graph.setGroupInheritance(groupName, inheritance.isDataAccess(), inheritance.isAdminRead(),
                    inheritance.isAdminWrite(), inheritance.isAdminManage(), inheritance.isAdminCreateChild());
        } catch (VertexNotFoundException e) {
            event.arg("Group does not exist", groupName);
            event.failed();
            logger.error("No group found. Cannot manage inheritance! group {}", groupName);
            throw new EzGroupOperationException("Unable to manage group inheritance for " + groupName,
                    OperationError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            event.arg("User does not exist", principal);
            event.failed();
            logger.error("No user found: {}", principal);
            throw new EzGroupOperationException("No user found: " + principal, OperationError.USER_NOT_FOUND);
        } finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void changeGroupName(EzSecurityToken ezSecurityToken, String oldName, String newName) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken);
        // TODO: need to be able to change group names! How to handle admin permissions for the subgroups?

        // First get the group and start the update...
        // Next get all child groups, and update their names


        throw new EzGroupOperationException("Change group names not implemented!", OperationError.UNRECOVERABLE_ERROR);
    }

    @Override
    public Set<Long> getGroupsMask(EzSecurityToken ezSecurityToken, Set<String> groupNames) throws EzSecurityTokenException, EzGroupOperationException {
        validateToken(ezSecurityToken);

        // Get all the groups
        groupNames = nameHelper.addRootGroupPrefix(groupNames.toArray(new String[groupNames.size()]));
        Set<Group> groups = graph.getGroups(groupNames);

        Set<Long> mask = Sets.newTreeSet();
        for (Group group : groups) {
            Long index = group.getIndex();
            if (index != null) {
                mask.add(index);
            }
        }

        return mask;
    }



    @Override
    public Set<Long> createUserAndGetAuthorizations(EzSecurityToken token, List<String> chain, String id, String name) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        logger.info("createUserAndGetAuthorizations - requesting app: {}, request chain: {}, user id: {}, user name: {}",
                token.getValidity().getIssuedTo(), chain, id, name);
        validatePrivilegedPeer(token, new EzX509());

        try {
            createUser(token, id, name);
        } catch (EzGroupOperationException userExists) {
            if (userExists.getOperation() != OperationError.USER_EXISTS) {
                throw userExists;
            }
            logger.info("user already exists, getting their group authorizations list");
        }
        logger.info("returning authorizations for user: {}", id);
        return getUserAuthorizations(token, TokenType.USER, id, chain);
    }

    @Override
    public Set<Long> createAppUserAndGetAuthorizations(EzSecurityToken token, List<String> chain, String securityId, String appName) throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        logger.info("createAppUserAndGetAuthorizations - requesting app: {}, request chain: {}, security id: {}, app name: {}",
                token.getValidity().getIssuedTo(), chain, securityId, appName);
        validatePrivilegedPeer(token, new EzX509());

        long user;
        try {
            user = createAppUser(token, securityId, appName);
        } catch (EzGroupOperationException e) {
            logger.info("user already exists, getting their group authorizations list");
            // Ok if user already exists... get the user
            try {
                user = graph.getUser(BaseVertex.VertexType.APP_USER, securityId).getIndex();
            } catch (InvalidVertexTypeException|UserNotFoundException e1) {
                // Unexpected - just throw
                logger.error("creatUserAndGetAuths: Unable to get user after VertexExistsException {}", e.getMessage());
                throw new EzGroupOperationException("User supposedly existed, but now unable to get user: "+
                        e.getMessage(), OperationError.UNRECOVERABLE_ERROR);
            }
        }

        Set<Long> authorizations = getGroupIds(BaseVertex.VertexType.APP_USER, securityId, false);
        authorizations.add(user);

        return authorizations;
    }

    /**
     * Get a set of longs containing all of the group IDs the user is a member of
     *
     * @param token a token granting access to EzGroups
     * @param userType the type of token this will be for, either USER or APP
     * @param userId the user for which groups will be queried
     * @param chain the chain of apps in the request
     * @return a bitvector with a bit set for every group the user is a member of
     */
    @Override
    public Set<Long> getUserAuthorizations(EzSecurityToken token, TokenType userType, String userId, List<String> chain) throws EzSecurityTokenException, GroupQueryException {
        validatePrivilegedPeer(token, new EzX509());
        return getAuthorizations(vertexTypeFromTokenType(userType), userId, chain);
    }

    
    /**
     * Get user details such as principal and whether or not user is active
     * 
     * @param token - authorization token
     * @param userType - user type as defined in UserType enum
     * @param userID - usually DN of a user or security id for app user
     * 
     * @return User object
     */
    @Override
    public ezbake.groups.thrift.User getUser(EzSecurityToken token, UserType userType, String userID) throws EzSecurityTokenException, TException {
        validateToken(token);
        ezbake.groups.thrift.User result = new ezbake.groups.thrift.User();
        
        try {
            User user = graph.getUser(userTypeFromUserType(userType), userID);
            return result.setIsActive(user.isActive()).setName(user.getName()).setPrincipal(user.getPrincipal());
        } catch (InvalidVertexTypeException | UserNotFoundException e) {
            logger.error("Failed to retrieve user {} details", userID);
        }
        
        return result;
    }

    /**
     * Get given group metadata, i.e. whether or not group is active
     *
     * @param token EzSecurityToken belonging to the party performing the query. The user must have admin_read on the
     *              parent groupName
     * @param groupName the group name for which metadata is being requested 
     * 
     * @return group metadata
     * @throws EzSecurityTokenException 
     */
    @Override
    public ezbake.groups.thrift.Group getGroup(EzSecurityToken token, String groupName) throws EzSecurityTokenException {
        validateToken(token);

        ezbake.groups.thrift.Group result = new ezbake.groups.thrift.Group();
        String realGroupName = nameHelper.addRootGroupPrefix(groupName);
        
        try {
            Group group = graph.getGroup(vertexTypeFromTokenType(token.getType()), token.getTokenPrincipal().getPrincipal(), realGroupName);

            result.setGroupName(nameHelper.removeRootGroupPrefix(group.getGroupName()))
                .setId(group.getIndex())
                .setIsActive(group.isActive())
                .setRequireOnlyUser(group.isRequireOnlyUser())
                .setRequireOnlyAPP(group.isRequireOnlyApp())
                .setInheritancePermissions(graph.getGroupInheritancePermissions(group.getGroupName()));
        } catch (Exception e) {
            logger.error("Failed to get group {}. Failed with exception: {}", realGroupName, e.getMessage());
        }
        
        return result;
    }


    /**
     * Validate an EzSecurityToken, using the EzbakeSecurityClient to make sure it is valid, but performing no other
     * access checks
     *
     * @param token a received EzSecurityToken to be validated
     * @throws EzSecurityTokenException
     */
    private void validateToken(EzSecurityToken token) throws EzSecurityTokenException {
        validateToken(token, false);
    }


    /**
     * Validate an EzSecurityToken, using the EzbakeSecurityClient to make sure it is valid. If requireAdmin is true,
     * then the token must belong to an EzAdmin
     *
     * @param token a received EzSecurityToken to be validated
     * @param requireAdmin whether or not to require administrator privileges
     * @throws EzSecurityTokenException if the token fails to validate, or requireAdmin is true and it is not EzAdmin
     */
    private void validateToken(EzSecurityToken token, boolean requireAdmin) throws EzSecurityTokenException {
        logger.debug("Validating security token from: {} require admin: {}", token.getValidity().getIssuedTo(), requireAdmin);
        ezbakeSecurityClient.validateReceivedToken(token);
        if (requireAdmin && !EzbakeSecurityClient.isEzAdmin(token)) {
            logger.info("Rejecing security token from: {}. EzAdmin is required!", token.getValidity().getIssuedTo());
            throw new EzSecurityTokenException("EzAdmin permissions are required");
        }
    }


    protected static final String X509_RESTRICT = "ezbake.groups.service.x509.restrict";
    private boolean isPrivilegedPeer(EzX509 peer, SecurityID.ReservedSecurityId ...requiredPeers) {
        boolean privileged;
        if (ezProperties.getBoolean(X509_RESTRICT, true)) {
            try {
                privileged = isPrivilegedPeer(peer.getPeerSecurityID(), requiredPeers);
            } catch (ThriftPeerUnavailableException e) {
                privileged = false;
            }
        } else {
            // Not restricting based on x509 certificates, let them through
            privileged = true;
        }
        return privileged;
    }
    private boolean isPrivilegedPeer(String peerSId, SecurityID.ReservedSecurityId ...requiredPeers) {
        boolean privileged = false;
        List<SecurityID.ReservedSecurityId> allowedPeers = Lists.newArrayList(requiredPeers);
        if (SecurityID.ReservedSecurityId.isReserved(peerSId)) {
            if (allowedPeers.contains(SecurityID.ReservedSecurityId.fromEither(peerSId))) {
                privileged = true;
            }
        }
        return privileged;
    }

    private void validatePrivilegedPeer(EzSecurityToken token, EzX509 peer) throws EzSecurityTokenException {
        validateToken(token);

        if (ezProperties.getBoolean(X509_RESTRICT, true)) {
            try {
                String peerSId = peer.getPeerSecurityID();
                if (!isPrivilegedPeer(peerSId, SecurityID.ReservedSecurityId.EzSecurity)) {
                    logger.error("createUserAndGetAuthorizations request from unauthorized peer; {}", peerSId);
                    throw new EzSecurityTokenException("createUserAndGetAuthorizations may only be called by EzSecurity, " +
                            "not: " + peerSId);
                }
            } catch (ThriftPeerUnavailableException e) {
                logger.error("createUserAndGetAuthorizations request unable to get peer CN from X509 cert");
                throw new EzSecurityTokenException("peer CN unavailable from X509 cert. Are you using SSL?");
            }
        }
    }

    private void ensureUserHasAdminGroup(BaseVertex.VertexType userType, String userId, String groupName,
                                         BaseEdge.EdgeType adminType, boolean privileged
    ) throws AuthorizationException {
        String err = null;
        try {
            if (!graph.userPermissionsOnGroup(userType, userId, groupName).contains(adminType)) {
                err = userId+" does not have the required permission on: "+nameHelper.removeRootGroupPrefix(groupName);
            }
        } catch (UserNotFoundException|VertexNotFoundException e) {
            err = "user: " + userId + " does not exist. " + e.getMessage();
        }

        if (err != null && !privileged) {
            logger.error("Bad access. User: {} does not have {} on  group {} - {}", userId, adminType, groupName, err);
            throw new AuthorizationException(err, AuthorizationError.ACCESS_DENIED);
        }
    }

    /**
     * Will get a user's authorizations, filtering by the groups that apps in the filter chain have access to
     *
     * @param userType
     * @param userId
     * @return
     */
    private Set<Long> getAuthorizations(BaseVertex.VertexType userType, String userId, List<String> appFilterChain) throws GroupQueryException {
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
        Set<Group> userGroups = getUserGroups(userType, userId, false);

        // These are the groups the apps always include, even if the user doesn't have access
        List<Set<Group>> appsGroups = getAuthorizationsForApps(appFilterChain, false);
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
                groupsDependingOnApp = Sets.intersection(groupsDependingOnApp, appsFilter);
            }

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

        return Sets.union(auths, groupsAppsAlwaysInclude);
    }

    private List<Set<Group>> getAuthorizationsForApps(List<String> apps, boolean includeInactive) {
        List<Set<Group>> appAuthorizations = Lists.newArrayList();

        for (String securityId : apps) {
            try {
                appAuthorizations.add(getUserGroups(BaseVertex.VertexType.APP_USER, securityId, includeInactive));
            } catch (GroupQueryException e) {
                logger.info("No groups returned for app: {}", securityId);
            }
        }
        return appAuthorizations;
    }


    private AllGroupMembers getGroupMembers(BaseVertex.VertexType userType, String userId, String groupName,
                                            boolean includeUsers, boolean includeApps, boolean explicit
    ) throws GroupQueryException {
        Set<String> userPrincipals = new HashSet<>();
        Set<String> appPrincipals = new HashSet<>();
        try {
            Set<User> users = graph.groupMembers(userType, userId, groupName, includeUsers, includeApps, explicit);
            for (User user : users ) {
                logger.debug("Group: {} has user: {} - {}", groupName, user.getPrincipal(), user.getIndex());
                switch (user.getType()) {
                    case USER:
                        userPrincipals.add(user.getPrincipal());
                        break;
                    case APP_USER:
                        appPrincipals.add(user.getPrincipal());
                        break;
                    case APP_GROUP:
                        break;
                    case GROUP:
                        break;
                    case GROUP_MAPPING:
                        break;
                    default:
                        break;
                }
            }
        } catch (VertexNotFoundException e) {
            logger.error("Group {} not found", groupName);
            throw new GroupQueryException("No group found with name: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
        } catch (UserNotFoundException e) {
            logger.error("User {}({}) not found", userId, userType);
            throw new GroupQueryException("No user found: "+userId+" ("+userType+")", GroupQueryError.USER_NOT_FOUND);
        }

        AllGroupMembers members = new AllGroupMembers();
        members.setApps(appPrincipals);
        members.setUsers(userPrincipals);
        return members;
    }

    private User getTokenSubject(EzSecurityToken token) throws UserNotFoundException {
        return getUser(userTypeFromToken(token), token.getTokenPrincipal().getPrincipal());
    }
    private User getUser(BaseVertex.VertexType type, String principal) throws UserNotFoundException {
        try {
            return graph.getUser(type, principal);
        } catch (InvalidVertexTypeException e) {
            throw new UserNotFoundException("User was of invalid user type", e);
        }

    }

    private Set<Group> getUserGroups(BaseVertex.VertexType type, String id, boolean includeInactive) throws GroupQueryException {
        Set<Group> gs;
        try {
            // Get the groups from the graph and add them to the set
            gs = graph.userGroups(type, id, false, includeInactive);
        } catch (UserNotFoundException e) {
            logger.error("Cannot get user groups for {}. User Not found", id);
            throw new GroupQueryException("User "+id+" not found. Cannot compute group user groups",
                    GroupQueryError.USER_NOT_FOUND);
        }

        return gs;
    }

    private Set<Long> getGroupIds(BaseVertex.VertexType type, String id, boolean includeInactive) throws GroupQueryException {
        Set<Group> appGroups = getUserGroups(type, id, includeInactive);
        Set<Long> auths = Sets.newHashSet();

        for (Group g : appGroups) {
            auths.add(g.getIndex());
        }
        return auths;
    }

    /**
     * Modify a user by updating their external ID
     *
     * @param type type of the user, app user or user
     * @param id the user id
     * @param newId the new user id
     * @param newName the new user name
     * @throws EzGroupOperationException
     */
    private void modifyUserOfType(BaseVertex.VertexType type, String id, String newId, String newName) throws EzGroupOperationException {
        try {
            graph.updateUser(type, id, newId, newName);
        } catch (InvalidVertexTypeException e) {
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to modify users at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            logger.error("Cannot update principal from {} -> {}. User Not found", id, newId);
            throw new EzGroupOperationException("User " + id + " not found. Cannot update principal",
                    OperationError.USER_NOT_FOUND);
        } catch (VertexNotFoundException e) {
            logger.error("Cannot update principal from {} -> {} with new name: {}. Group was not found. ", id, newId,
                    newName, e.getMessage());
            throw new EzGroupOperationException("Failed changing id:" + id + " to:" + newId + " with name:" + newName +
                    ". Failed updating group names. " + e.getMessage(), OperationError.USER_NOT_FOUND);
        }
    }

    /**
     * Activate a user. The user will immediately become active
     *
     * @param type type of the user, app user or user
     * @param id the user id
     * @throws EzGroupOperationException
     */
    private void activateUserOfType(BaseVertex.VertexType type, String id) throws EzGroupOperationException {
        try {
            graph.setUserActiveOrNot(type, id, true);
        } catch (InvalidVertexTypeException e) {
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to activate users at this time. Internal error, invalid " +
                    "user type", OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            logger.error("Cannot activate user {} ({}). User Not found", id, type);
            throw new EzGroupOperationException("User "+id+" not found. Cannot activate the user",
                    OperationError.USER_NOT_FOUND);
        }
    }

    /**
     * Deactivate a user, The user should not be granted any accesses from the service while inactive
     *
     * @param type type of the user, app user or user
     * @param id the user id
     * @throws EzGroupOperationException
     */
    private void deactivateUserOfType(BaseVertex.VertexType type, String id) throws EzGroupOperationException {
        try {
            graph.setUserActiveOrNot(type, id, false);
        } catch (InvalidVertexTypeException e) {
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to deactivate users at this time. Internal error, invalid " +
                    "user type", OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            logger.error("Cannot deactivate user from {}. User Not found", id);
            throw new EzGroupOperationException("User "+id+" not found. Cannot deactivate the user",
                    OperationError.USER_NOT_FOUND);
        }
    }

    /**
     * Delete a user from the graph. This may leave the group orphaned
     *
     * @param type type of user to delete, app_user or user
     * @param id the user id
     * @throws EzGroupOperationException
     */
    private void deleteUserOfType(BaseVertex.VertexType type, String id) throws EzGroupOperationException {
        try {
            graph.deleteUser(type, id);
        } catch (InvalidVertexTypeException e) {
            logger.error("Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and " +
                    "BaseVertex.VertexType.APP_USER");
            throw new EzGroupOperationException("Unable to activate users at this time. Internal error, invalid " +
                    "user type", OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            logger.error("Cannot delete user from {}. User Not found", id);
            throw new EzGroupOperationException("User "+id+" not found. Cannot delete the user",
                    OperationError.USER_NOT_FOUND);
        }
    }


    private Set<String> querySpecialGroupForApps(BaseVertex.VertexType type, String id, String group)
            throws EzGroupOperationException, GroupQueryException {
        Set<String> groups;
        try {
            User user = graph.getUser(type, id);
            groups = specialAppGroupQuery.specialAppNamesQuery(group, user.asVertex());

        } catch (InvalidVertexTypeException e) {
            logger.error("Invalid vertex type, this should not happen");
            throw new EzGroupOperationException("Unable to query users at this time",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (UserNotFoundException e) {
            logger.error("No user found id: {}", id);
            throw new GroupQueryException("No user found for id: " + id, GroupQueryError.USER_NOT_FOUND);
        }

        return groups;
    }


    private BaseVertex.VertexType userTypeFromUserType(UserType type) {
        if(type == UserType.USER) {
            return BaseVertex.VertexType.USER;
        } else {
            return BaseVertex.VertexType.APP_USER;
        }
    }

    private BaseVertex.VertexType userTypeFromToken(EzSecurityToken token) {
        return vertexTypeFromTokenType(token.getType());
    }

    private BaseVertex.VertexType vertexTypeFromTokenType(TokenType type) {
        if (type == TokenType.USER) {
            return BaseVertex.VertexType.USER;
        }  else {
            return BaseVertex.VertexType.APP_USER;
        }
    }

    private List<String> requestChainFromToken(EzSecurityToken token) {
        List<String> appChain = token.getTokenPrincipal().getRequestChain();
        if (appChain == null) {
            appChain = Lists.newArrayList();
        }

        String issuedTo = token.getValidity().getIssuedTo();
        String issuedFor = token.getValidity().getIssuedFor();
        if (!appChain.contains(issuedTo)) {
            appChain.add(issuedTo);
        }
        if (!appChain.contains(issuedFor)) {
            appChain.add(issuedFor);
        }

        return appChain;
    }
}
