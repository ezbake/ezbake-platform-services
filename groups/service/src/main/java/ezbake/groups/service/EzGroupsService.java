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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.metrics.MetricRegistryThrift;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.graph.UserGroupPermissionsWrapper;
import ezbake.groups.graph.exception.AccessDeniedException;
import ezbake.groups.graph.exception.IndexUnavailableException;
import ezbake.groups.graph.exception.InvalidGroupNameException;
import ezbake.groups.graph.exception.InvalidVertexTypeException;
import ezbake.groups.graph.exception.UserNotFoundException;
import ezbake.groups.graph.exception.VertexExistsException;
import ezbake.groups.graph.exception.VertexNotFoundException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.AllGroupMembers;
import ezbake.groups.thrift.AuthorizationError;
import ezbake.groups.thrift.AuthorizationException;
import ezbake.groups.thrift.EzGroupOperationException;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryError;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.groups.thrift.GroupsRequest;
import ezbake.groups.thrift.GroupsRequestResponse;
import ezbake.groups.thrift.OperationError;
import ezbake.groups.thrift.UserGroup;
import ezbake.groups.thrift.UserGroupPermissions;
import ezbake.groups.thrift.UserGroupsRequest;
import ezbake.groups.thrift.UserType;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.common.core.SecurityID;
import ezbake.thrift.authentication.EzX509;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;

/**
 * This is the implementation of the EzGroups thrift service. It implements the EzBakeBaseThriftService, and can be run
 * with thrift runner if built into a jar with dependencies.
 */
public class EzGroupsService extends BaseGroupsService {
    /**
     * Value used to populate the map returned by getGroupNamesByIndices when a name cannot be returned.
     */
    public static final String UNABLE_TO_RETRIEVE_GROUP_NAME = "<NOT_FOUND>";

    private static final Logger logger = LoggerFactory.getLogger(EzGroupsService.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(EzGroupsService.class);

    /**
     * Predicate for filtering out the root group.
     */
    private static final Predicate<Group> IS_ROOT_GROUP = new Predicate<Group>() {
        @Override
        public boolean apply(@Nullable Group input) {
            return input != null && input.getGroupName().equals(EzGroupsConstants.ROOT);
        }
    };

    /* Instance variables */
    private final GroupsGraph graph;

    @Inject
    public EzGroupsService(Properties configuration, GroupsGraph graph, EzbakeSecurityClient ezbakeSecurityClient) {
        super(configuration, ezbakeSecurityClient);
        this.graph = graph;
    }

    public GroupsGraph getGraph() {
        return graph;
    }

    @Override
    public boolean ping() {
        logger.info("received ping");
        return true;
    }

    @Override
    public MetricRegistryThrift getMetricRegistryThrift() {
        return new MetricRegistryThrift();
    }

    @Override
    public long createGroup(EzSecurityToken ezSecurityToken, String parent, String name,
            GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            return createGroupWithInclusion(ezSecurityToken, parent, name, inheritance, true, false);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public ezbake.groups.thrift.Group createAndGetGroup(EzSecurityToken ezSecurityToken, String parent, String name,
            GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            return createAndGetGroupWithInclusion(ezSecurityToken, parent, name, inheritance, true, false);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public long createGroupWithInclusion(EzSecurityToken ezSecurityToken, String parent, String name,
            GroupInheritancePermissions inheritance, boolean includeOnlyRequiresUser, boolean includeOnlyRequiresApp)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            return createAndGetGroupWithInclusion(
                    ezSecurityToken, parent, name, inheritance, includeOnlyRequiresUser, includeOnlyRequiresApp)
                    .getId();
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public ezbake.groups.thrift.Group createAndGetGroupWithInclusion(EzSecurityToken ezSecurityToken, String parent,
            String name, GroupInheritancePermissions inheritance, boolean includeOnlyRequiresUser,
            boolean includeOnlyRequiresApp)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            logger.debug(
                    "createGroup request. Initiator: {}, Parent Group: {}, Group Name: {}, Inheritance options: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), parent, name, inheritance);

            validateToken(ezSecurityToken);

            // Information about the user creating the group
            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtGroupRoleAdd, ezSecurityToken)
                    .arg("Parent group", parent).arg("New group", name).arg("Group inheritance", inheritance)
                    .arg("always include group if user has access", includeOnlyRequiresUser)
                    .arg("always include group if app has access", includeOnlyRequiresApp);

            // Always add root to the parent groups
            if (Strings.isNullOrEmpty(parent) || parent.trim().isEmpty()) {
                parent = EzGroupsConstants.ROOT;
            } else {
                parent = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(EzGroupsConstants.ROOT, parent);
            }

            final String parentWithoutRoot = nameHelper.removeRootGroupPrefix(parent);

            try {
                // Create the group
                final Group group = graph.addGroup(
                        userTypeFromToken(ezSecurityToken), principal, name, parent, inheritance,
                        UserGroupPermissionsWrapper.ownerPermissions(), includeOnlyRequiresUser,
                        includeOnlyRequiresApp);

                event.arg("Assigned group index", group.getIndex());

                // Create a thrift group from our group-vertex and return it
                final ezbake.groups.thrift.Group tGroup = internalGroupToThriftGroup(group, inheritance);
                graph.commitTransaction();
                return tGroup;
            } catch (final VertexNotFoundException e) {
                event.failed();
                logger.error(
                        "Cannot create group, the parent vertex doesn't exist! parent name: {}, group: {}",
                        parentWithoutRoot, name);

                throw new EzGroupOperationException(
                        String.format(
                                "Parent group (%s) can not be found, unable to create child " + "group",
                                parentWithoutRoot), OperationError.PARENT_GROUP_NOT_FOUND);
            } catch (final VertexExistsException e) {
                event.failed();
                logger.error("Cannot create group, the vertex already exists! group name: {}", name);
                throw new EzGroupOperationException("Group already exists", OperationError.GROUP_EXISTS);
            } catch (final UserNotFoundException e) {
                event.failed();
                logger.error("Cannot create group, the owner could not be found! owner name: {}", principal);
                throw new AuthorizationException(
                        "Group owner does not exist. The user must be added to EzGroups first",
                        AuthorizationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.failed();
                logger.error(
                        "Cannot create group, the owner does not have permission to create a child group! owner "
                                + "name: {}", principal);
                throw new AuthorizationException(
                        "User does not have permissions to create groups from parent group: " + parentWithoutRoot,
                        AuthorizationError.ACCESS_DENIED);
            } catch (final IndexUnavailableException e) {
                event.failed();
                logger.error("Cannot get an index for the group");
                throw new EzGroupOperationException(
                        "Unable to create group at this time. No group ID available", OperationError.INDEX_UNAVAILABLE);
            } catch (final InvalidGroupNameException e) {
                event.failed();
                logger.error("Create group with invalid group name: {}", e.getMessage());
                throw new EzGroupOperationException(
                        "Invalid group name passed to createGroup: " + e.getMessage(),
                        OperationError.UNRECOVERABLE_ERROR);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public long createUser(EzSecurityToken ezSecurityToken, String principal, String name)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "createUser request. Initiator: {}, UserId: {}, UserName: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), principal, name);

            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken).arg("Create user id", principal)
                            .arg("user name", name);

            long userId;

            try {
                final User user = graph.addUser(BaseVertex.VertexType.USER, principal, name);
                userId = user.getIndex();
                graph.commitTransaction();
                event.arg("Assigned group index to user", userId);
            } catch (final VertexExistsException e) {
                event.arg("User already exists", true);
                event.failed();
                logger.error("Cannot create user, the vertex already exists! user name: {}", principal);
                throw new EzGroupOperationException(
                        "User with principal: " + principal + " already exists", OperationError.USER_EXISTS);
            } catch (InvalidVertexTypeException | InvalidGroupNameException e) {
                event.failed();
                logger.error("Unexpected exception.", e);
                throw new EzGroupOperationException(
                        "Unable to create users at this time", OperationError.UNRECOVERABLE_ERROR);
            } catch (final UserNotFoundException e) {
                event.failed();
                logger.error("User not found: {}?", principal);
                throw new EzGroupOperationException(
                        "User not found creating app group for user: " + principal + "the app " +
                                "group will need to be created manually", OperationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.arg("Creating user does have permission to create groups", true);
                event.failed();
                logger.error("User does not have permission to create groups");
                throw new AuthorizationException(
                        "User does not have permissions for creating groups. This usually "
                                + "means that an app group cannot be created", AuthorizationError.ACCESS_DENIED);
            } catch (final IndexUnavailableException e) {
                event.arg("Failed setting group index", true);
                event.failed();
                logger.error("Cannot get an index for the user");
                throw new EzGroupOperationException(
                        "Unable to create users at this time. No ID available", OperationError.INDEX_UNAVAILABLE);
            } finally {
                auditLogger.logEvent(event);
            }

            return userId;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void modifyUser(EzSecurityToken ezSecurityToken, String principal, String newPrincipal)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "modifyUser request. Initiator: {}, Old Id: {}, New Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), principal, newPrincipal);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("change user principal", principal).arg("to new principal", newPrincipal);
            try {
                modifyUserOfType(BaseVertex.VertexType.USER, principal, newPrincipal, null);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void deactivateUser(EzSecurityToken ezSecurityToken, String principal)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "deactivateUser request. Initiator: {}, User Id: {}", ezSecurityToken.getValidity().getIssuedTo(),
                    principal);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("deactivate user", principal);
            try {
                deactivateUserOfType(BaseVertex.VertexType.USER, principal);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void activateUser(EzSecurityToken ezSecurityToken, String principal)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "activateUser request. Initiator: {}, User Id: {}", ezSecurityToken.getValidity().getIssuedTo(),
                    principal);

            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken).arg("activate user", principal);
            try {
                activateUserOfType(BaseVertex.VertexType.USER, principal);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void deleteUser(EzSecurityToken ezSecurityToken, String principal)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "deleteUser request. Initiator: {}, User Id: {}", ezSecurityToken.getValidity().getIssuedTo(),
                    principal);

            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken).arg("delete user", principal);
            try {
                deleteUserOfType(BaseVertex.VertexType.USER, principal);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public long createAppUser(EzSecurityToken ezSecurityToken, String securityID, String name)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "createAppUser request. Initiator: {}, Security Id: {}, App Name: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityID, name);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken)
                    .arg("Create app user id", securityID).arg("app name", name);
            try {
                // Create the app user. root.app.name and root.appaccess.name will be created
                final User appUser = graph.addUser(BaseVertex.VertexType.APP_USER, securityID, name);

                final String appGroup = nameHelper.getNamespacedAppGroup(name, null);

                // Also create root.app.name.ezbAudits, root.app.name.ezbMetrics, root.app.name.ezbDiagnostics
                graph.addGroup(
                        BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.AUDIT_GROUP, appGroup,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        new UserGroupPermissionsWrapper(false, true, true, true, true), true, false);
                graph.addGroup(
                        BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.METRICS_GROUP, appGroup,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        UserGroupPermissionsWrapper.ownerPermissions(), true, false);
                graph.addGroup(
                        BaseVertex.VertexType.APP_USER, securityID, EzGroupsConstants.DIAGNOSTICS_GROUP, appGroup,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        UserGroupPermissionsWrapper.ownerPermissions(), true, false);

                event.arg("Assigned App Group Index", appUser.getIndex());
                return appUser.getIndex();
            } catch (final VertexExistsException e) {
                event.arg("Already exists", true);
                event.failed();
                logger.error(
                        "Cannot create user, the vertex already exists! user name: {}. Original Exception: {}",
                        securityID, e.getMessage());
                throw new EzGroupOperationException(
                        "User with principal: " + securityID + " already exists", OperationError.USER_EXISTS);
            } catch (final InvalidVertexTypeException e) {
                event.failed();
                logger.error("Invalid vertex type, this should not happen");
                throw new EzGroupOperationException(
                        "Unable to create users at this time", OperationError.UNRECOVERABLE_ERROR);
            } catch (final UserNotFoundException e) {
                event.failed();
                logger.error("User not found? Most likely when creating app group: {}", securityID);
                throw new EzGroupOperationException(
                        "User not found creating app group for user: " + securityID, OperationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.arg("User does not have permission to create groups", true);
                event.failed();
                logger.error("User does not have permission to create groups");
                throw new AuthorizationException(
                        "User does not have permissions for creating groups. This usually "
                                + "means the app group cannot be created", AuthorizationError.ACCESS_DENIED);
            } catch (final VertexNotFoundException e) {
                event.arg("Unable to create groups for app user because parent group was not found", true);
                event.failed();
                logger.error("Unable to create groups for app user because parent group was not found");
                throw new EzGroupOperationException(
                        "Unable to create groups for app user (" + securityID + ") : " +
                                e.getMessage(), OperationError.PARENT_GROUP_NOT_FOUND);
            } catch (final IndexUnavailableException e) {
                event.arg("Failed setting group index", true);
                event.failed();
                logger.error("Cannot get an index for the user");
                throw new EzGroupOperationException(
                        "Unable to create users at this time. No ID available", OperationError.INDEX_UNAVAILABLE);
            } catch (final InvalidGroupNameException e) {
                event.arg("Unable to create app groups with app name", name);
                event.failed();
                throw new EzGroupOperationException(
                        "Unable to create app users with no name", OperationError.UNRECOVERABLE_ERROR);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void modifyAppUser(EzSecurityToken ezSecurityToken, String securityId, String newSecurityID, String newName)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "modifyAppUser request. Initiator: {}, Old Security Id: {}, New Security Id: {}, New Name: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityId, newSecurityID, newName);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("change user principal", securityId).arg("to new principal", newSecurityID)
                    .arg("New name", newName);
            try {
                // If not an admin, must have admin manage access to the app group
                if (!EzSecurityTokenUtils.isEzAdmin(ezSecurityToken)) {
                    // Query for app_user to determine app name
                    final User app;
                    try {
                        app = getUser(BaseVertex.VertexType.APP_USER, securityId);
                    } catch (final UserNotFoundException e) {
                        throw new EzGroupOperationException(
                                "No app found with security id: " + securityId, OperationError.USER_NOT_FOUND);
                    }

                    final String appGroup = nameHelper.getNamespacedAppGroup(app.getName());
                    ensureUserHasAdminGroup(
                            userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                            appGroup, BaseEdge.EdgeType.A_MANAGE, false);
                }
                modifyUserOfType(BaseVertex.VertexType.APP_USER, securityId, newSecurityID, newName);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void deactivateAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "deactivateAppUser request. Initiator: {}, Security Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityId);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("deactivate app user", securityId);
            try {
                deactivateUserOfType(BaseVertex.VertexType.APP_USER, securityId);
            } catch (final EzGroupOperationException e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void activateAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken, true);
            logger.debug(
                    "activateAppUser request. Initiator: {}, Security Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityId);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("activate app user", securityId);
            try {
                activateUserOfType(BaseVertex.VertexType.APP_USER, securityId);
            } catch (final EzGroupOperationException e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void deleteAppUser(EzSecurityToken ezSecurityToken, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "deleteAppUser request. Initiator: {}, Security Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityId);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                    .arg("delete app user", securityId);
            try {
                // First rename the app user, which will rename all of the app groups that were created for it
                modifyAppUser(ezSecurityToken, securityId, securityId, "_DELETED_APP_" + securityId);

                // Now delete the user
                deleteUserOfType(BaseVertex.VertexType.APP_USER, securityId);
            } catch (final Exception e) {
                event.arg("error", e.getMessage());
                event.failed();
                throw e;
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
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
    public AllGroupMembers getGroupMembers(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getGroupMembers request. Initiator: {}, groupName: {}, onlyExplicitMembers: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, explicitMembers);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            return getGroupMembers(
                    userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                    realGroupName, true, true, explicitMembers);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
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
    public Set<String> getGroupApps(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getGroupApps request. Initiator: {}, groupName: {}, onlyExplicitMembers: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, explicitMembers);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            return getGroupMembers(
                    userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                    realGroupName, false, true, explicitMembers).getApps();
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
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
    public Set<String> getGroupUsers(EzSecurityToken ezSecurityToken, String groupName, boolean explicitMembers)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getGroupUsers request. Initiator: {}, groupName: {}, onlyExplicitMembers: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, explicitMembers);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            return getGroupMembers(
                    userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                    realGroupName, true, false, explicitMembers).getUsers();
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public Set<Long> getAuthorizations(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);

            return graph.getAuthorizations(
                    userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                    requestChainFromToken(ezSecurityToken));
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Returns all groups an user will have data access to when making a request with the given token. The app the token
     * was issued to is taken into consideration when determining the returned groups.
     *
     * @param token security token used to determine the groups to which an user has data access when making requests
     * through the app this token was issued to.
     * @param explicitGroups currently not implemented, this flag would restrict the return values to only those groups
     * to which the user has direct data access
     * @return a set of UserGroups containing information, including ID and name, of the groups to which an user has
     * data access
     * @throws EzSecurityTokenException if EzGroups is unable to validate the token
     * @throws AuthorizationException may be thrown if authorization is denied to the user for this operation
     * @throws GroupQueryException may be thrown if there is problem executing the query
     */
    @Override
    public Set<UserGroup> getUserGroups(EzSecurityToken token, boolean explicitGroups)
            throws GroupQueryException, EzSecurityTokenException {
        try {
            logger.debug(
                    "getUserGroups request. Initiator: {}, User: {}", token.getValidity().getIssuedTo(),
                    token.getTokenPrincipal().getPrincipal());

            validateToken(token);
            final String userId = token.getTokenPrincipal().getPrincipal();
            final String requestApp = token.getValidity().getIssuedTo();

            // First get all user and apps groups
            final Set<Group> userGroups = getUserGroups(BaseVertex.VertexType.USER, userId, true);
            final Set<Group> appGroups = getUserGroups(BaseVertex.VertexType.APP_USER, requestApp, true);

            // Split groups into 2 sets - those that users always have (even if app doesn't) and those that users
            // only have if app has too
            final Set<Group> groupsUserHasRegardless = Sets.newHashSet();
            Set<Group> groupsDependingOnApp = Sets.newHashSet();
            for (final Group g : userGroups) {
                if (g.isRequireOnlyUser()) {
                    groupsUserHasRegardless.add(g);
                } else {
                    groupsDependingOnApp.add(g);
                }
            }

            // Get app groups that go in regardless
            final Set<Group> appAlwaysGetsGroups = Sets.newHashSet();
            for (final Group g : appGroups) {
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
            final Set<UserGroup> ugs = getUserGroupsInfo(userId, groups);

            graph.commitTransaction();

            return ugs;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Gets permissions on a set of groups for a particular user and packages them in an UserGroup object with the
     * corresponding group.
     *
     * @param userId ID of the user to get group permissions for
     * @param groups groups for which to get user permissions
     * @return a set of UserGroup; UserGroup includes a group packaged with an users permissions on that group.
     */
    private Set<UserGroup> getUserGroupsInfo(String userId, Set<Group> groups) {
        final Set<UserGroup> ugs = new HashSet<>();
        for (final Group g : groups) {
            GroupInheritancePermissions inheritancePermissions = null;

            try {
                inheritancePermissions = graph.getGroupInheritancePermissions(g.getGroupName());
            } catch (final VertexNotFoundException e) {
                logger.error(g.getGroupName(), e);
            }

            final ezbake.groups.thrift.Group group = internalGroupToThriftGroup(g, inheritancePermissions);

            Set<BaseEdge.EdgeType> edges = null;

            try {
                edges = graph.userPermissionsOnGroup(BaseVertex.VertexType.USER, userId, g.getGroupName());
            } catch (UserNotFoundException | VertexNotFoundException e) {
                // ignore. We just won't have permissions
            }
            final UserGroupPermissions ugp = convertEdgesToUserGroupPermissions(edges);

            ugs.add(new UserGroup(group, ugp));
        }

        return ugs;
    }

    /**
     * Takes a set of edges and converts their labels to an UserGroupPermissions object. Certain edge labels correspond
     * to certain group permissions. If those labels are detected in the set of edges then the field for that permission
     * on the returned UserGroupPermissions object will be set to true. It is expected that this set of edges will be
     * built from the set of edges from {@link GroupsGraph#userPermissionsOnGroup (BaseVertex.VertexType, String,
     * String)}.
     *
     * @param edges Set of edges to convert an an UserGroupPermissions object
     * @return an UserGroupsPermissions object built from the set of given edges
     */
    private static UserGroupPermissions convertEdgesToUserGroupPermissions(Set<BaseEdge.EdgeType> edges) {
        final UserGroupPermissions userGroupPermissions = new UserGroupPermissions();
        for (final BaseEdge.EdgeType edge : edges) {
            switch (edge) {
                case DATA_ACCESS:
                    userGroupPermissions.setDataAccess(true);
                    break;
                case A_CREATE_CHILD:
                    userGroupPermissions.setAdminCreateChild(true);
                    break;
                case A_MANAGE:
                    userGroupPermissions.setAdminManage(true);
                    break;
                case A_READ:
                    userGroupPermissions.setAdminRead(true);
                    break;
                case A_WRITE:
                    userGroupPermissions.setAdminWrite(true);
                    break;
            }
        }

        return userGroupPermissions;
    }

    /**
     * Request groups to which an user has data access.  Requester must be an EzBake Admin and all groups to which an
     * user has data access will be returned.
     *
     * @param token EzSecurityToken user to determine if requester is authorized to perform this action
     * @param userGroupsRequest information needed to carry about the request for the users groups
     * @return a set of UserGroups containing information, including ID and name, of the groups to which an user has
     * data access
     * @throws EzSecurityTokenException thrown if EzGroups is unable to validate the token
     * @throws AuthorizationException thrown if the requester is not an EzBake Admin or authorization is denied
     * @throws GroupQueryException may be thrown if there is problem executing the query
     */
    @Override
    public Set<UserGroup> requestUserGroups(EzSecurityToken token, UserGroupsRequest userGroupsRequest)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException, TException {
        try {
            validateToken(token);

            if (!EzSecurityTokenUtils.isEzAdmin(token)) {
                final String errMsg = String.format(
                        "User '%s' not an EzBake Admin. Must be EzBake Admin to request groups for an user!",
                        token.getTokenPrincipal().getPrincipal());

                logger.error(errMsg);
                throw new AuthorizationException(errMsg, AuthorizationError.ACCESS_DENIED);
            }

            final String id = userGroupsRequest.getIdentifier();
            final Set<Group> groupsUserBelongsTo = getUserGroups(BaseVertex.VertexType.USER, id, true);

            // root group should not be returned to the user
            Iterables.removeIf(groupsUserBelongsTo, IS_ROOT_GROUP);

            final Set<UserGroup> groups = getUserGroupsInfo(id, groupsUserBelongsTo);

            graph.commitTransaction();

            return groups;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is a diagnostic user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has diagnostic access to
     */
    @Override
    public Set<String> getUserDiagnosticApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getUserDiagnosticApps request. Initiator: {}, User: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), ezSecurityToken.getTokenPrincipal().getPrincipal());

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            return querySpecialGroupForApps(
                    userTypeFromToken(ezSecurityToken), principal, EzGroupsConstants.DIAGNOSTICS_GROUP);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is a metrics user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has metrics access to
     */
    @Override
    public Set<String> getUserMetricsApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getUserMetricsApps request. Initiator: {}, User: {}", ezSecurityToken.getValidity().getIssuedTo(),
                    ezSecurityToken.getTokenPrincipal().getPrincipal());

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            return querySpecialGroupForApps(
                    userTypeFromToken(ezSecurityToken), principal, EzGroupsConstants.METRICS_GROUP);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Get the app names for which the user given in the EzSecurity token is an audit user
     *
     * @param ezSecurityToken a USER token issued to the user of interest
     * @return a set of the application names the user has auditor access to
     */
    @Override
    public Set<String> getUserAuditorApps(EzSecurityToken ezSecurityToken)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "getUserAuditorApps request. Initiator: {}, User: {}", ezSecurityToken.getValidity().getIssuedTo(),
                    ezSecurityToken.getTokenPrincipal().getPrincipal());

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            return querySpecialGroupForApps(
                    userTypeFromToken(ezSecurityToken), principal, EzGroupsConstants.AUDIT_GROUP);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public boolean checkUserAccessToGroup(EzSecurityToken ezSecurityToken, String groupName)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "checkUserAccsesToGroup request. Initiator: {}, User: {}, Group: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), ezSecurityToken.getTokenPrincipal().getPrincipal(),
                    groupName);

            final EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(ezSecurityToken);
            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            // First get the user and app user vertex
            final User user;
            final User appUser;
            try {
                user = graph.getUser(
                        userTypeFromToken(ezSecurityToken), ezSecurityToken.getTokenPrincipal().getPrincipal());
                appUser = graph.getUser(BaseVertex.VertexType.APP_USER, wrapper.getSecurityId());
            } catch (InvalidVertexTypeException | UserNotFoundException e) {
                logger.info("Unable to get user: {}", e.getMessage());
                graph.commitTransaction(true);
                throw new GroupQueryException("Unable to get user: " + e.getMessage(), GroupQueryError.USER_NOT_FOUND);
            }

            // Now get the group and check both accesses
            final Group group;
            try {
                group = graph.getGroup(realGroupName);
            } catch (final VertexNotFoundException e) {
                logger.info("Group does not exist to check access: {}", groupName);
                graph.commitTransaction(true);
                throw new GroupQueryException("Group not found: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
            }

            // Determine user access. User must have access, and if not require only user check app,
            // give unconditionally
            // if require only app
            boolean userAccess =
                    graph.pathExists(user.asVertex(), group.asVertex(), BaseEdge.EdgeType.DATA_ACCESS.toString());
            if (!group.isRequireOnlyUser() || group.isRequireOnlyApp()) {
                final boolean appAccess = graph.pathExists(
                        appUser.asVertex(), group.asVertex(), BaseEdge.EdgeType.DATA_ACCESS.toString());
                if (group.isRequireOnlyApp()) {
                    userAccess = appAccess;
                } else if (!group.isRequireOnlyUser()) {
                    userAccess = userAccess && appAccess;
                }
            }

            graph.commitTransaction();
            return userAccess;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Return a set of UserGroups, which include group information and the user's group permissions
     *
     * @param token a token granting access to EzGroups
     * @param explicitGroups whether or not to fetch all user groups, or just the ones they have direct membership to
     * @return a set of all the apps groups
     */
    @Override
    public Set<UserGroup> getAppUserGroups(EzSecurityToken token, boolean explicitGroups)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(token);
            logger.debug(
                    "getAppUserGroups request. Initiator: {}, App User: {}, onlyExplicitGroups: {}",
                    token.getValidity().getIssuedTo(), token.getTokenPrincipal().getPrincipal(), explicitGroups);

            final String securityId = token.getTokenPrincipal().getPrincipal();

            final Set<UserGroup> ugs = new HashSet<>();
            final Set<Group> groups = getUserGroups(BaseVertex.VertexType.APP_USER, securityId, true);
            for (final Group g : groups) {
                final ezbake.groups.thrift.Group group = new ezbake.groups.thrift.Group(g.getIndex(), g.getGroupName());
                try {
                    group.setInheritancePermissions(graph.getGroupInheritancePermissions(g.getGroupName()));
                } catch (final VertexNotFoundException e) {
                    logger.error(g.getGroupName(), e);
                }
                group.setRequireOnlyUser(g.isRequireOnlyUser());
                group.setRequireOnlyAPP(g.isRequireOnlyApp());
                group.setGroupName(nameHelper.removeRootGroupPrefix(g.getGroupName()));
                // Figure out what the users permissions are
                final UserGroupPermissions ugp = new UserGroupPermissions();
                try {
                    final Set<BaseEdge.EdgeType> edges =
                            graph.userPermissionsOnGroup(BaseVertex.VertexType.APP_USER, securityId, g.getGroupName());
                    for (final BaseEdge.EdgeType edge : edges) {
                        switch (edge) {
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
                        }
                    }
                } catch (UserNotFoundException | VertexNotFoundException e) {
                    // ignore. We just won't have permissions
                }

                ugs.add(new UserGroup(group, ugp));
            }

            graph.commitTransaction();
            return ugs;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
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
    public Set<ezbake.groups.thrift.Group> getChildGroups(EzSecurityToken ezSecurityToken, String groupName,
            boolean recurse) throws EzSecurityTokenException, GroupQueryException, AuthorizationException {
        try {
            logger.debug(
                    "getChildGroups request. Initiator: {}, Parent Group: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName);

            validateToken(ezSecurityToken);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);
            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            final Set<ezbake.groups.thrift.Group> groupNames = Sets.newHashSet();
            try {
                final Set<Group> groups =
                        graph.getGroupChildren(userTypeFromToken(ezSecurityToken), principal, realGroupName, recurse);

                for (final Group g : groups) {
                    final ezbake.groups.thrift.Group group =
                            new ezbake.groups.thrift.Group(g.getIndex(), g.getGroupName());
                    try {
                        group.setInheritancePermissions(graph.getGroupInheritancePermissions(g.getGroupName()));
                    } catch (final VertexNotFoundException e) {
                        logger.error(g.getGroupName(), e);
                    }
                    group.setRequireOnlyUser(g.isRequireOnlyUser());
                    group.setRequireOnlyAPP(g.isRequireOnlyApp());
                    group.setIsActive(g.isActive());
                    group.setGroupName(nameHelper.removeRootGroupPrefix(g.getGroupName()));
                    groupNames.add(group);
                }
                graph.commitTransaction();
            } catch (final VertexNotFoundException e) {
                logger.error("No group found getting children of {}", groupName);
                throw new GroupQueryException("No group found for name: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                logger.error("No user found getting children of {}, user id: {}", groupName, principal);
                throw new GroupQueryException("No user found for id: " + principal, GroupQueryError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                logger.error("User did not have required permissions for accessing group: {}", groupName);
                throw new AuthorizationException(
                        "User does not have access to group: " + groupName, AuthorizationError.ACCESS_DENIED);
            }

            return groupNames;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
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
    public void addAppUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId,
            UserGroupPermissions permissions)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "addAppUserToGroup request. Initiator: {}, Security Id: {}, GroupName: {}, Permissions: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), securityId, groupName, permissions);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);
            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken).arg("add app user", securityId)
                            .arg("to group", groupName).arg("with permissions", permissions);
            try {
                // First need to check the actors permissions on the group
                final boolean privileged = EzSecurityTokenUtils.isEzAdmin(ezSecurityToken) && isPrivilegedPeer(
                        new EzX509(), SecurityID.ReservedSecurityId.INS_REG);
                ensureUserHasAdminGroup(
                        userTypeFromToken(ezSecurityToken), principal, realGroupName, BaseEdge.EdgeType.A_WRITE,
                        privileged);

                graph.addUserToGroup(
                        BaseVertex.VertexType.APP_USER, securityId, realGroupName, permissions.isDataAccess(),
                        permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                        permissions.isAdminCreateChild());
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot add user to {}", groupName);
                throw new EzGroupOperationException(
                        "No group found for name: " + groupName, OperationError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", securityId);
                event.failed();
                logger.error("No user found. Cannot add user {} to {}", securityId, groupName);
                throw new EzGroupOperationException(
                        "No user found, " + securityId + " will not be added to the group",
                        OperationError.USER_NOT_FOUND);
            } catch (final InvalidVertexTypeException e) {
                event.failed();
                logger.error("Invalid vertex type, this should not happen");
                throw new EzGroupOperationException(
                        "Unable to add users to groups at this time", OperationError.UNRECOVERABLE_ERROR);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
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
    public void addUserToGroup(EzSecurityToken ezSecurityToken, String groupName, String userId,
            UserGroupPermissions permissions)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "addUserToGroup request. token: {}, group: {}, user: {}, permissions: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, userId, permissions);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);
            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken).arg("add user", userId)
                            .arg("to group", groupName).arg("with permissions", permissions);
            try {
                // First need to check the actors permissions on the group
                final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();

                final boolean privileged = EzSecurityTokenUtils.isEzAdmin(ezSecurityToken) || isPrivilegedPeer(
                        new EzX509(), SecurityID.ReservedSecurityId.INS_REG);
                ensureUserHasAdminGroup(
                        userTypeFromToken(ezSecurityToken), principal, realGroupName, BaseEdge.EdgeType.A_WRITE,
                        privileged);

                graph.addUserToGroup(
                        BaseVertex.VertexType.USER, userId, realGroupName, permissions.isDataAccess(),
                        permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                        permissions.isAdminCreateChild());
            } catch (final VertexNotFoundException e) {
                event.arg("Group not found", groupName);
                event.failed();
                logger.error("No group found. Cannot add user to {}", groupName);
                throw new EzGroupOperationException(
                        "No group found for name: " + groupName, OperationError.GROUP_NOT_FOUND);
            } catch (final InvalidVertexTypeException e) {
                event.arg("Invalid vertex type", userTypeFromToken(ezSecurityToken));
                event.failed();
                logger.error("Invalid vertex type, this should not happen");
                throw new EzGroupOperationException(
                        "Unable to add users to groupsat this time", OperationError.UNRECOVERABLE_ERROR);
            } catch (final UserNotFoundException e) {
                // Create the user now, and then add them
                logger.info("User being added does not exist. Creating the user, and then reattempting to add");
                final AuditEvent addUserEvent =
                        new AuditEvent(AuditEventType.UserGroupMgmtAdd, ezSecurityToken).arg("Adding user", userId);
                try {
                    graph.addUser(BaseVertex.VertexType.USER, userId, "");

                    graph.addUserToGroup(
                            BaseVertex.VertexType.USER, userId, realGroupName, permissions.isDataAccess(),
                            permissions.isAdminRead(), permissions.isAdminWrite(), permissions.isAdminManage(),
                            permissions.isAdminCreateChild());
                } catch (final AccessDeniedException e1) {
                    addUserEvent.failed();
                    logger.error(
                            "Access denied creating user. This should not have happened because it only happens for "
                                    + "APP_USERS, not USERS");
                    throw new EzGroupOperationException(
                            "Unable to create user: " + userId + " reason: " + e1.getMessage(),
                            OperationError.UNRECOVERABLE_ERROR);
                } catch (InvalidVertexTypeException | UserNotFoundException | VertexExistsException |
                        InvalidGroupNameException e1) {
                    addUserEvent.arg("Unable to create user", e1.getMessage());
                    addUserEvent.failed();
                    logger.error("Unable to create a user: {}", e1.getMessage());
                    throw new EzGroupOperationException(
                            "Unable to create user: " + userId + " reason: " + e1.getMessage(),
                            OperationError.UNRECOVERABLE_ERROR);
                } catch (final IndexUnavailableException e1) {
                    addUserEvent.arg("Failed setting group index", true);
                    addUserEvent.failed();
                    logger.error("Cannot get an index for the user");
                    throw new EzGroupOperationException(
                            "Unable to create users at this time. No ID available", OperationError.INDEX_UNAVAILABLE);
                } catch (final VertexNotFoundException e1) {
                    addUserEvent.arg("Group not found", groupName);
                    addUserEvent.failed();
                    logger.error("No group found. Cannot add user to {}", groupName);
                    throw new EzGroupOperationException(
                            "No group found for name: " + groupName, OperationError.GROUP_NOT_FOUND);
                } finally {
                    auditLogger.logEvent(addUserEvent);
                }
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void deactivateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "deactivateGroup request. Initiator: {}, Group: {}, Recursive: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, andChildren);

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("Deactivate group", groupName).arg("Deactivate child groups, too", andChildren);
            try {
                graph.setGroupActiveOrNot(
                        userTypeFromToken(ezSecurityToken), principal, nameHelper.addRootGroupPrefix(groupName), false,
                        andChildren);
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot deactivate group {}", groupName);
                throw new EzGroupOperationException(
                        "Unable to deactivate group:" + groupName, OperationError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", groupName);
                event.failed();
                logger.error("User not found. Cannot deactivate group {}", groupName);
                throw new EzGroupOperationException(
                        "Unable to deactivate group:" + groupName, OperationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.arg("User does not have permission to deactivate", groupName);
                event.failed();
                logger.error("User does not have admin manage permissions on {}", groupName);
                throw new AuthorizationException(
                        "User does not have required permissions on group: " + groupName,
                        AuthorizationError.ACCESS_DENIED);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void activateGroup(EzSecurityToken ezSecurityToken, String groupName, boolean andChildren)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "activateGroup request. Initiator: {}, Group: {}, Recursive: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, andChildren);

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("Activate group", groupName);
            try {
                graph.setGroupActiveOrNot(
                        userTypeFromToken(ezSecurityToken), principal, nameHelper.addRootGroupPrefix(groupName), true,
                        andChildren);
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot activate group {}", groupName);
                throw new EzGroupOperationException(
                        "Unable to activate group:" + groupName, OperationError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", groupName);
                event.failed();
                logger.error("User not found. Cannot activate group {}", groupName);
                throw new EzGroupOperationException(
                        "Unable to activate group:" + groupName, OperationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.arg("User does not have permission to activate", groupName);
                event.failed();
                logger.error("User does not have admin manage permissions on {}", groupName);
                throw new AuthorizationException(
                        "User does not have required permissions on group: " + groupName,
                        AuthorizationError.ACCESS_DENIED);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void removeAppUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String securityId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "removeAppUserFromGroup request. Initiator: {}, Group: {}, Security Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, securityId);
            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken)
                    .arg("remove app user", securityId).arg("from group", groupName);
            try {
                // First need to check the actors permissions on the group
                final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
                final BaseVertex.VertexType ownerType = vertexTypeFromTokenType(ezSecurityToken.getType());

                if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName)
                        .contains(BaseEdge.EdgeType.A_WRITE)) {
                    event.arg("permissions missing", BaseEdge.EdgeType.A_WRITE.toString());
                    event.failed();
                    logger.error(
                            "Bad access. User: {} tried removing {} to group {}, "
                                    + "but is lacking admin manage permissions", principal, securityId, realGroupName);

                    throw new AuthorizationException(
                            "This user cannot add other users to the group", AuthorizationError.ACCESS_DENIED);
                }

                graph.removeUserFromGroup(BaseVertex.VertexType.APP_USER, securityId, realGroupName);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", securityId);
                event.failed();
                logger.error("No user found. Cannot remove user {} from {}", securityId, realGroupName);
                throw new EzGroupOperationException(
                        "No app user found for id: " + securityId, OperationError.USER_NOT_FOUND);
            } catch (final InvalidVertexTypeException e) {
                event.failed();
                logger.error(
                        "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                                + "BaseVertex.VertexType.APP_USER");
                throw new EzGroupOperationException(
                        "Unable to modify users at this time", OperationError.UNRECOVERABLE_ERROR);
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot remove user {} from {}", securityId, groupName);
                throw new EzGroupOperationException(
                        "No group found for name: " + groupName, OperationError.GROUP_NOT_FOUND);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Gets groups matching the parameters in the given GroupsRequest or all groups if no parameters are given.
     *
     * @param token EzSecurityToken used to determine the returned groups. Currently all groups can be returned to an
     * EzBake Admin, but an AuthorizationException will be thrown for any other requester.
     * @param requestInfo criteria for the returned results - currently unused
     * @return the requested groups
     * @throws EzSecurityTokenException if EzGroups is unable to validate the token
     * @throws AuthorizationException may be thrown if authorization is denied to the user for this operation
     * @throws GroupQueryException may be thrown if there is problem executing the query
     */
    @Override
    public GroupsRequestResponse getGroups(EzSecurityToken token, GroupsRequest requestInfo)
            throws EzSecurityTokenException, AuthorizationException, GroupQueryException {
        try {
            validateToken(token);

            final GroupsRequestResponse response = new GroupsRequestResponse();
            response.setRetrievedGroups(internalGroupsToThriftGroups(graph.getGroups()));

            return response;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void removeUserFromGroup(EzSecurityToken ezSecurityToken, String groupName, String userId)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "removeUserFromGroup request. Initiator: {}, Group: {}, User Id: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, userId);
            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            final AuditEvent event =
                    new AuditEvent(AuditEventType.UserGroupMgmtDelete, ezSecurityToken).arg("remove user", userId)
                            .arg("from group", groupName);
            try {
                // First need to check the actors permissions on the group
                final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
                final BaseVertex.VertexType ownerType = vertexTypeFromTokenType(ezSecurityToken.getType());

                if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName)
                        .contains(BaseEdge.EdgeType.A_WRITE)) {
                    event.arg("permissions missing", BaseEdge.EdgeType.A_WRITE.toString());
                    event.failed();
                    logger.error(
                            "Bad access. User: {} tried removing {} from group {}, "
                                    + "but is lacking admin manage permissions", principal, userId, realGroupName);

                    throw new AuthorizationException(
                            "This user cannot add other users to the group", AuthorizationError.ACCESS_DENIED);
                }

                graph.removeUserFromGroup(BaseVertex.VertexType.USER, userId, realGroupName);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", userId);
                event.failed();
                logger.error("No user found. Cannot remove user {} from {}", userId, realGroupName);
                throw new EzGroupOperationException("No user found for id: " + userId, OperationError.USER_NOT_FOUND);
            } catch (final InvalidVertexTypeException e) {
                event.failed();
                logger.error(
                        "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                                + "BaseVertex.VertexType.APP_USER");

                throw new EzGroupOperationException(
                        "Unable to modify users at this time", OperationError.UNRECOVERABLE_ERROR);
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot remove user {} from {}", userId, groupName);
                throw new EzGroupOperationException(
                        "No group found for name: " + groupName, OperationError.GROUP_NOT_FOUND);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void changeGroupInheritance(EzSecurityToken ezSecurityToken, String groupName,
            GroupInheritancePermissions inheritance)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "changeGroupInheritance request. Initiator: {}, Group: {}, New inheritance: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), groupName, inheritance);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);

            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtGroupRoleModify, ezSecurityToken)
                    .arg("Group name", groupName).arg("Set Group Ineritance", inheritance);
            try {
                // First need to check the actors permissions on the group
                final BaseVertex.VertexType ownerType = vertexTypeFromTokenType(ezSecurityToken.getType());

                if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName)
                        .contains(BaseEdge.EdgeType.A_MANAGE)) {
                    event.arg("permissions missing", BaseEdge.EdgeType.A_MANAGE.toString());
                    event.failed();
                    logger.error(
                            "Bad access. User: {} tried removing {} from group {}, "
                                    + "but is lacking admin manage permissions", principal, principal, realGroupName);

                    throw new AuthorizationException(
                            "This user cannot add other users to the group", AuthorizationError.ACCESS_DENIED);
                }

                graph.setGroupInheritance(
                        realGroupName, inheritance.isDataAccess(), inheritance.isAdminRead(),
                        inheritance.isAdminWrite(), inheritance.isAdminManage(), inheritance.isAdminCreateChild());
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", groupName);
                event.failed();
                logger.error("No group found. Cannot manage inheritance! group {}", groupName);
                throw new EzGroupOperationException(
                        "Unable to manage group inheritance for " + groupName, OperationError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", principal);
                event.failed();
                logger.error("No user found: {}", principal);
                throw new EzGroupOperationException("No user found: " + principal, OperationError.USER_NOT_FOUND);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public void changeGroupName(EzSecurityToken ezSecurityToken, String oldFullyQualifiedName, String newFriendlyName)
            throws EzSecurityTokenException, EzGroupOperationException, AuthorizationException {
        try {
            validateToken(ezSecurityToken);
            logger.debug(
                    "changeGroupName request. Initiator: {}, Old Name: {}, New Name: {}",
                    ezSecurityToken.getValidity().getIssuedTo(), oldFullyQualifiedName, newFriendlyName);

            final String realGroupName = nameHelper.addRootGroupPrefix(oldFullyQualifiedName);
            final String principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
            final AuditEvent event = new AuditEvent(AuditEventType.UserGroupMgmtModify, ezSecurityToken)
                    .arg("Group name", oldFullyQualifiedName).arg("new friendly group name", newFriendlyName);

            final BaseVertex.VertexType ownerType = vertexTypeFromTokenType(ezSecurityToken.getType());

            try {
                // First need to check the actors permissions on the group, fail fast if they don't have access
                if (!graph.userPermissionsOnGroup(ownerType, principal, realGroupName)
                        .contains(BaseEdge.EdgeType.A_MANAGE)) {
                    event.arg("permissions missing", BaseEdge.EdgeType.A_MANAGE.toString());
                    event.failed();
                    final String errMsg = String.format(
                            "Bad access. User: %s tried changing group name for %s, "
                                    + "but is lacking admin manage permissions", principal, realGroupName);

                    logger.error(errMsg);
                    throw new AuthorizationException(errMsg, AuthorizationError.ACCESS_DENIED);
                }

                Map<String, String> changedGroupNames = null;
                try {
                    changedGroupNames = graph.changeGroupName(ownerType, principal, realGroupName, newFriendlyName);
                } catch (final VertexExistsException e) {
                    final String errMsg = String.format(
                            "Unable to update group '%s' with new friendly name '%s', group already exists!",
                            oldFullyQualifiedName, newFriendlyName);

                    logger.error(errMsg, e);
                    throw new EzGroupOperationException(errMsg, OperationError.GROUP_EXISTS);
                }

                event.arg("changed group names", changedGroupNames);
            } catch (final VertexNotFoundException e) {
                event.arg("Group does not exist", realGroupName);
                event.failed();
                logger.error("No group found. Cannot manage inheritance! group {}", realGroupName);
                throw new EzGroupOperationException(
                        "Unable to manage group name for " + realGroupName, OperationError.GROUP_NOT_FOUND);
            } catch (final UserNotFoundException e) {
                event.arg("User does not exist", principal);
                event.failed();
                logger.error("No user found: {}", principal, e);
                throw new EzGroupOperationException("No user found: " + principal, OperationError.USER_NOT_FOUND);
            } catch (final AccessDeniedException e) {
                event.arg("permissions missing", BaseEdge.EdgeType.A_MANAGE.toString());
                event.failed();
                final String errMsg = String.format(
                        "Bad access. User: %s tried changing group %s, but is lacking admin manage permissions to one" +
                                " or " + "more of its child group(s).", principal, realGroupName);

                logger.error(errMsg, e);
                throw new AuthorizationException(errMsg, AuthorizationError.ACCESS_DENIED);
            } finally {
                auditLogger.logEvent(event);
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public Set<Long> getGroupsMask(EzSecurityToken ezSecurityToken, Set<String> groupNames, Set<String> ezbakeUserIds,
            Set<String> ezbakeAppIds) throws EzSecurityTokenException, EzGroupOperationException {
        try {
            validateToken(ezSecurityToken);

            // Get all the groups
            groupNames = nameHelper.addRootGroupPrefix(groupNames.toArray(new String[groupNames.size()]));
            final Set<Group> groups = graph.getGroups(groupNames);

            final Set<User> users = Sets.newHashSet();

            if (ezbakeUserIds != null) {
                users.addAll(graph.getUsers(BaseVertex.VertexType.USER, ezbakeUserIds));
            }

            if (ezbakeAppIds != null) {
                users.addAll(graph.getUsers(BaseVertex.VertexType.APP_USER, ezbakeAppIds));
            }

            final Set<Long> mask = Sets.newTreeSet();
            for (final Group group : groups) {
                final Long index = group.getIndex();
                if (index != null) {
                    mask.add(index);
                }
            }

            for (final User user : users) {
                final Long index = user.getIndex();
                if (index != null) {
                    mask.add(index);
                }
            }

            graph.commitTransaction();

            return mask;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public Set<Long> createUserAndGetAuthorizations(EzSecurityToken token, List<String> chain, String id, String name)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        logger.debug(
                "createUserAndGetAuthorizations - requesting app: {}, request chain: {}, user id: {}, user name: {}",
                token.getValidity().getIssuedTo(), chain, id, name);
        try {
            validatePrivilegedPeer(token, new EzX509());

            try {
                return getUserAuthorizations(token, TokenType.USER, id, chain);
            } catch (final GroupQueryException e) {
                if (e.getErrorType() == GroupQueryError.USER_NOT_FOUND) {
                    logger.info("User did not exist. Attempting to create");
                    createUser(token, id, name);
                    return getUserAuthorizations(token, TokenType.USER, id, chain);
                } else {
                    logger.error("createUserAndGetAuth Failure", e);
                    throw e;
                }
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public Set<Long> createAppUserAndGetAuthorizations(EzSecurityToken token, List<String> chain, String securityId,
            String appName)
            throws EzSecurityTokenException, AuthorizationException, EzGroupOperationException, GroupQueryException {
        try {
            logger.debug(
                    "createAppUserAndGetAuthorizations - requesting app: {}, request chain: {}, security id: {}, "
                            + "app name: {}", token.getValidity().getIssuedTo(), chain, securityId, appName);

            validatePrivilegedPeer(token, new EzX509());
            try {
                return getUserAuthorizations(token, TokenType.APP, securityId, chain);
            } catch (final GroupQueryException e) {
                if (e.getErrorType() == GroupQueryError.USER_NOT_FOUND) {
                    createAppUser(token, securityId, appName);
                    return getUserAuthorizations(token, TokenType.APP, securityId, chain);
                } else {
                    logger.error("createAppUserAndGetAuth Failure", e);
                    throw e;
                }
            }
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Get a set of longs containing all of the group IDs the user is a member of
     *
     * @param token a token granting access to EzGroups
     * @param userType the type of token this will be for, either USER or APP
     * @param userId the user for which groups will be queried
     * @param chain the chain of apps in the request
     * @return a set of longs with a bit set for every group the user is a member of
     */
    @Override
    public Set<Long> getUserAuthorizations(EzSecurityToken token, TokenType userType, String userId, List<String> chain)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validatePrivilegedPeer(token, new EzX509());
            logger.debug(
                    "getUserAuthorizations request. Initiator: {}, UserType: {}, User Id: {}, Request chain: {}",
                    token.getValidity().getIssuedTo(), userType, userId, chain);

            return graph.getAuthorizations(
                    vertexTypeFromTokenType(userType), userId, chain);
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Get user details such as principal and whether or not user is active
     *
     * @param token - authorization token
     * @param userType - user type as defined in UserType enum
     * @param userId - usually DN of a user or security id for app user
     * @return User object
     */
    @Override
    public ezbake.groups.thrift.User getUser(EzSecurityToken token, UserType userType, String userId)
            throws EzSecurityTokenException {
        try {
            validateToken(token);
            logger.debug(
                    "getUser request. Initiator: {}, UserType: {}, User Id: {}", token.getValidity().getIssuedTo(),
                    userType, userId);

            final ezbake.groups.thrift.User result = new ezbake.groups.thrift.User();
            try {
                final User user = graph.getUser(userTypeFromUserType(userType), userId);
                graph.commitTransaction();
                return result.setIsActive(user.isActive()).setName(user.getName()).setPrincipal(user.getPrincipal());
            } catch (InvalidVertexTypeException | UserNotFoundException e) {
                logger.error("Failed to retrieve user {} details", userId);
            }

            return result;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    /**
     * Gets given group metadata, for example whether or not group is active.  EzAdmin will have access to all groups,
     * while regular users will only be able to retrieve group metadata for groups which they have admin read access
     * to.
     *
     * @param token EzSecurityToken belonging to the party performing the query. The user must have admin_read on the
     * group referenced by groupName
     * @param groupName the group name for which metadata is being requested
     * @return group metadata for the requested group
     * @throws EzSecurityTokenException if the token cannot be validated
     */
    @Override
    public ezbake.groups.thrift.Group getGroup(EzSecurityToken token, String groupName)
            throws EzSecurityTokenException {
        try {
            validateToken(token);
            logger.debug(
                    "getGroup request. Initiator: {}, Group name: {}", token.getValidity().getIssuedTo(), groupName);

            final String realGroupName = nameHelper.addRootGroupPrefix(groupName);
            ezbake.groups.thrift.Group result = new ezbake.groups.thrift.Group();
            try {

                Group group = null;
                if (EzSecurityTokenUtils.isEzAdmin(token)) {
                    group = graph.getGroup(realGroupName);
                } else {
                    group = graph.getGroup(
                            vertexTypeFromTokenType(token.getType()), token.getTokenPrincipal().getPrincipal(),
                            realGroupName);
                }
                result = internalGroupToThriftGroup(group, graph.getGroupInheritancePermissions(group.getGroupName()));
                graph.commitTransaction();
            } catch (final Exception e) {
                logger.error("Failed to get group {}. Failed with exception: {}", realGroupName, e.getMessage());
            }

            return result;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    @Override
    public Map<Long, String> getGroupNamesByIndices(EzSecurityToken token, Set<Long> groupIndices)
            throws EzSecurityTokenException, GroupQueryException {
        try {
            validateToken(token);
            logger.debug(
                    "getGroupNamesByIndices request. Initiator: {}, User: {}, IDs: {}",
                    token.getValidity().getIssuedTo(), token.getTokenPrincipal().getPrincipal(), groupIndices);

            final EzSecurityTokenWrapper wrapper = new EzSecurityTokenWrapper(token);
            final String userId = token.getTokenPrincipal().getPrincipal();
            final String appId = token.getValidity().getIssuedTo();

            Set<Group> groupsToReturn = null;
            Map<Long, String> groupNames = null;

            try {
                if (wrapper.isEzAdmin()) {
                    groupsToReturn = graph.getGroupsByIds(groupIndices);
                } else {
                    try {
                        final Set<Group> userHasGroups = graph.getGroupsByIdsWithAuths(
                                userTypeFromToken(token), userId, groupIndices);

                        final Set<Group> appHasGroups =
                                graph.getGroupsByIdsWithAuths(BaseVertex.VertexType.APP_USER, appId, groupIndices);

                        // get the intersection of the available groups to the app and user
                        // a group is returned if and only if both the app and the user have access
                        groupsToReturn = Sets.intersection(userHasGroups, appHasGroups);
                    } catch (final UserNotFoundException e) {
                        final String errMsg = String.format(
                                "Could not find APP_USER (%s) or USER (%s) referenced in token!", userId, appId);

                        logger.error(errMsg, e);
                        throw new GroupQueryException(errMsg, GroupQueryError.USER_NOT_FOUND);
                    } catch (final InvalidVertexTypeException e) {
                        // this should never happen
                        final String errMsg = "Token points to an invalid user type! This should never happen!";
                        logger.error(errMsg, e);
                        throw new GroupQueryException(errMsg, GroupQueryError.USER_NOT_FOUND);
                    }
                }
                groupNames = getUnloadedGroupIndexToNameMap(groupIndices);

                for (final Group group : groupsToReturn) {
                    groupNames.put(group.getIndex(), nameHelper.removeRootGroupPrefix(group.getGroupName()));
                }
            } finally {
                // free up resources; rolling back instead of committing because we *should* only be reading.
                graph.commitTransaction(true);
            }

            return groupNames;
        } catch (TException | RuntimeException e) {
            graph.commitTransaction(true);
            throw e;
        } finally {
            graph.commitTransaction();
        }
    }

    private void ensureUserHasAdminGroup(BaseVertex.VertexType userType, String userId, String groupName,
            BaseEdge.EdgeType adminType, boolean privileged) throws AuthorizationException {
        String err = null;
        try {
            if (!graph.userPermissionsOnGroup(userType, userId, groupName).contains(adminType)) {
                err = userId + " does not have the required permission on: " + nameHelper
                        .removeRootGroupPrefix(groupName);
            }
        } catch (UserNotFoundException | VertexNotFoundException e) {
            err = "user: " + userId + " does not exist. " + e.getMessage();
        }

        if (err != null && !privileged) {
            logger.error("Bad access. User: {} does not have {} on  group {} - {}", userId, adminType, groupName, err);
            throw new AuthorizationException(err, AuthorizationError.ACCESS_DENIED);
        }
    }

    private AllGroupMembers getGroupMembers(BaseVertex.VertexType userType, String userId, String groupName,
            boolean includeUsers, boolean includeApps, boolean explicit) throws GroupQueryException {
        final Set<String> userPrincipals = new HashSet<>();
        final Set<String> appPrincipals = new HashSet<>();
        try {
            final Set<User> users =
                    graph.groupMembers(userType, userId, groupName, includeUsers, includeApps, explicit);
            for (final User user : users) {
                logger.debug("Group: {} has user: {} - {}", groupName, user.getPrincipal(), user.getIndex());
                switch (user.getType()) {
                    case USER:
                        userPrincipals.add(user.getPrincipal());
                        break;
                    case APP_USER:
                        appPrincipals.add(user.getPrincipal());
                        break;
                }
            }
            graph.commitTransaction();
        } catch (final VertexNotFoundException e) {
            logger.error("Group {} not found", groupName);
            throw new GroupQueryException("No group found with name: " + groupName, GroupQueryError.GROUP_NOT_FOUND);
        } catch (final UserNotFoundException e) {
            logger.error("User {}({}) not found", userId, userType);
            throw new GroupQueryException(
                    "No user found: " + userId + " (" + userType + ")", GroupQueryError.USER_NOT_FOUND);
        }

        final AllGroupMembers members = new AllGroupMembers();
        members.setApps(appPrincipals);
        members.setUsers(userPrincipals);
        return members;
    }

    private User getUser(BaseVertex.VertexType type, String principal) throws UserNotFoundException {
        try {
            return graph.getUser(type, principal);
        } catch (final InvalidVertexTypeException e) {
            throw new UserNotFoundException("User was of invalid user type", e);
        }
    }

    /**
     * Get groups that an user is a member of optionally including inactive groups. Group membership is defined as
     * having data access to the group Group membership is represented on the graph by a path of outgoing DATA_ACCESS
     * edges from the user to the group.
     *
     * @param type type of user, USER or APP_USER
     * @param id EzBake ID of the user for which to get the groups they are a member of
     * @param includeInactive if inactive groups should be included in the results
     * @return the set of groups to which an user belongs, optionally including inactive groups
     * @throws GroupQueryException if the user groups are being requested for cannot be found
     */
    private Set<Group> getUserGroups(BaseVertex.VertexType type, String id, boolean includeInactive)
            throws GroupQueryException {
        final Set<Group> gs;
        try {
            // Get the groups from the graph and add them to the set
            gs = graph.userGroups(type, id, false, includeInactive);
        } catch (final UserNotFoundException e) {
            logger.error("Cannot get user groups for {}. User Not found", id);
            throw new GroupQueryException(
                    "User " + id + " not found. Cannot compute group user groups", GroupQueryError.USER_NOT_FOUND);
        }

        return gs;
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
    private void modifyUserOfType(BaseVertex.VertexType type, String id, String newId, String newName)
            throws EzGroupOperationException {
        try {
            graph.updateUser(type, id, newId, newName);
        } catch (final InvalidVertexTypeException e) {
            logger.error(
                    "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                            + "BaseVertex.VertexType.APP_USER");

            throw new EzGroupOperationException(
                    "Unable to modify users at this time", OperationError.UNRECOVERABLE_ERROR);
        } catch (final UserNotFoundException e) {
            logger.error("Cannot update principal from {} -> {}. User Not found", id, newId);
            throw new EzGroupOperationException(
                    "User " + id + " not found. Cannot update principal", OperationError.USER_NOT_FOUND);
        } catch (final VertexNotFoundException e) {
            logger.error(
                    "Cannot update principal from {} -> {} with new name: {}. Group was not found. Message: {} ", id,
                    newId, newName, e.getMessage());

            throw new EzGroupOperationException(
                    "Failed changing id:" + id + " to:" + newId + " with name:" + newName +
                            ". Failed updating group names. " + e.getMessage(), OperationError.USER_NOT_FOUND);
        } catch (final VertexExistsException e) {
            final String errMsg =
                    String.format("Cannot update principal from %s -> %s. User %s already exists", id, newId, newId);

            logger.error(errMsg);
            throw new EzGroupOperationException(errMsg, OperationError.USER_EXISTS);
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
        } catch (final InvalidVertexTypeException e) {
            logger.error(
                    "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                            + "BaseVertex.VertexType.APP_USER");

            throw new EzGroupOperationException(
                    "Unable to activate users at this time. Internal error, invalid " + "user type",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (final UserNotFoundException e) {
            logger.error("Cannot activate user {} ({}). User Not found", id, type);
            throw new EzGroupOperationException(
                    "User " + id + " not found. Cannot activate the user", OperationError.USER_NOT_FOUND);
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
        } catch (final InvalidVertexTypeException e) {
            logger.error(
                    "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                            + "BaseVertex.VertexType.APP_USER");

            throw new EzGroupOperationException(
                    "Unable to deactivate users at this time. Internal error, invalid " + "user type",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (final UserNotFoundException e) {
            logger.error("Cannot deactivate user from {}. User Not found", id);
            throw new EzGroupOperationException(
                    "User " + id + " not found. Cannot deactivate the user", OperationError.USER_NOT_FOUND);
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
        } catch (final InvalidVertexTypeException e) {
            logger.error(
                    "Invalid vertex type. Valid vertex types are BaseVertex.VertexType.USER and "
                            + "BaseVertex.VertexType.APP_USER");

            throw new EzGroupOperationException(
                    "Unable to activate users at this time. Internal error, invalid " + "user type",
                    OperationError.UNRECOVERABLE_ERROR);
        } catch (final UserNotFoundException e) {
            logger.error("Cannot delete user from {}. User Not found", id);
            throw new EzGroupOperationException(
                    "User " + id + " not found. Cannot delete the user", OperationError.USER_NOT_FOUND);
        }
    }

    private Set<String> querySpecialGroupForApps(BaseVertex.VertexType type, String id, String group)
            throws GroupQueryException {
        final Set<String> groups;
        try {
            final User user = graph.getUser(type, id);
            groups = graph.specialAppNamesQuery(group, user.asVertex());
            graph.commitTransaction();
        } catch (UserNotFoundException | InvalidVertexTypeException e) {
            logger.error("No user found id: {}", id, e);
            throw new GroupQueryException("No user found for id: " + id, GroupQueryError.USER_NOT_FOUND);
        } catch (final VertexNotFoundException e) {
            logger.error("App Group not found", e);
            throw new GroupQueryException("Internal Error", GroupQueryError.GROUP_NOT_FOUND);
        }

        return groups;
    }

    /**
     * Converts a set of internal Groups to thrift Groups by requesting permissions for each group and then calling
     * {@link #internalGroupToThriftGroup(ezbake.groups.graph.frames.vertex.Group,
     * ezbake.groups.thrift.GroupInheritancePermissions)}
     * with the group and its corresponding permissions.
     *
     * @param groupsToConvert internal groups to convert to thrift groups
     * @return a set of thrift Groups that are equivalent to the internal groups they were converted from
     * @throws ezbake.groups.thrift.GroupQueryException if a group could not be found in graph
     */
    private Set<ezbake.groups.thrift.Group> internalGroupsToThriftGroups(Set<Group> groupsToConvert)
            throws GroupQueryException {
        final Set<ezbake.groups.thrift.Group> convertedGroups = Sets.newHashSet();

        for (final Group g : groupsToConvert) {
            GroupInheritancePermissions inheritance = null;
            try {
                inheritance = graph.getGroupInheritancePermissions(g.getGroupName());
            } catch (final VertexNotFoundException e) {
                final String errMsg =
                        String.format("Could not get expected groups! Could not find group '%s!'", g.getGroupName());

                logger.error(errMsg, e);
                throw new GroupQueryException(errMsg, GroupQueryError.GROUP_NOT_FOUND);
            }

            convertedGroups.add(internalGroupToThriftGroup(g, inheritance));
        }

        return convertedGroups;
    }

    /**
     * Convert an internal Group to a thrift Group.
     *
     * @param group Group to convert to thrift Group
     * @param inheritance inheritance to assign group
     * @return a thrift Group with fields populated by values in the internal Group
     */
    private static ezbake.groups.thrift.Group internalGroupToThriftGroup(Group group,
            GroupInheritancePermissions inheritance) {
        final ezbake.groups.thrift.Group thriftGroup = new ezbake.groups.thrift.Group(
                group.getIndex(), nameHelper.removeRootGroupPrefix(group.getGroupName()));

        thriftGroup.setInheritancePermissions(inheritance);
        thriftGroup.setIsActive(true);
        thriftGroup.setRequireOnlyUser(group.isRequireOnlyUser());
        thriftGroup.setRequireOnlyAPP(group.isRequireOnlyApp());
        thriftGroup.setFriendlyName(group.getGroupFriendlyName());

        return thriftGroup;
    }

    /**
     * Builds a map using the given set of Long as keys and an error string as values.
     *
     * @param ids IDs to use as keys in the map
     * @return a map with keys from the given set of Long and an error string for values
     */
    public static Map<Long, String> getUnloadedGroupIndexToNameMap(Set<Long> ids) {
        final Map<Long, String> unloadedMap = Maps.newHashMap();
        for (final Long index : ids) {
            unloadedMap.put(index, UNABLE_TO_RETRIEVE_GROUP_NAME);
        }

        return unloadedMap;
    }

    /**
     * Closes this stream and releases any system resources associated with it. If the stream is already closed then
     * invoking this method has no effect.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (graph != null) {
            graph.close();
        }
    }
}
