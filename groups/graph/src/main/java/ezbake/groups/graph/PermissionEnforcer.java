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

 package ezbake.groups.graph;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.exception.AccessDeniedException;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.query.BaseQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

/**
 * PermissionQuery class enforces permissions on EzGroups
 *
 * Permissions on EzGroups are enforced with "admin" edges, these queries can determine whether a user has the requested
 * permission or not.
 */
public class PermissionEnforcer {
    private static final Logger logger = LoggerFactory.getLogger(PermissionEnforcer.class);

    private BaseQuery query;

    @Inject
    public PermissionEnforcer(BaseQuery query) {
        this.query = query;
    }

    /**
     * Definitions:
     *
     * DISCOVER: Able to "discover" that the group exists, but not view anything else
     * READ: View the group, child groups, and group members
     * WRITE: Add/Remove users to/from the group
     * MANAGE: Update group inheritance, change group name
     * CREATE_CHILD: Add new child groups
     */
    public enum Permission {
        DISCOVER(BaseEdge.EdgeType.A_READ),
        READ(BaseEdge.EdgeType.A_READ),
        WRITE(BaseEdge.EdgeType.A_WRITE),
        MANAGE(BaseEdge.EdgeType.A_MANAGE),
        CREATE_CHILD(BaseEdge.EdgeType.A_CREATE_CHILD);

        private BaseEdge.EdgeType grantingEdge;

        Permission(BaseEdge.EdgeType grantingEdge) {
            this.grantingEdge = grantingEdge;
        }

        public BaseEdge.EdgeType getGrantingEdge() {
            return grantingEdge;
        }

        public String getGrantingEdgePropertyName() {
            return grantingEdge.toString();
        }
    }


    /**
     * Validate the user has the required permissions
     *
     * @param requester requester whose auths to check
     * @param group vertex against which an user must have auths
     * @param requiredPermissions array of required edges. Each one will be checked for a path to the requestor
     * @throws ezbake.groups.graph.exception.AccessDeniedException if the user does not have permission to manage all of the groups in the given
     * pipe.
     */
    public void validateAuthorized(User requester, Group group, Permission... requiredPermissions) throws AccessDeniedException {
        validateAuthorized(requester, Lists.newArrayList(group), requiredPermissions);
    }

    /**
     * Validate the user has the required permissions
     *
     * @param requester requester whose auths to check
     * @param group vertex against which an user must have auths
     * @param requiredPermissions array of required edges. Each one will be checked for a path to the requestor
     * @throws ezbake.groups.graph.exception.AccessDeniedException if the user does not have permission to manage all of the groups in the given
     * pipe.
     */
    public void validateAuthorized(User requester, Vertex group, Permission... requiredPermissions) throws AccessDeniedException {
        validateAuthorized(requester, Lists.newArrayList(group), requiredPermissions);
    }

    /**
     * Validate that an User has manage auths on all groups in a given pipe.
     *
     * @param requester requester whose auths to check
     * @param vertices list containing group-vertices against which an user must have auths
     * @param requiredPermissions array of required edges. Each one will be checked for a path to the requestor
     * @throws ezbake.groups.graph.exception.AccessDeniedException if the user does not have permission to manage all of the groups in the given
     * pipe.
     */
    public void validateAuthorized(User requester, List vertices, Permission... requiredPermissions) throws AccessDeniedException {
        for (Object object : vertices) {
            Vertex vertex;
            if (object instanceof Group)  {
                vertex = ((Group) object).asVertex();
            } else if (object instanceof Vertex) {
                vertex = (Vertex) object;
            } else {
                throw new IllegalStateException("Invalid object type passed in list: " + object.getClass());
            }

            for (Permission permission : requiredPermissions) {
                if (!query.pathExists(
                        requester.asVertex().getId(), vertex.getId(), permission.getGrantingEdgePropertyName())) {
                    query.getGraph().getBaseGraph().rollback();
                    final String errMsg = String.format(
                            "requester '%s' does not have the required permission '%s'", requester.getPrincipal(),
                            permission);
                    logger.error(errMsg);
                    throw new AccessDeniedException(errMsg);
                }
            }
        }
    }

    /**
     * Determines if an user has any permission on a group including DATA_ACCESS.
     *
     * @param user user for which to determine if they have any permission against a group
     * @param group group to determine if the given user has any permission on
     * @return true if the user has any permission, false if not
     */
    public boolean hasAnyPermission(User user, Group group) {
        final Set<String> permissionEdgeLabels = Sets.newHashSet();
        for (Permission permission : Permission.values()) {
            permissionEdgeLabels.add(permission.getGrantingEdgePropertyName());
        }
        permissionEdgeLabels.add(BaseEdge.EdgeType.DATA_ACCESS.toString());

        for (String label : permissionEdgeLabels) {
            if (query.pathExists(user.asVertex().getId(), group.asVertex().getId(), label)) {

                return true;
            }
        }

        return false;
    }

}
