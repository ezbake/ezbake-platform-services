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

import com.tinkerpop.blueprints.Vertex;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.groups.thrift.UserGroupPermissions;

import java.io.Closeable;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: jhastings
 * Date: 2/3/15
 * Time: 10:31 AM
 */
public interface GroupsGraph extends Closeable {

    /**************************************************************/
    /******                   UTILITY                      ********/
    /**************************************************************/

    /**
     * Commit a transaction with the graph
     *
     * Transactions must be committed before changes made to the graph in other threads will be realized. If an
     * operation is doing many reads, it may be ideal to wait until the current transaction is fully finished
     * before committing the transaction. For this reason, read operations on this class do not automatically close
     * the transaction, and you must do so by calling this method
     */
    public void commitTransaction();

    /**
     * Transactions must be committed before changes made to the graph in other threads will be realized. If an
     * operation is doing many reads, it may be ideal to wait until the current transaction is fully finished
     * before committing the transaction. For this reason, read operations on this class do not automatically close
     * the transaction, and you must do so by calling this method
     *
     * @param rollback optionally, rollback the transaction instead of committing it
     */
    public void commitTransaction(boolean rollback);

    /**************************************************************/
    /****                       CREATE                         ****/
    /**************************************************************/

    /**
     * Add a new child group to the common group with the specified inheritances
     *
     * @param ownerType type of group owner, app or user
     * @param ownerId id of owner
     * @param groupName name of new group, not including parent group namespace
     * @param parentGroup name of parent group
     * @param inheritance group permissions inherited from parent group
     * @param permissions permissions to grant owner on group
     * @param requireOnlyUser whether or not the user should always be granted access if they are a member
     * @param requireOnlyApp whether or not the user membership should hinge on the app used being a member
     * @return the newly created group
     * @throws VertexExistsException
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     */
    public Group addGroup(BaseVertex.VertexType ownerType, String ownerId, String groupName, String parentGroup, GroupInheritancePermissions inheritance, UserGroupPermissions permissions, boolean requireOnlyUser, boolean requireOnlyApp) throws UserNotFoundException, AccessDeniedException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException;

    public User addUser(BaseVertex.VertexType type, String principal, String name) throws InvalidVertexTypeException, VertexExistsException, AccessDeniedException, UserNotFoundException, IndexUnavailableException, InvalidGroupNameException;

    public void addUserToGroup(BaseVertex.VertexType type, String principal, String groupName) throws VertexNotFoundException, UserNotFoundException, InvalidVertexTypeException;

    public void addUserToGroup(BaseVertex.VertexType type, String principal, String groupName, boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws VertexNotFoundException, UserNotFoundException, InvalidVertexTypeException;

    public void removeUserFromGroup(BaseVertex.VertexType type, String principal, String groupName) throws UserNotFoundException, VertexNotFoundException, InvalidVertexTypeException;


    /** READ **/

    public Group getGroup(BaseVertex.VertexType type, String userId, String groupName) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException, InvalidVertexTypeException;
    public Group getGroup(String groupName) throws VertexNotFoundException;
    public Set<Group> getGroups(Set<String> groupNames);

    /**
     * Gets a set of users by EzBake ID and user type. Users that cannot be found will simply not be included in the
     * results.
     *
     * @param type type of users to get
     * @param ezbakeUserIds IDs for the users to get
     * @return a set of users represented by the given ID
     */
    public Set<User> getUsers(BaseVertex.VertexType type, Set<String> ezbakeUserIds);

    /**
     * Gets all groups except special groups.
     *
     * @return all groups
     */
    public Set<Group> getGroups();

    /**
     * Gets groups by their EzGroups indices.
     *
     * @param indices indices of the groups to get
     * @return groups identified by the given indices
     */
    public Set<Group> getGroupsByIds(Set<Long> indices);

    /**
     * Gets groups by their EzGroups indices. Only returns groups for which the given user has any permission.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal EzBake ID of the user making the request
     * @param indices indices of the groups to get
     * @return groups identified by the given indices
     * @throws InvalidVertexTypeException if the user type is not USER or APP_USER
     * @throws UserNotFoundException if the given type and principal cannot be mapped to an user
     */
    public Set<Group> getGroupsByIdsWithAuths(BaseVertex.VertexType type, String principal, Set<Long> indices)
            throws UserNotFoundException, InvalidVertexTypeException;

    public Set<Group> getGroupChildren(BaseVertex.VertexType userType, String id, String groupName) throws UserNotFoundException, VertexNotFoundException, AccessDeniedException;
    public Set<Group> getGroupChildren(BaseVertex.VertexType userType, String id, String groupName, final boolean recurse) throws UserNotFoundException, VertexNotFoundException, AccessDeniedException;
    public GroupInheritancePermissions getGroupInheritancePermissions(String groupName) throws VertexNotFoundException;
    public Set<User> groupMembers(BaseVertex.VertexType userType, String userId, String groupName, final boolean includeUsers, final boolean includeApps, boolean explicitOnly) throws VertexNotFoundException, UserNotFoundException;
    public Set<String> specialAppNamesQuery(final String specialGroupName, final Vertex user) throws VertexNotFoundException;

    /**
     * Gets an user by its Titan/ EzGroups ID.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param id Titan ID/ vertex ID of the user
     * @return an User identified by the given ID and type
     * @throws InvalidVertexTypeException if the UserType is not USER or APP_USER
     * @throws UserNotFoundException if the user cannot be found
     */
    public User getUser(BaseVertex.VertexType type, String id) throws InvalidVertexTypeException, UserNotFoundException;

    /**
     * Traverse the graph to determine all of the groups an user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param userID EzBake ID of the user for which to get the groups they are a member of
     * @return the set of all groups the user is a member of
     * @throws UserNotFoundException if the user whose groups were requested could not be found
     */
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID) throws UserNotFoundException;

    /**
     * Traverse the graph to determine all of the groups a user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param userID EzBake ID of the user for which to get the groups they are a member of
     * @return the set of all groups the user is a member of
     * @throws UserNotFoundException if the user whose groups were requested could not be found
     */
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID, boolean explicitGroupsOnly)
            throws UserNotFoundException;

    /**
     * Traverse the graph to determine all of the groups a user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param userType type of user, USER or APP_USER
     * @param userID EzBake ID of the user for which to get the groups they are a member of
     * @param explicitGroupsOnly if only groups that have a direct DATA_ACCESS edge to the user should be included in
     * the result
     * @param includeInactive if inactive groups should be included in the result
     * @return the set of all groups the user is a member of, or just the set of groups the user has a direct
     * DATA_ACCESS edge to if {@code explicitGroupsOnly} is true.
     * @throws UserNotFoundException if the user for which groups are requested cannot be found
     */
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID, boolean explicitGroupsOnly,
            boolean includeInactive) throws UserNotFoundException;

    public Set<BaseEdge.EdgeType> userPermissionsOnGroup(BaseVertex.VertexType type, String userPrincipal, String groupName) throws UserNotFoundException, VertexNotFoundException;

    public boolean pathExists(Vertex source, final Vertex target, String... edgeLabel);

    public Set<Long> getAuthorizations(BaseVertex.VertexType userType, String userId, List<String> appFilter) throws GroupQueryException;

    /** UPDATE **/
    public void setGroupInheritance(String groupName, boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws VertexNotFoundException;
    public void setGroupActiveOrNot(BaseVertex.VertexType type, String userId, final String groupName, boolean active) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException;
    public void setGroupActiveOrNot(BaseVertex.VertexType type, String userId, final String groupName, boolean active, boolean andChildren) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException;

    /**
     * Updates an user's principal to the new principal.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user's original principal
     * @param newPrincipal new principal for the user
     * @throws UserNotFoundException if the user cannot be found
     * @throws InvalidVertexTypeException if the type is not USER or APP_USER
     * @throws VertexNotFoundException if the user is an APP user and associated groups (e.g. app access) are missing
     * @throws VertexExistsException if a vertex already exists with the new principal
     */
    public void updateUser(BaseVertex.VertexType type, String principal, String newPrincipal)
            throws UserNotFoundException, InvalidVertexTypeException, VertexNotFoundException, VertexExistsException;

    /**
     * Updates an user's principal to the new principal.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user's original principal
     * @param newPrincipal new principal for the user
     * @param newName new name for the user - if this field is the same as original name or empty, name is not changed
     * @throws UserNotFoundException if the user cannot be found
     * @throws InvalidVertexTypeException if the type is not USER or APP_USER
     * @throws VertexNotFoundException if the user is an APP user and associated groups (e.g. app access) are missing
     * @throws VertexExistsException if a vertex already exists with the new principal
     */
    public void updateUser(BaseVertex.VertexType type, String principal, String newPrincipal, String newName)
            throws UserNotFoundException, InvalidVertexTypeException, VertexNotFoundException, VertexExistsException;

    public void setUserActiveOrNot(BaseVertex.VertexType type, String principal, boolean active) throws InvalidVertexTypeException, UserNotFoundException;
    public void deleteUser(BaseVertex.VertexType type, String principal) throws InvalidVertexTypeException, UserNotFoundException;

    /**
     * Change the friendly name of a group and all of its children's fully qualified names as appropriate. This
     * operation is only allowed if the given {@code User} is authorized to manage all affected groups.
     *
     * @param requesterType whether the requester is a USER or an APP_USER
     * @param userId ID/principal of user requesting the operation
     * @param fullyQualifiedGroupName name of the group whose name will be changed
     * @param newFriendlyName new friendly name for the given group
     * @return a map of names before the change to names after the change, or an empty map if none were changed
     * @throws UserNotFoundException if the user principal could not be mapped to a valid user
     * @throws AccessDeniedException if the given User does not have manage rights on all affected groups
     * @throws VertexNotFoundException if the given group cannot be found
     * @throws ezbake.groups.graph.exception.VertexExistsException if a group by the new name already exists
     * @throws IllegalArgumentException if {@code requesterType} is not USER or APP_USER
     */
    Map<String, String> changeGroupName(BaseVertex.VertexType requesterType, String userId,
            String fullyQualifiedGroupName, String newFriendlyName)
            throws UserNotFoundException, AccessDeniedException, VertexNotFoundException, VertexExistsException;
}
