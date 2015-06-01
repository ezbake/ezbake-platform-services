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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.frames.FrameInitializer;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphConfiguration;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.AbstractModule;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.api.GroupIDProvider;
import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.edge.BaseEdge;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.graph.frames.vertex.Group;
import ezbake.groups.graph.frames.vertex.User;
import ezbake.groups.graph.query.*;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.groups.thrift.GroupInheritancePermissions;
import ezbake.groups.thrift.GroupQueryException;
import ezbake.groups.thrift.UserGroupPermissions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static ezbake.groups.graph.PermissionEnforcer.Permission;

import javax.annotation.Nullable;

public class EzGroupsGraphImpl implements GroupsGraph {
    private static final Logger logger = LoggerFactory.getLogger(EzGroupsGraphImpl.class);
    public static final String GROUP_NAME_SEP = EzGroupsConstants.GROUP_NAME_SEP;

    /**
     * Predicate for filtering out groups that should not be returned to the user, including 'root' and app access
     * related groups.
     */
    private static final com.google.common.base.Predicate<Group>
            NON_USER_FACING_GROUP = new com.google.common.base.Predicate<Group>(){


        private final String appAccessName =
                String.format("%s.%s", Group.COMMON_GROUP, EzGroupsConstants.APP_ACCESS_GROUP);

        @Override
        public boolean apply(@Nullable Group input) {
            if (input != null && input.getGroupName().equals(Group.COMMON_GROUP) || input.getGroupName().equals(appAccessName) || input.getGroupName()
                    .startsWith(appAccessName + '.')) {
                return true;
            }

            return false;
        }
    };

    private final GroupNameHelper gnh = new GroupNameHelper();
    private final TitanGraph graph;
    private final FramedGraphFactory framedGraphFactory;

    private GroupIDProvider idProvider;

    final Object commonGroupId;
    final Object appGroupId;
    final Object appAccessGroupId;

    @Inject
    public EzGroupsGraphImpl(TitanGraph graph, GroupIDProvider idProvider, GroupQuery query) {
        this.graph = graph;
        this.idProvider = idProvider;

        // Set up the framed factory
        framedGraphFactory = new FramedGraphFactory(new AbstractModule() {
            @Override
            public void doConfigure(FramedGraphConfiguration config) {
                config.addFrameInitializer(new FrameInitializer() {
                    @Override
                    public void initElement(Class<?> kind, FramedGraph<?> framedGraph, Element element) {
                        element.setProperty("class", kind.getName());
                    }
                });
            }
        }, new JavaHandlerModule());

        commonGroupId = createCommonGroup();
        appGroupId = addSpecialGroup(Group.APP_GROUP);
        appAccessGroupId = addSpecialGroup(Group.APP_ACCESS_GROUP);
    }

    public GroupIDProvider getIdProvider() {
        return this.idProvider;
    }

    public Graph getGraph() {
        return this.graph;
    }

    public FramedGraph<TitanGraph> getFramedGraph() {
        return framedGraphFactory.create(graph);
    }

    public Object getCommonGroupId() {
        return commonGroupId;
    }

    public Object getAppGroupId() {
        return appGroupId;
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
        graph.shutdown();
    }

    /**
     * Commit a transaction with the graph
     *
     * Transactions must be committed before changes made to the graph in other threads will be realized. If an
     * operation is doing many reads, it may be ideal to wait until the current transaction is fully finished
     * before committing the transaction. For this reason, read operations on this class do not automatically close
     * the transaction, and you must do so by calling this method
     */
    @Override
    public void commitTransaction() {
        commitTransaction(false);
    }

    /**
     * Transactions must be committed before changes made to the graph in other threads will be realized. If an
     * operation is doing many reads, it may be ideal to wait until the current transaction is fully finished
     * before committing the transaction. For this reason, read operations on this class do not automatically close
     * the transaction, and you must do so by calling this method
     *
     * @param rollback optionally, rollback the transaction instead of committing it
     */
    @Override
    public void commitTransaction(boolean rollback) {
        if (rollback) {
            graph.rollback();
        } else {
            graph.commit();
        }
    }

    /**
     * Set up the common group, or just get it's vertex ID. This is the default parent group, and must be created if not
     * present
     *
     * @return vertex id for the common group
     */
    public Object createCommonGroup() {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        Object commonId;

        Iterator<Vertex> v = graph.query()
                .has(BaseVertex.INDEX, Compare.EQUAL, 0)
                .has(Group.GROUP_NAME, Compare.EQUAL, EzGroupsConstants.ROOT)
                .limit(1)
                .vertices().iterator();
        if (!v.hasNext()) {
            try {
                // create the common group
                Group cg = framedGraph.addVertex(null, Group.class);
                cg.setName(EzGroupsConstants.ROOT);
                cg.setGroupName(EzGroupsConstants.ROOT);
                cg.setGroupFriendlyName(EzGroupsConstants.ROOT);
                cg.setIndex(0l);
                cg.setType(BaseVertex.VertexType.GROUP);
                commonId = cg.asVertex().getId();
                graph.commit();
                logger.info("Created common group with name: {}, index:{}", cg.getGroupName(), cg.getIndex());
            } catch (Exception e) {
                graph.rollback();
                logger.error("Failed to create common group", e);
                throw new RuntimeException("Unable to set current ID for common group");
            }
        } else {
            commonId = v.next().getId();
        }

        return commonId;
    }

    public Object addSpecialGroup(String specialName) {
        Vertex parentGroup = graph.getVertex(commonGroupId);

        // Get the vertex/id
        Object specialId;
        String groupName = Joiner.on(GROUP_NAME_SEP).join(Group.COMMON_GROUP, specialName);
        Iterator<Vertex> groups = graph.query()
                .has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .limit(1)
                .vertices().iterator();
        if (groups.hasNext()) {
            specialId = groups.next().getId();
        } else {
            try {
                Group specialGroup = addGroupPrivileged(parentGroup, specialName,
                        new GroupInheritancePermissions(false, false, false, false, false), true, false);
                specialId = specialGroup.asVertex().getId();
            } catch (VertexExistsException e) {
                // Should not happen, but create as best we can
                try {
                    Group specialGroup = getFramedGraph().addVertex(null, Group.class);
                    specialGroup.setGroupName(new GroupNameHelper().addRootGroupPrefix(groupName));
                    specialGroup.setGroupFriendlyName(groupName);
                    specialGroup.setIndex(idProvider.nextID());
                    assignEdges(parentGroup, specialGroup.asVertex(), false, false, false, false, false);
                    specialId = specialGroup.asVertex().getId();
                    graph.commit();
                } catch (Exception e1) {
                    graph.rollback();
                    logger.error("Failed to create {} group", specialName, e1);
                    throw new RuntimeException("Unable to acquire ID for "+specialName);
                }
            } catch (IndexUnavailableException e) {
                // transaction rollback responsibility of thrower
                logger.error("Failed to create {} group. Problem getting id", specialName, e);
                throw new RuntimeException("Unable to acquire ID for "+specialName, e);
            } catch (InvalidGroupNameException e) {
                logger.error("Failed to create {} group. Invalid name", specialName, e);
                throw new RuntimeException("Invalid name for group "+specialName, e);
            }
        }

        return specialId;
    }


    /**
     * Add a new child group to the common group, with the default inheritances
     *
     * @param ownerType
     * @param ownerPrincipal
     * @param groupName
     * @return
     * @throws VertexExistsException
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     */
    public Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName) throws VertexExistsException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        return addGroup(ownerType, ownerPrincipal, groupName, new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
    }


    /**
     * Add a new child group to the common group with the specified inheritances
     *
     * @param ownerType
     * @param ownerPrincipal
     * @param groupName
     * @return
     * @throws VertexExistsException
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     */
    public Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName,
                           GroupInheritancePermissions inheritance, UserGroupPermissions permissions,
                           boolean requireOnlyUser, boolean requireOnlyApp
    ) throws UserNotFoundException, AccessDeniedException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        return addGroup(ownerType, ownerPrincipal, groupName, graph.getVertex(commonGroupId), inheritance, permissions,
                requireOnlyUser, requireOnlyApp);
    }

    /**
     *
     * @param ownerType
     * @param ownerID
     * @param groupName
     * @param parentGroup
     * @return
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     * @throws VertexExistsException
     * @throws VertexNotFoundException
     */
    public Group addGroup(BaseVertex.VertexType ownerType, String ownerID, String groupName, String parentGroup,
                           GroupInheritancePermissions inheritance, UserGroupPermissions permissions,
                           boolean requireOnlyUser, boolean requireOnlyApp
    ) throws UserNotFoundException, AccessDeniedException, VertexExistsException, VertexNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        Iterator<Vertex> parents = graph.query().has(Group.GROUP_NAME, parentGroup).limit(1).vertices().iterator();
        if (!parents.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the parent group: " + parentGroup);
        }

        return addGroup(ownerType, ownerID, groupName, parents.next(), inheritance, permissions, requireOnlyUser,
                requireOnlyApp);
    }

    protected Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName, String parentGroup) throws VertexExistsException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, VertexNotFoundException, InvalidGroupNameException {
        return addGroup(ownerType, ownerPrincipal, groupName, getGroup(parentGroup).asVertex(), new GroupInheritancePermissions(true, false, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
    }

    protected Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName, Group parentGroup) throws VertexExistsException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        return addGroup(ownerType, ownerPrincipal, groupName, parentGroup.asVertex(), new GroupInheritancePermissions(true, false, false, false, false), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
    }

    protected Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName, Vertex parent) throws VertexExistsException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        return addGroup(ownerType, ownerPrincipal, groupName, parent, new GroupInheritancePermissions(), new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
    }

    /**
     * Add a new Group vertex to the graph. A childGroup edge will run from the parent vertex to the new group. Direct
     * admin edges (all of them) will run from the owner to the new group vertex. The inherit flags are used to set up
     * admin edges from the parent group to the new group. If a sibling group with the same name exists, throw an error
     *
     * @param groupName
     * @param parent
     * @throws ezbake.groups.graph.exception.VertexExistsException if a sibling vertex with the same groupName
     * already exists
     * @return vertex id of the newly created group
     */
    protected Group addGroup(BaseVertex.VertexType ownerType, String ownerPrincipal, String groupName, Vertex parent,
                              GroupInheritancePermissions inheritance, UserGroupPermissions permissions,
                              boolean requireOnlyUser, boolean requireOnlyApp
    ) throws AccessDeniedException, UserNotFoundException, VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        logger.info("Processing addGroup with groupName: {}, parent: {}", groupName,
                parent.getProperty(Group.GROUP_NAME));
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        if (isAppAccessGroup(parent)) {
            throw new AccessDeniedException("No child groups may be added to the " + Group.APP_ACCESS_GROUP);
        }

        // Find the owner
        User owner;
        Iterator<User> ownerit = framedGraph.query()
                .has(User.TYPE, Compare.EQUAL, ownerType.toString())
                .has(User.PRINCIPAL, Compare.EQUAL, ownerPrincipal)
                .limit(1)
                .vertices(User.class).iterator();
        if (!ownerit.hasNext()) {
            graph.rollback();
            throw new UserNotFoundException("Requested owner for new group does not exist");
        }
        owner = ownerit.next();

        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));
        // Owner must have admin create child on the parent group
        q.getPermissionEnforcer().validateAuthorized(owner, parent, Permission.CREATE_CHILD);

        return addGroupPrivileged(
                parent, owner.asVertex(), groupName, inheritance, permissions, requireOnlyUser, requireOnlyApp);
    }


    /**
     * Adds a group to a parent group, but does not do any access checks. This should only be used internally, and only
     * if you know what you are doing!
     *
     * @param parent
     * @param owner
     * @param groupName
     * @return
     * @throws VertexExistsException
     */
    private Group addGroupPrivileged(Vertex parent, Vertex owner, String groupName,
                                      GroupInheritancePermissions inheritance, UserGroupPermissions permissions,
                                      boolean requireOnlyUser, boolean requireOnlyApp
    ) throws VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        Group group = addGroupPrivileged(parent, groupName, inheritance, requireOnlyUser, requireOnlyApp);

        // Add direct admin to the group from the owner
        assignEdges(owner, group.asVertex(), permissions.isDataAccess(), permissions.isAdminRead(),
                permissions.isAdminWrite(), permissions.isAdminManage(), permissions.isAdminCreateChild());
        graph.commit();

        return group;
    }

    /**
     * Adds a group to a parent group, but does not do any access checks. This should only be used internally, and only
     * if you know what you are doing!
     *
     * @param parent
     * @param groupName
     * @return
     * @throws VertexExistsException
     */
    private Group addGroupPrivileged(Vertex parent, String groupName, GroupInheritancePermissions inheritance,
                                      boolean requireOnlyUser, boolean requireOnlyApp
    ) throws VertexExistsException, IndexUnavailableException, InvalidGroupNameException {
        if (groupName.contains(EzGroupsConstants.GROUP_NAME_SEP)) {
            throw new InvalidGroupNameException("Group name must not contain '"+
                    EzGroupsConstants.GROUP_NAME_SEP+"'");
        }

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        Group parentGroup = framedGraph.frame(parent, Group.class);

        // Append the parent group name:
        String groupNameWithPath = Joiner.on(GROUP_NAME_SEP).join(parentGroup.getGroupName(), groupName);

        // Check existence - query parent group for group of the same name
        GremlinPipeline siblingExists = new GremlinPipeline(parent)
                .outE(Group.CHILD_GROUP)
                .inV()
                .has(Group.FRIENDLY_GROUP_NAME, groupName);
        if (siblingExists.hasNext()) {
            logger.debug("Parent vertex: {} has children with groupName: {}", parentGroup.getGroupName(), groupName);
            graph.rollback();
            throw new VertexExistsException(parentGroup.getGroupName() + " already has child with name: " + groupName);
        }

        Group g = framedGraph.addVertex(null, Group.class);
        g.setType(BaseVertex.VertexType.GROUP);
        try {
            g.setIndex(idProvider.nextID());
        } catch (Exception e) {
            graph.rollback();
            throw new IndexUnavailableException("Unable to acquire an index for group");
        }
        g.setName(groupName);
        g.setGroupFriendlyName(groupName);
        g.setGroupName(groupNameWithPath);
        g.setRequireOnlyUser(requireOnlyUser);
        g.setRequireOnlyApp(requireOnlyApp);

        parentGroup.addChildGroup(g);
        assignEdges(parent, g.asVertex(), inheritance.isDataAccess(), inheritance.isAdminRead(),
                inheritance.isAdminWrite(), inheritance.isAdminManage(), inheritance.isAdminCreateChild());

        graph.commit();
        return g;
    }

    /**
     * Get group metadata with is ADMIN_READ permission check
     *
     * @param type of Vertex
     * @param userId of a user calling getGroup
     * @param groupName being searched for
     *
     * @return Group
     *
     * @throws VertexNotFoundException
     * @throws UserNotFoundException
     * @throws AccessDeniedException
     * @throws InvalidVertexTypeException
     */
    public Group getGroup(BaseVertex.VertexType type, String userId, String groupName) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException, InvalidVertexTypeException  {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // Get the group vertex
        Iterator<Group> g = framedGraph.query()
                .has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .has(BaseVertex.TYPE, Compare.EQUAL, BaseVertex.VertexType.GROUP.toString())
                .limit(1)
                .vertices(Group.class).iterator();
        if (!g.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Group not found: " + groupName);
        }

        final Group group = g.next();
        final User user = getUser(type, userId);

        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));
        q.getPermissionEnforcer().validateAuthorized(user, group, Permission.READ);

        return group;
    }

    /**
     * Returns true if group exists.
     *
     * @param name name of group to check
     * @return true if group exists, false if not
     */
    private boolean groupExists(String name) {
        try {
            getGroup(name);
            return true;
        } catch (VertexNotFoundException e) {
            return false;
        }
    }

    /**
     * Get a group with NO permission check.
     *
     * @param groupName name of group to get.
     * @return the requested group.
     * @throws VertexNotFoundException if the group could not be found
     */
    @Override
    public Group getGroup(String groupName) throws VertexNotFoundException {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        // Get the group vertex
        Iterator<Group> g = framedGraph.query().has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .has(BaseVertex.TYPE, Compare.EQUAL, BaseVertex.VertexType.GROUP.toString()).limit(1)
                .vertices(Group.class).iterator();
        if (!g.hasNext()) {
            graph.rollback();
            final String errMsg = "Group not found: " + groupName;
            logger.error(errMsg);
            throw new VertexNotFoundException(errMsg);
        }

        return g.next();
    }

    public Set<Group> getGroups(Set<String> groupNames) {
        Set<Group> groups = Sets.newHashSet();
        for (String groupName : groupNames) {
            try {
                groups.add(getGroup(groupName));
            } catch (VertexNotFoundException e) {
                // ignoring group
            }
        }
        return groups;
    }

    @Override
    public Set<User> getUsers(BaseVertex.VertexType type, Set<String> ezbakeUserIds) {
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            final String errMsg = String.format(
                    "Attempted to get invalid user type. Type '%s' is not a valid user type to get!", type);

            logger.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        Set<User> users = Sets.newHashSet();
        for (String userId : ezbakeUserIds) {
            try {
                users.add(getUserByEzBakeId(type, userId));
            } catch (VertexNotFoundException e) {
                // ignoring user
            }
        }

        return users;
    }

    /**
     * Gets all groups excluding any 'special' groups.
     *
     * @return all groups excluding any 'special' groups
     */
    @Override
    public Set<Group> getGroups() {
        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        final Iterable<Group> groups = framedGraph.frameVertices(
                framedGraph.query().has(BaseVertex.TYPE, BaseVertex.VertexType.GROUP.toString()).vertices(),
                Group.class);

        final Set<Group> retrievedGroups = Sets.newHashSet(groups);
        Iterables.removeIf(retrievedGroups, NON_USER_FACING_GROUP);

        return retrievedGroups;
    }

    @Override
    public Set<Group> getGroupsByIds(Set<Long> indices) {
        final Set<Group> foundGroups = Sets.newHashSet();

        for (Long id : indices) {
            final Group group = getGroupById(id);
            if (group != null) {
                foundGroups.add(group);
            }
        }

        return foundGroups;
    }

    @Override
    public Set<Group> getGroupsByIdsWithAuths(BaseVertex.VertexType type, String principal, Set<Long> indices)
            throws UserNotFoundException, InvalidVertexTypeException {
        final Set<Group> groups = Sets.newHashSet();

        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        for (Group group : getGroupsByIds(indices)) {
            if (q.getPermissionEnforcer().hasAnyPermission(getUser(type, principal), group)) {
                groups.add(group);
            }
        }

        return groups;
    }

    /**
     * Gets a group by its EzGroups index.
     *
     * @param ezGroupsIndex EzGroups index
     * @return group with the given EzGroups index or null if a group with that index cant be found
     */
    private Group getGroupById(long ezGroupsIndex) {
        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        // Get the group vertex
        final Iterator<Group> g = framedGraph.query().has(Group.INDEX, Compare.EQUAL, ezGroupsIndex)
                .has(BaseVertex.TYPE, Compare.EQUAL, BaseVertex.VertexType.GROUP.toString()).limit(1)
                .vertices(Group.class).iterator();
        if (!g.hasNext()) {
            return null;
        }

        return g.next();
    }

    /**
     * Gets an Ezbake User by the given EzBake ID.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param ezBakeUserId user ID of the desired user
     * @return the requested User object
     * @throws VertexNotFoundException if the User could not be found
     */
    private User getUserByEzBakeId(BaseVertex.VertexType type, String ezBakeUserId) throws VertexNotFoundException {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // Get the user vertex
        Iterator<User> g = framedGraph.query()
                .has(User.PRINCIPAL, Compare.EQUAL, ezBakeUserId)
                .has(BaseVertex.TYPE, Compare.EQUAL, type.toString())
                .limit(1)
                .vertices(User.class).iterator();
        if (!g.hasNext()) {
            final String errMsg = "User not found: " + ezBakeUserId;
            logger.error(errMsg);
            throw new VertexNotFoundException(errMsg);
        }

        return g.next();
    }

    /**
     * Gets if an user exists in groups.
     *
     * @param type type of user
     * @param ezBakeUserId ezBake ID of user
     * @return true if the user exists, false if not
     */
    private boolean userExists(BaseVertex.VertexType type, String ezBakeUserId){
        try{
            getUserByEzBakeId(type, ezBakeUserId);
            return true;
        }catch(VertexNotFoundException e){
            return false;
        }
    }
    public void setGroupInheritance(String groupName, boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws VertexNotFoundException {
        Iterator<Vertex> gs = graph.query()
                .has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .limit(1)
                .vertices().iterator();
        if (!gs.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the group: " + groupName);
        }
        setGroupInheritance(gs.next(), dataAccess, adminRead, adminWrite, adminManage, adminCreateChild);
    }

    protected void setGroupInheritance(Vertex group, boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws VertexNotFoundException {
        // Find the parent group
        GremlinPipeline<Vertex, Vertex> parentPipe = new GremlinPipeline<Vertex, Vertex>(group).in(Group.CHILD_GROUP);
        if (!parentPipe.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Unable to find parent group of: " + group.getProperty(Group.GROUP_NAME));
        }
        Vertex parent = parentPipe.next();

        assignEdges(parent, group, dataAccess, adminRead, adminWrite, adminManage, adminCreateChild);
        graph.commit();
    }

    /**
     * Change the friendly name of a group and all of its children's fully qualified names as appropriate. Note that
     * there are NO checks on whether this operation should be permit here. For a change group name operation that
     * should have permission checked, see {@link #changeGroupName(ezbake.groups.graph.frames.vertex.User, String,
     * String)}
     * <p/>
     * For example: 'root.parentName.myName' could be a fully qualified group name with a friendly name of 'myName'. By
     * passing in this fully qualified group name and new friendly name 'newName' the fully qualified name would become
     * 'root.parentName.newName' with friendly name 'newName'.
     *
     * @param groupName name of the group whose name to change
     * @param newFriendlyName new friendly name for the given group
     * @return a map of old group names to new group names if any were changed
     * @throws VertexNotFoundException if the given group cannot be found
     * @throws VertexExistsException if a group by the new name already exists
     */
    public Map<String, String> changeGroupName(String groupName, String newFriendlyName)
            throws VertexNotFoundException, VertexExistsException {
        if (groupName.endsWith(newFriendlyName)) {
            return Maps.newHashMap();
        }

        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        final String newGroupName = gnh.changeGroupName(groupName, newFriendlyName);
        if(groupExists(newGroupName)){
            final String errMsg = String.format(
                    "Cannot rename a group to a group that already exists! Changing group's name '%s to '%s' cannot " +
                            "be completed.",
                    groupName, newGroupName);

            logger.error(errMsg);
            throw new VertexExistsException(errMsg);
        }

        final Group group = getGroup(groupName);
        logger.info("Updating group name {} -> {}", groupName, newGroupName);
        group.setGroupName(newGroupName);
        group.setGroupFriendlyName(newFriendlyName);

        final Map<String, String> changedGroupNames = Maps.newHashMap();
        changedGroupNames.put(groupName, newGroupName);
        // Change the group name and all child groups
        final List<Vertex> groups = Lists.newArrayList();
        getChildGroups(true, framedGraph, group.asVertex(), true).store(groups).iterate();

        for (Vertex v : groups) {
            final Group currentGroup = framedGraph.frame(v, Group.class);
            final String cgName = currentGroup.getGroupName();
            final String cgNewName = cgName.replace(groupName, newGroupName);
            logger.info("Updating child group name {} -> {}", cgName, cgNewName);
            currentGroup.setGroupName(cgNewName);
            changedGroupNames.put(cgName, cgNewName);
        }
        graph.commit();

        return changedGroupNames;
    }

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
    @Override
    public Map<String, String> changeGroupName(BaseVertex.VertexType requesterType, String userId,
            String fullyQualifiedGroupName, String newFriendlyName)
            throws UserNotFoundException, AccessDeniedException, VertexNotFoundException, VertexExistsException {
        if (requesterType == BaseVertex.VertexType.APP_USER || requesterType == BaseVertex.VertexType.USER) {
            final FramedGraph<TitanGraph> framedGraph = getFramedGraph();
            final User requester = findAndRetrieveUserById(requesterType, userId, framedGraph);

            return changeGroupName(requester, fullyQualifiedGroupName, newFriendlyName);
        } else {
            final String errMsg = String.format(
                    "Type of requester for a name change must be either '%s' or '%s'!", BaseVertex.VertexType.APP_USER,
                    BaseVertex.VertexType.USER);

            logger.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Change the friendly name of a group and all of its children's fully qualified names as appropriate. This
     * operation is only allowed if the given {@code User} is authorized to manage all affected groups.
     *
     * @param requester User requesting the operation
     * @param fullyQualifiedGroupName name of the group whose name will be changed
     * @param newFriendlyName new friendly name for the given group
     * @return a Map of old group names to new group names or an empty map if none were changed
     * @throws AccessDeniedException if the given User does not have manage rights on all affected groups
     * @throws VertexNotFoundException if the given group cannot be found
     * @throws ezbake.groups.graph.exception.VertexExistsException if a group by the new name already exists
     */
    private Map<String, String> changeGroupName(final User requester, String fullyQualifiedGroupName,
            String newFriendlyName) throws AccessDeniedException, VertexNotFoundException, VertexExistsException {
        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        final Group group = getGroup(fullyQualifiedGroupName);

        final GremlinPipeline<Vertex, Vertex> groupVertexPipe =
                getChildGroups(true, framedGraph, group.asVertex(), true);

        final List<Vertex> verticesToCheck = groupVertexPipe.toList();
        verticesToCheck.add(group.asVertex());

        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));
        q.getPermissionEnforcer().validateAuthorized(requester, verticesToCheck, Permission.MANAGE);

        return changeGroupName(fullyQualifiedGroupName, newFriendlyName);
    }

    /**
     * Add a new user to the graph. Create a DataAccess edge, and all the admin edges to the common group
     * @param type
     * @param principal
     * @param name
     * @throws ezbake.groups.graph.exception.VertexExistsException
     * @return
     */
    @Override
    public User addUser(BaseVertex.VertexType type, String principal, String name) throws InvalidVertexTypeException, VertexExistsException, AccessDeniedException, UserNotFoundException, IndexUnavailableException, InvalidGroupNameException {
        logger.info("Processing addUser type: {}, principal: {}, name: {}", type, principal, name);

        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            throw new InvalidVertexTypeException("Cannot create user of type: " + type);
        }

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        Iterator<User> users = framedGraph.query()
                .has(User.PRINCIPAL, Compare.EQUAL, principal)
                .has(BaseVertex.TYPE, Compare.EQUAL, type.toString())
                .limit(1)
                .vertices(User.class).iterator();
        if (users.hasNext()) {
            logger.warn("User with principal: {} and type: {} already exists, not creating new user", principal, type);
            graph.rollback();
            throw new VertexExistsException("A " + type + " with principal: " + principal + " already exists");
        }

        // Add the User
        User user = framedGraph.addVertex(null, User.class);
        user.setPrincipal(principal);
        user.setName(name);
        user.setType(type);
        user.setTerminator(false);
        try {
            user.setIndex(idProvider.nextID());
        } catch (Exception e) {
            graph.rollback();
            logger.error("Error getting the next group ID", e);
            throw new IndexUnavailableException("Unable to receive new index", e);
        }

        // Add edges for the User to the common group
        Vertex commonGroup = graph.getVertex(commonGroupId);
        assignEdges(user.asVertex(), commonGroup, true, false, false, false, true);

        switch (type) {
            case APP_USER:
                if (Strings.isNullOrEmpty(name)) {
                    graph.rollback();
                    throw new InvalidGroupNameException("App Users must have a name in order to create app groups");
                }
                // App users get direct data_access on appaccess group
                assignEdges(user.asVertex(), graph.getVertex(appAccessGroupId), true, false, false, false, false);

                // Add the app group and app access group
                Object appGroup = addGroupPrivileged(graph.getVertex(appGroupId), user.asVertex(), name,
                        new GroupInheritancePermissions(false, false, false, false, false),
                        new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
                Object appAccessGroup = addGroupPrivileged(graph.getVertex(appAccessGroupId), user.asVertex(), name,
                        new GroupInheritancePermissions(true, false, false, false, false),
                        new UserGroupPermissionsWrapper(true, true, true, true, true), true, false);
                break;
            case USER:
                break;
            default:
                logger.error("Adding user encountered type : {}", type);
        }

        graph.commit();
        return user;
    }

    /**
     * Gets an user by its Titan/ EzGroups ID.
     *
     * @param type whether the user is a USER or an APP_USER
     * @param id Titan ID/ vertex ID of the user
     * @return an User identified by the given ID and type
     * @throws InvalidVertexTypeException if the UserType is not USER or APP_USER
     * @throws UserNotFoundException if the user cannot be found
     */
    public User getUser(BaseVertex.VertexType type, String id)
            throws InvalidVertexTypeException, UserNotFoundException {
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            throw new InvalidVertexTypeException(
                    String.format("'%s' is not an user type that can be retrieved!", type));
        }
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        return findAndRetrieveUserById(type, id, framedGraph);
    }

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
    @Override
    public void updateUser(BaseVertex.VertexType type, String principal, String newPrincipal)
            throws UserNotFoundException, InvalidVertexTypeException, VertexNotFoundException, VertexExistsException {

        updateUser(type, principal, newPrincipal, null);
    }

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
    @Override
    public void updateUser(BaseVertex.VertexType type, String principal, String newPrincipal, String newName)
            throws UserNotFoundException, InvalidVertexTypeException, VertexNotFoundException, VertexExistsException {
        logger.info(
                "Processing updateUser user type: {}, principal: {}, newPrincipal: {}", type, principal, newPrincipal);

        // only accept valid user types
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            final String errMsg = "Cannot update user for type: " + type;
            logger.error("Cannot update user for type: " + type);
            throw new InvalidVertexTypeException(errMsg);
        }

        User user;

        // prevent principal change to a principal that already exists
        if (!principal.equals(newPrincipal) && userExists(type, newPrincipal)) {
            final String errMsg = String.format(
                    "Cannot update user with principal '%s' to new principal '%s', a vertex already exists with that "
                            + "principal!", principal, newPrincipal);

            logger.error(errMsg);
            throw new VertexExistsException(errMsg);
        } else{
            // get the user and set its principal to the new value
            user = getUserByEzBakeId(type, principal);
            user.setPrincipal(newPrincipal);
        }

        // Only set the name if a value is provided
        if (!Strings.isNullOrEmpty(newName) && !newName.trim().isEmpty()) {
            // If app user, need to update the app groups
            if (type == BaseVertex.VertexType.APP_USER) {
                final String currentName = user.getName();

                // Update group name
                final GroupNameHelper helper = new GroupNameHelper();
                final String appGroupName = helper.getNamespacedAppGroup(currentName);
                final String appAccessGroupName = helper.getNamespacedAppAccessGroup(currentName);

                try {
                    changeGroupName(appGroupName, newName);
                    changeGroupName(appAccessGroupName, newName);
                } catch (VertexNotFoundException | VertexExistsException e) {
                    logger.error("Failed to change APP_USER(id:{}) Name {} -> {}", principal, currentName, newName);
                    graph.rollback();
                    throw e;
                }
            }

            // update the user name
            user.setName(newName);
        }

        graph.commit();
    }

    public void setUserActiveOrNot(BaseVertex.VertexType type, String principal, boolean active) throws InvalidVertexTypeException, UserNotFoundException {
        logger.info("Processing {} User user type: {}, principal: {}", active?"activate":"deactivate", type, principal);
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            throw new InvalidVertexTypeException("Cannot add vertex of type: " + type + " to groups");
        }

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        // Get the user vertex
        final User user = findAndRetrieveUserById(type, principal, framedGraph);
        user.setIsActive(active);
        graph.commit();
    }

    public void setGroupActiveOrNot(BaseVertex.VertexType type, String userId, final String groupName, boolean active) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException {
        setGroupActiveOrNot(type, userId, groupName, active, false);
    }

    public void setGroupActiveOrNot(BaseVertex.VertexType type, String userId, final String groupName, boolean active, boolean andChildren) throws VertexNotFoundException, UserNotFoundException, AccessDeniedException {
        logger.info("Processing activate/deactivate group: {}, become active: {}", groupName, active);
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // find the user
        final User user = findAndRetrieveUserById(type, userId, framedGraph);

        // find the group
        Iterator<Group> gs = framedGraph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices(Group.class).iterator();
        if (!gs.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Group: " + groupName + " does not exist. Cannot change active/deactivated");
        }
        Group g = gs.next();

        // Find the group by traversing from the user along admin manage
        GremlinPipeline<Vertex, Vertex> gi = new GremlinPipeline<Vertex, Vertex>(user.asVertex())
                .as("traversalLoop")
                .outE(BaseEdge.EdgeType.A_MANAGE.toString())
                .gather().scatter()
                .inV()
                .loop("traversalLoop", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                                   @Override
                                   public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        return !vertexLoopBundle.getObject().getProperty(Group.GROUP_NAME).equals(groupName);
                }
            });
        if (!gi.hasNext()) {
            // Check if the vertex even exists
            if (!graph.query().has(Group.GROUP_NAME, groupName).vertices().iterator().hasNext()) {
                graph.rollback();
                throw new VertexNotFoundException("Group: " + groupName +
                        " does not exist. Cannot change active/deactivated");
            } else {
                graph.rollback();
                throw new AccessDeniedException("User must have admin manage permissions on a group to " +
                        "activate/deactivate it");
            }
        }
        Group g1 = framedGraph.frame(gi.next(), Group.class);
        g1.setIsActive(active);

        if (andChildren) {
            GremlinPipeline<Vertex, Vertex> pipe = new GremlinPipeline<Vertex, Vertex>(g.asVertex())
                    .as("LOOPING")
                    // Make sure we can get back to the user
                    .inE(BaseEdge.EdgeType.A_MANAGE.toString())
                    .gather().scatter()
                    .outV()
                    .loop("LOOPING", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            return !vertexLoopBundle.getObject().getId().equals(user.asVertex().getId());
                        }
                    })
                    .back("LOOPING")
                    .as("NEWLOOP")
                    .outE(Group.CHILD_GROUP)
                    .inV()
                    .loop("NEWLOOP", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            return true;
                        }
                    }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            return true;
                        }
                    })
                    .as("child groups")
                    .inE(BaseEdge.EdgeType.A_MANAGE.toString())
                    .outV()
                    .loop("child groups", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                        @Override
                        public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                            return !vertexLoopBundle.getObject().getId().equals(user.asVertex().getId());
                        }
                    })
                    .back("child groups")
                    .cast(Vertex.class);

            for (Vertex v : pipe) {
                Group childGroup = framedGraph.frame(v, Group.class);
                logger.info("Also {} child group {}", (active)?"activating":"deactivating", childGroup.getGroupName());
                childGroup.setIsActive(active);
            }
        }
        graph.commit();
    }

    public void deleteUser(BaseVertex.VertexType type, String principal) throws InvalidVertexTypeException, UserNotFoundException {
        logger.info("Processing deleteUser user type: {}, principal: {}", type, principal);
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            throw new InvalidVertexTypeException("Cannot add vertex of type: " + type + " to groups");
        }

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        // Get the user vertex
        final User user = findAndRetrieveUserById(type, principal, framedGraph);

        graph.removeVertex(user.asVertex());
        graph.commit();
    }


    /**
     * Add a user to a group, which involves creating a direct DATA_ACCESS edge between the two
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user principal
     * @param groupName String group name
     */
    public void addUserToGroup(BaseVertex.VertexType type, String principal, String groupName) throws VertexNotFoundException, UserNotFoundException, InvalidVertexTypeException {
        addUserToGroup(type, principal, groupName, true, false, false, false, false);
    }

    /**
     * Add a user to a group, which involves creating a direct DATA_ACCESS edge between the two
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user principal
     * @param groupName String group name
     */
    public void addUserToGroup(BaseVertex.VertexType type, String principal, String groupName, boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws VertexNotFoundException, UserNotFoundException, InvalidVertexTypeException {
        // Get the group verted
        Iterator<Group> groups = getFramedGraph().query()
                .has(Group.GROUP_NAME, Compare.EQUAL, groupName)
                .limit(1)
                .vertices(Group.class).iterator();
        if (!groups.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the group: " + groupName);
        }
        addUserToGroup(type, principal, groups.next(), dataAccess, adminRead, adminWrite, adminManage, adminCreateChild);
    }

    /**
     * Add a user to a group, which involves creating a direct DATA_ACCESS edge between the two
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user principal
     * @param group Vertex of the group the user should be added to
     */
    public void addUserToGroup(BaseVertex.VertexType type, String principal, Group group) throws UserNotFoundException, InvalidVertexTypeException {
        addUserToGroup(type, principal, group, true, false, false, false, false);
    }

    /**
     * Add a user to a group, which involves creating a direct DATA_ACCESS edge between the two
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user principal
     * @param group Vertex of the group the user should be added to
     */
    public void addUserToGroup(BaseVertex.VertexType type, String principal, Group group, boolean dataAcces, boolean adminRead, boolean adminWrite, boolean adminManage, boolean adminCreateChild) throws UserNotFoundException, InvalidVertexTypeException {
        logger.info("Processing addUserToGroup group: {}, user type: {}, principal: {}", group.getGroupName(), type,
                principal);
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            graph.rollback();
            throw new InvalidVertexTypeException("Cannot add vertex of type: " + type + " to groups");
        }
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // Get the user vertex
        final User user = findAndRetrieveUserById(type, principal, framedGraph);
        assignEdges(user.asVertex(), group.asVertex(), dataAcces, adminRead, adminWrite, adminManage, adminCreateChild);

        graph.commit();
    }


    public void removeUserFromGroup(BaseVertex.VertexType type, String principal, String groupName) throws UserNotFoundException, VertexNotFoundException, InvalidVertexTypeException {
        // Get the group verted
        Iterator<Vertex> groups = graph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices().iterator();
        if (!groups.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the group: " + groupName);
        }
        removeUserFromGroup(type, principal, groups.next().getId());
    }

    /**
     * o
     * Remove a user from a group. The user must have explict access to that group in order to be removed. This simply
     * removes the DATA_ACCESS edges from the user to the group
     *
     * @param type whether the user is a USER or an APP_USER
     * @param principal user principal
     * @param groupId Vertex ID of the group the user should be removed from
     */
    public void removeUserFromGroup(BaseVertex.VertexType type, String principal, Object groupId) throws UserNotFoundException, InvalidVertexTypeException {
        logger.info("Processing removeUserFromGroup group: {}, user type: {}, principal: {}", groupId, type, principal);
        if (type != BaseVertex.VertexType.APP_USER && type != BaseVertex.VertexType.USER) {
            graph.rollback();
            throw new InvalidVertexTypeException("Cannot remove vertex of type: " + type + " to groups");
        }

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // Get the user vertex
        final User user = findAndRetrieveUserById(type,principal,framedGraph);
        Group group = framedGraph.getVertex(groupId, Group.class);

        removeEdges(user.asVertex(), group.asVertex(), true, false, false, false, false);
        graph.commit();
    }

    /**
     * Traverse the graph to determine all of the groups an user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param vertexID Titan ID of the user
     * @return the set of Group objects for all the groups the user is a member of
     */
    @VisibleForTesting
    Set<Group> userGroups(Object vertexID) {
        return userGroups(graph.getVertex(vertexID), false, true);
    }

    /**
     * Traverse the graph to determine all of the groups an user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param userID EzBake ID of the user for which to get the groups they are a member of
     * @return the set of all groups the user is a member of
     * @throws UserNotFoundException if the user whose groups were requested could not be found
     */
    @Override
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID) throws UserNotFoundException {
        return userGroups(userType, userID, false);
    }

    /**
     * Traverse the graph to determine all of the groups a user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group.
     *
     * @param userID EzBake ID of the user for which to get the groups they are a member of
     * @return the set of all groups the user is a member of
     * @throws UserNotFoundException if the user could not be found
     */
    @Override
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID, boolean explicitGroupsOnly)
            throws UserNotFoundException {
        return userGroups(userType, userID, explicitGroupsOnly, true);
    }

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
    @Override
    public Set<Group> userGroups(BaseVertex.VertexType userType, String userID, boolean explicitGroupsOnly,
            boolean includeInactive) throws UserNotFoundException {
        Iterator<Vertex> users =
                graph.query().has(BaseVertex.TYPE, userType.toString()).has(User.PRINCIPAL, userID).limit(1).vertices()
                        .iterator();
        if (!users.hasNext()) {
            graph.rollback();
            throw new UserNotFoundException("No user found with ID: " + userID);
        }
        return userGroups(users.next(), explicitGroupsOnly, includeInactive);
    }

    /**
     * Traverse the graph to determine all of the groups a user is a member of. Group membership is defined as having a
     * path along DATA_ACCESS edges from the user to the group. If retrieving explicit groups, only return groups that
     * have a direct DATA_ACCESS edge.
     *
     * @param user the user vertex from which to traverse down DATA_ACCESS edges
     * @param explicitPath if true, only return groups that have a direct DATA_ACCESS edge
     * @return the set of all groups the user is a member of
     */
    @VisibleForTesting
    Set<Group> userGroups(Vertex user, final boolean explicitPath, boolean includeInactive) {
        final Set<Group> groups = new HashSet<>();

        GremlinPipeline<Object, Vertex> pipe =
                new GremlinPipeline<>(user).as("user_groups").outE(BaseEdge.EdgeType.DATA_ACCESS.toString()).gather()
                        .scatter().inV().loop(
                        "user_groups", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return !explicitPath;
                            }
                        }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return BaseVertex.VertexType.GROUP.toString()
                                        .equals(vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE));
                            }
                        }).dedup();

        // Collect the results
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        while (pipe.hasNext()) {
            Group g = framedGraph.frame(pipe.next(), Group.class);
            if (!includeInactive && !g.isActive()) {
                continue;
            }
            groups.add(g);
        }
        return groups;
    }

    /**
     * Look up members of a group by first querying the graph for the group by group name. Collect the group members by
     * traversing the graph from a group vertex, along IN DATA_ACCESS edges, to find users who have access to that group
     *
     * @param groupName name of the group
     * @param explicityOnly only return group members who are explicitly included in the group
     * @return a set of User objects (both app and regular users)
     */
    public Set<User> groupMembers(BaseVertex.VertexType userType, String userId, String groupName, boolean explicityOnly) throws VertexNotFoundException, UserNotFoundException {
        return groupMembers(userType, userId, groupName, true, true, explicityOnly);
    }

    /**
     * Look up members of a group by first querying the graph for the group by group name. Collect the group members by
     * traversing the graph from a group vertex, along IN DATA_ACCESS edges, to find users who have access to that group
     *
     * @param groupName name of the group
     * @param explicityOnly only return group members who are explicitly included in the group
     * @param includeUsers whether or not users should be included in the query
     * @param includeApps whether or not apps should be included in the query
     * @return a set of User objects (both app and regular users)
     */
    @Override
    public Set<User> groupMembers(BaseVertex.VertexType userType, String userId, String groupName, final boolean includeUsers, final boolean includeApps, boolean explicityOnly) throws VertexNotFoundException, UserNotFoundException {
        // Find the owner
        Iterator<Vertex> users = graph.query()
                .has(BaseVertex.TYPE, userType.toString())
                .has(User.PRINCIPAL, userId)
                .limit(1).vertices().iterator();
        if (!users.hasNext()) {
            graph.rollback();
            throw new UserNotFoundException("No user id: "+userId);
        }

        // Find the group
        Iterator<Vertex> groups = graph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices().iterator();
        if (!groups.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("No group found with name: " + groupName);
        }

        return groupMembers(users.next(), groups.next(), includeUsers, includeApps, explicityOnly);
    }

    @Override
    public Set<String> specialAppNamesQuery(String specialGroupName, Vertex user) throws VertexNotFoundException {
        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        return q.getAppGroupQuery().specialAppNamesQuery(specialGroupName, user);
    }

    /**
     * Traverse the graph from a group vertex, along IN DATA_ACCESS edges, to find users who have access to that group
     *
     * @param group Vertex of group where the query should begin
     * @param explicitPath if true, only return users that have a direct edge
     * @param includeUsers whether group members of type USER are included in result
     * @param includeApps whether group members of type APP_USER are included in result
     * @return a set of User objects (both app and regular users)
     */
    public Set<User> groupMembers(final Vertex user, final Vertex group, final boolean includeUsers, final boolean includeApps, final boolean explicitPath) {
        final Set<User> users = new HashSet<>();

        GremlinPipeline<Object, Vertex> pipe = new GremlinPipeline<>(user)
                .as("owner_access")
                .outE(BaseEdge.EdgeType.A_READ.toString())
                .inV()
                .loop("owner_access", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        return !vertexLoopBundle.getObject().getId().equals(group.getId());
                    }
                })
                .as("group")
                .as("group_member_traversal")
                .inE(BaseEdge.EdgeType.DATA_ACCESS.toString())
                .outV()
                .loop(
                        "group_member_traversal", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return !explicitPath;
                            }
                        }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                boolean isUser = BaseVertex.VertexType.USER.toString()
                                        .equals(vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE));
                                boolean isApp = BaseVertex.VertexType.APP_USER.toString()
                                        .equals(vertexLoopBundle.getObject().getProperty(BaseVertex.TYPE));

                                return (includeUsers && isUser) || (includeApps && isApp);
                            }
                        }).dedup();

        FramedGraph<TitanGraph> framedGraph = getFramedGraph();
        while(pipe.hasNext()) {
            User member = framedGraph.frame(pipe.next(), User.class);
            users.add(member);
        }
        return users;
    }


    /**
     * Get child groups starting at a named parent group, as a user. The user must have ADMIN_READ permissions on the
     * parent group to get any results, and they will only see child groups they also have ADMIN_READ on. This will
     * only return direct children of the named parent group
     *
     * @param userType type of user, USER or APP_USER
     * @param id the user's unique external id
     * @param groupName name of the group at which to start the query
     * @return a set of all the child groups the user can view
     * @throws UserNotFoundException
     * @throws VertexNotFoundException
     * @throws AccessDeniedException
     */
    public Set<Group> getGroupChildren(BaseVertex.VertexType userType, String id, String groupName) throws UserNotFoundException, VertexNotFoundException, AccessDeniedException {
        return getGroupChildren(userType, id, groupName, false);
    }

    /**
     * Get child groups starting at a named parent group, as a user. The user must have ADMIN_READ permissions on the
     * parent group to get any results, and they will only see child groups they also have ADMIN_READ on.
     *
     * @param userType type of user, USER or APP_USER
     * @param id the user's unique external id
     * @param recurse if true, this will return child groups of the children
     * @return a set of all the child groups the user can view
     * @throws UserNotFoundException
     * @throws VertexNotFoundException
     * @throws AccessDeniedException
     */
    public Set<Group> getGroupChildren(BaseVertex.VertexType userType, String id, String groupName, final boolean recurse) throws UserNotFoundException, VertexNotFoundException, AccessDeniedException {
        return getGroupChildren(userType, id, groupName, recurse, false);
    }

    public Set<Group> getGroupChildren(BaseVertex.VertexType userType, String id, String groupName, final boolean recurse, boolean includeSelf) throws UserNotFoundException, VertexNotFoundException, AccessDeniedException {
        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // find the user vertex
        final User owner = findAndRetrieveUserById(userType, id, framedGraph);

        // find the parent group
        Iterator<Vertex> parents = graph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices().iterator();
        if (!parents.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the parent group: " + groupName);
        }
        Vertex parent = parents.next();

        final GremlinPipeline<Vertex, Vertex> groupPath = getChildGroups(recurse, framedGraph, parent,true);

        groupPath.as("traverse_back_to_user")
                .inE(BaseEdge.EdgeType.A_READ.toString())
                .outV()
                .loop("traverse_back_to_user", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                    @Override
                    public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                        return !vertexLoopBundle.getObject().equals(owner.asVertex());
                    }
                })
                .back("traverse_back_to_user").cast(Vertex.class)
                .enablePath() ;


        Set<Group> childGroups = Sets.newHashSet(framedGraph.frameVertices(groupPath, Group.class));
        if (includeSelf) {
            childGroups.add(framedGraph.frame(parent, Group.class));
        }
        return childGroups;
    }

    /**
     * Gets children of a given Vertex. A group-vertex is considered a child group of another group-vertex if it
     * has path of incoming {@link Group#CHILD_GROUP} edges connecting to that vertex. If {@code recurse} is false, only
     * the children one hop away are returned, otherwise all children are returned.
     *
     * @param recurse true if all children are desired, false if only the immediate children
     * @param framedGraph framed graph to traverse
     * @param parent vertex to get the children from
     * @param activeGroupsOnly whether or not to only return active groups
     * @return a gremlin pipeline with the requested child groups
     */
    private GremlinPipeline<Vertex, Vertex> getChildGroups(final boolean recurse,
            final FramedGraph<TitanGraph> framedGraph, Vertex parent, final boolean activeGroupsOnly) {
        final String findChildGroups = "findChildGroups";

        return new GremlinPipeline<Vertex, Vertex>(parent)
                // Traverse all the child groups, and emit as appropriate
                .as(findChildGroups)
                .outE(Group.CHILD_GROUP)
                .gather().scatter()
                .inV().loop(
                        findChildGroups,

                        new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {

                                return recurse;
                            }
                        }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                final Group g = framedGraph.frame(vertexLoopBundle.getObject(), Group.class);
                                if (activeGroupsOnly) {
                                    return g.isActive();
                                }else{
                                    return true;
                                }
                            }
                        }).dedup();
    }

    /**
     * Given group name, infer permissions based on all edges between given vertex (determined by group name) and its
     * parent
     *
     * @param groupName
     * @return inheritance permissions
     * @throws VertexNotFoundException
     */
    public GroupInheritancePermissions getGroupInheritancePermissions(String groupName) throws VertexNotFoundException {
        List<String> split = Splitter.on(EzGroupsConstants.GROUP_NAME_SEP).splitToList(groupName);

        String parentName;
        if(split.size() > 1) { // prevent array out of boundary exception
            parentName = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).join(split.subList(0, split.size() - 1));
        } else {
            parentName = groupName;
        }

        Iterator<Vertex> parents = graph.query().has(Group.GROUP_NAME, parentName).limit(1).vertices().iterator();
        Iterator<Vertex> children = graph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices().iterator();

        if (!parents.hasNext() || !children.hasNext()) {
            logger.error("Cannot find group name: parent group {} | child group {}", parentName, groupName);
            graph.rollback();
            throw new VertexNotFoundException("Cannot find group name: parent group " + parentName + " | child group " + groupName );
        }

        Vertex parent = parents.next();
        Vertex child = children.next();
        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        return new GroupInheritancePermissions(
                q.getBaseQuery().pathExists(parent, child.getId(), BaseEdge.EdgeType.DATA_ACCESS.toString()),
                q.getBaseQuery().pathExists(parent, child.getId(), BaseEdge.EdgeType.A_READ.toString()),
                q.getBaseQuery().pathExists(parent, child.getId(), BaseEdge.EdgeType.A_WRITE.toString()),
                q.getBaseQuery().pathExists(parent, child.getId(), BaseEdge.EdgeType.A_MANAGE.toString()),
                q.getBaseQuery().pathExists(parent, child.getId(), BaseEdge.EdgeType.A_CREATE_CHILD.toString()));
    }

    public Set<BaseEdge.EdgeType> userPermissionsOnGroup(BaseVertex.VertexType type, String userPrincipal, String groupName) throws UserNotFoundException, VertexNotFoundException {
        final FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        // find the user vertex
        final User owner = findAndRetrieveUserById(type, userPrincipal, framedGraph);

        // find the group group
        Iterator<Group> groups = framedGraph.query().has(Group.GROUP_NAME, groupName).limit(1).vertices(Group.class).iterator();
        if (!groups.hasNext()) {
            graph.rollback();
            throw new VertexNotFoundException("Cannot find the group: " + groupName);
        }
        Group group = groups.next();

        Set<BaseEdge.EdgeType> edges = new HashSet<>();
        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        for (BaseEdge.EdgeType edge : BaseEdge.EdgeType.values()) {
            if (q.getBaseQuery().pathExists(owner.asVertex(), group.asVertex().getId(), edge.toString())) {
                edges.add(edge);
            }
        }
        graph.commit();

        return edges;
    }

    @Override
    public boolean pathExists(Vertex source, Vertex target, String... edgeLabel) {
        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        return q.getBaseQuery().pathExists(source, target, edgeLabel);
    }

    /**
     * Finds and returns an User-vertex of the given type, by the given ID.
     *
     * @param userType type of user, USER or APP_USER
     * @param id principal/id of the user to retrieve
     * @param framedGraph graph to search for the user
     * @return an User-vertex by the given ID
     * @throws UserNotFoundException if the user cannot be found
     */
    private User findAndRetrieveUserById(BaseVertex.VertexType userType, String id, FramedGraph<TitanGraph> framedGraph)
            throws UserNotFoundException {
        Iterator<User> ownerit =
                framedGraph.query().limit(1).has(User.TYPE, userType.toString()).has(User.PRINCIPAL, id)
                        .vertices(User.class).iterator();

        if (!ownerit.hasNext()) {
            graph.rollback();
            final String errMsg = String.format(String.format("User: '%s' of type: '%s' not found.", id, userType));
            logger.error(errMsg);
            throw new UserNotFoundException(errMsg);
        }

        return ownerit.next();
    }

    /**
     * App Access groups cannot have children, this will return true if it is a child group of App access
     *
     * @param group the group that is being checked
     * @return true if the group is a child of app access
     */
    protected boolean isAppAccessGroup(Vertex group) {
        return group != null && new GremlinPipeline<Vertex, Vertex>(group)
                .inE(Group.CHILD_GROUP)
                .outV()
                .has("id", appAccessGroupId).hasNext();
    }

    /**
     * Helper function that will assign the various different edges between two vertices
     *  @param parent Vertex that will be the source of the edges
     * @param child Vertex that will be the destination of the edges
     * @param dataAccess if true, assign a data access edge
     * @param a_read if true, assign an admin read edge
     * @param a_write if true, assign an admin write edge
     * @param a_manage if true, assign an admin manage edge
     * @param a_create_child if true, assign an admin create child edge
     */
    protected void assignEdges(final Vertex parent, final Vertex child, boolean dataAccess, boolean a_read, boolean a_write, boolean a_manage, boolean a_create_child) {
        if (dataAccess) {
            parent.addEdge(BaseEdge.EdgeType.DATA_ACCESS.toString(), child);
        } else {
            removeSpecificEdge(parent.getId(), child.query().labels(BaseEdge.EdgeType.DATA_ACCESS.toString()).direction(Direction.IN).edges());
        }
        if (a_create_child) {
            parent.addEdge(BaseEdge.EdgeType.A_CREATE_CHILD.toString(), child);
        } else {
            removeSpecificEdge(parent.getId(), child.query().labels(BaseEdge.EdgeType.A_CREATE_CHILD.toString()).direction(Direction.IN).edges());
        }
        if (a_manage) {
            parent.addEdge(BaseEdge.EdgeType.A_MANAGE.toString(), child);
        } else {
            removeSpecificEdge(parent.getId(), child.query().labels(BaseEdge.EdgeType.A_MANAGE.toString()).direction(Direction.IN).edges());
        }
        if (a_read) {
            parent.addEdge(BaseEdge.EdgeType.A_READ.toString(), child);
        } else {
            removeSpecificEdge(parent.getId(), child.query().labels(BaseEdge.EdgeType.A_READ.toString()).direction(Direction.IN).edges());
        }
        if (a_write) {
            parent.addEdge(BaseEdge.EdgeType.A_WRITE.toString(), child);
        } else {
            removeSpecificEdge(parent.getId(), child.query().labels(BaseEdge.EdgeType.A_WRITE.toString()).direction(Direction.IN).edges());
        }
    }

    private void removeSpecificEdge(Object id, Iterable<Edge> edges) {
        for (Edge e : edges) {
            if (e.getVertex(Direction.OUT).getId().equals(id)) {
                logger.debug("Removing edge: {} {}", e, e.getLabel());
                graph.removeEdge(e);
            }
        }
    }

    protected Iterable<Edge> getAllEdgesBetweenVertices(final Vertex parent, final Vertex child) {
        return new GremlinPipeline<Vertex, Edge>(parent)
                .outE(BaseEdge.EdgeType.DATA_ACCESS.toString(), BaseEdge.EdgeType.A_CREATE_CHILD.toString(),
                        BaseEdge.EdgeType.A_MANAGE.toString(), BaseEdge.EdgeType.A_READ.toString(),
                        BaseEdge.EdgeType.A_WRITE.toString())
                .as("edges")
                .inV()
                .has("id", child.getId())
                .back("edges")
                .cast(Edge.class);
    }

    protected void removeEdges(final Vertex parent, final Vertex child, boolean dataAccess, boolean a_create_child,
                             boolean a_manage, boolean a_read, boolean a_write) {
        Iterable<Edge> edges = getAllEdgesBetweenVertices(parent, child);
        for (Edge e : edges) {
            BaseEdge.EdgeType edgeType = BaseEdge.EdgeType.valueOf(e.getLabel());
            boolean needRemove = false;

            switch (edgeType) {
                case DATA_ACCESS:
                    needRemove = dataAccess;
                    break;
                case A_CREATE_CHILD:
                    needRemove = a_create_child;
                    break;
                case A_MANAGE:
                    needRemove = a_manage;
                    break;
                case A_WRITE:
                    needRemove = a_write;
                    break;
                case A_READ:
                    needRemove = a_read;
                    break;
            }

            // Remove edges that need it
            if (needRemove) {
                graph.removeEdge(e);
            }
        }
        graph.commit();
    }

    //TODO: test me.
    protected void manageEdges(final Vertex parent, final Vertex child, boolean dataAccess, boolean a_create_child,
                             boolean a_manage, boolean a_read, boolean a_write) {
        // Get all edges that currently exist between them
        Iterable<Edge> edges = new GremlinPipeline<Vertex, Edge>(parent)
                .outE(BaseEdge.EdgeType.DATA_ACCESS.toString(), BaseEdge.EdgeType.A_CREATE_CHILD.toString(),
                        BaseEdge.EdgeType.A_MANAGE.toString(), BaseEdge.EdgeType.A_READ.toString(),
                        BaseEdge.EdgeType.A_WRITE.toString())
                .as("edges")
                .inV()
                .has("id", child.getId())
                .back("edges")
                .cast(Edge.class);


        // This list will contain only edges that need to be created once the following loop is complete
        List<BaseEdge.EdgeType> needCreate = new ArrayList<>(Arrays.asList((BaseEdge.EdgeType.values())));

        // Remove ones that shouldn't exist, and remove edges from needCreate if they already exist (and should)
        for (Edge e : edges) {
            BaseEdge.EdgeType edgeType = BaseEdge.EdgeType.valueOf(e.getLabel());
            boolean needRemove = false;

            switch (edgeType) {
                case DATA_ACCESS:
                    needRemove = !dataAccess;
                    break;
                case A_CREATE_CHILD:
                    needRemove = !a_create_child;
                    break;
                case A_MANAGE:
                    needRemove = !a_manage;
                    break;
                case A_WRITE:
                    needRemove = !a_write;
                    break;
                case A_READ:
                    needRemove = !a_read;
                    break;
            }

            // Remove edges that need it
            if (needRemove) {
                graph.removeEdge(e);
            } else {
                needCreate.remove(edgeType);
            }
        }

        // Add those edges that didn't exist
        for (BaseEdge.EdgeType type : needCreate) {
            parent.addEdge(type.toString(), child);
        }
        graph.commit();
    }


    @Override
    public Set<Long> getAuthorizations(BaseVertex.VertexType userType, String userId, List<String> appFilter) throws GroupQueryException {
        return new AuthorizationQuery(this).execute(userType, userId, appFilter);
    }



    public Set<BaseEdge.EdgeType> edgesBetweenVertices(Vertex source, final Vertex destination, BaseEdge.EdgeType... edgeLabels) {
        Set<BaseEdge.EdgeType> edges = new HashSet<>();
        final GroupQuery q = new GroupQuery(framedGraphFactory.create(this.graph));

        for (BaseEdge.EdgeType edgeLabel : edgeLabels) {
            logger.trace("Checking edge between {} and {}", source, destination);
            if (q.getBaseQuery().pathExists(source, destination.getId(), edgeLabel.toString())) {
                logger.trace("Path existed {}", edgeLabel);
                edges.add(edgeLabel);
            }
        }

        return edges;
    }
}
