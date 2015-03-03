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

import ezbake.groups.thrift.UserGroupPermissions;

/**
 * User: jhastings
 * Date: 7/29/14
 * Time: 3:35 PM
 */
public class UserGroupPermissionsWrapper extends UserGroupPermissions {
    public UserGroupPermissionsWrapper() {
        super();
    }

    public UserGroupPermissionsWrapper(UserGroupPermissions other) {
        super(other);
    }

    public UserGroupPermissionsWrapper(boolean dataAccess, boolean adminRead, boolean adminWrite, boolean adminManage,
                                       boolean adminCreateChild) {
        super();
        setDataAccess(dataAccess);
        setAdminRead(adminRead);
        setAdminWrite(adminWrite);
        setAdminManage(adminManage);
        setAdminCreateChild(adminCreateChild);
    }

    public static UserGroupPermissions ownerPermissions() {
        return new UserGroupPermissionsWrapper(true, true, true, true, true);
    }
}
