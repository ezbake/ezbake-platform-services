/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.security.persistence.api;

import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;

import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 5/2/14
 * Time: 10:47 AM
 */
public interface RegistrationManager {

    public void register(String id, String owner, String appName, String visibilityLevel, List<String> visibility, Set<String> admins, String appDn) throws RegistrationException;
    public void register(AppPersistenceModel appPersistenceModel) throws RegistrationException;
    public void approve(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException;
    public void deny(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException;
    public void delete(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException;
    public void update(AppPersistenceModel registration, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException;
    public void setStatus(String id, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException;
    public void addAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException;
    public void removeAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException;

    public List<AppPersistenceModel> all(String[] auths, String owner, RegistrationStatus status) throws RegistrationException;

    public RegistrationStatus getStatus(String[] auths, String id) throws SecurityIDNotFoundException;
    public AppPersistenceModel getRegistration(String[] auths, String id, String owner, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException;
    public boolean containsId(String[] auths, String id) throws RegistrationException;
}
