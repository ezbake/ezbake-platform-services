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

package ezbake.security.service.policy;

import ezbake.base.thrift.CommunityMembership;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.api.ua.Community;
import ezbake.security.api.ua.User;
import ezbake.security.persistence.model.AppPersistenceModel;

import java.util.*;

/**
 * User: jhastings
 * Date: 5/12/14
 * Time: 11:26 AM
 */
public class SimplePolicy extends AuthorizationPolicy {
    public SimplePolicy(Properties ezConfiguration) {
        super(ezConfiguration);
    }


    @Override
    public void populateTokenForUser(final EzSecurityToken token, final User user) {
        token.setAuthorizationLevel(user.getAuthorizations().getLevel());
        token.setCitizenship(user.getAuthorizations().getCitizenship());
        token.setOrganization(user.getAuthorizations().getOrganization());

        if (token.getExternalProjectGroups() == null) {
            token.setExternalProjectGroups(new HashMap<String, List<String>>());
        }
        for (Map.Entry<String, List<String>> entry : user.getProjects().entrySet()) {
            token.getExternalProjectGroups().put(entry.getKey(), entry.getValue());
        }

        if (token.getExternalCommunities() == null) {
            token.setExternalCommunities(new HashMap<String, CommunityMembership>());
        }
        for (Community c : user.getCommunities()) {
            CommunityMembership cm = new CommunityMembership();
            cm.setType(c.getCommunityType());
            cm.setName(c.getCommunityName());
            cm.setOrganization(c.getOrganization());
            cm.setGroups(c.getGroups());
            cm.setRegions(c.getRegions());
            cm.setTopics(c.getTopics());
            cm.setFlags(c.getFlags());
            token.getExternalCommunities().put(cm.getName(), cm);
        }
    }

    @Override
    public Set<String> authorizationsForUser(User user) {
        Set<String> auths = new TreeSet<String>();

        auths.addAll(user.getAuthorizations().getAuths());
        auths.add(user.getAuthorizations().getCitizenship());
        auths.add(user.getAuthorizations().getLevel());
        auths.add(user.getAuthorizations().getOrganization());

        return auths;
    }

    @Override
    public Set<String> authorizationsForApp(AppPersistenceModel app) {
        Set<String> auths = new TreeSet<String>();

        auths.addAll(app.getFormalAuthorizations());
        auths.add(app.getAuthorizationLevel());

        return auths;
    }

    @Override
    public Set<String> externalCommunityAuthorizationsForUser(User user) {
        return user.getAuthorizations().getCommunityAuthorizations();
    }

}
