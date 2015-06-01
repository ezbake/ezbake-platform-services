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

package ezbake.security.api.ua;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * User: jhastings
 * Date: 10/3/13
 * Time: 11:09 AM
 */
public class Authorizations {
    private String level;
    private Set<String> auths;
    private Set<String> communityAuthorizations;
    private String citizenship;
    private String organization;

    public Authorizations() {
        this.level = "";
        this.auths = Sets.newTreeSet();
        this.communityAuthorizations = Sets.newTreeSet();
        this.citizenship = "";
        this.organization = "";
    }

    public Authorizations(Authorizations other) {
        this.level = other.level;
        this.auths = Sets.newTreeSet(other.auths);
        this.communityAuthorizations = Sets.newTreeSet(other.communityAuthorizations);
        this.citizenship = other.citizenship;
        this.organization = other.organization;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        if (level != null)
            this.level = level;
        else
            this.level = "";
    }

    public Set<String> getAuths() {
        return auths;
    }

    public void setAuths(Set<String> auths) {
        if (auths != null)
            this.auths = auths;
        else
            this.auths = Sets.newTreeSet();
    }

    public Set<String> getCommunityAuthorizations() {
        return communityAuthorizations;
    }

    public void setCommunityAuthorizations(Set<String> communityAuthorizations) {
        if (communityAuthorizations != null) {
            this.communityAuthorizations = communityAuthorizations;
        }
    }

    public String getCitizenship() {
        return citizenship;
    }

    public void setCitizenship(String citizenship) {
        if (citizenship != null)
            this.citizenship = citizenship;
        else
            this.citizenship = "";
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        if (organization != null)
            this.organization = organization;
        else
            this.organization = "";
    }
}
