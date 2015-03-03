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

package ezbake.security.persistence.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * User: jhastings
 * Date: 10/7/13
 * Time: 4:15 PM
 */
public class FileBackedRegistration {
    private String securityId;
    private String level;
    private Set<String> authorizations;
    private Set<String> communityAuthorizations;
    private String publicKey;
    private String appDn;
    
    public FileBackedRegistration() {
        this.securityId = "";
        this.level = "";
        this.authorizations = new HashSet<>();
        this.publicKey = "";
        this.appDn = "";
    }
    
    public void setAppDn(String appDn) {
    	this.appDn = appDn;
    }
    
    public String getAppDn() {
    	return this.appDn;
    }
    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        if (securityId != null)
            this.securityId = securityId;
        else
            this.securityId = "";
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

    public Set<String> getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(Set<String> authorizations) {
        if (authorizations != null)
            this.authorizations = authorizations;
        else
            this.authorizations = new HashSet<String>();
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        if (publicKey != null)
            this.publicKey = publicKey;
        else
            this.publicKey = "";
    }

    public Set<String> getCommunityAuthorizations() {
        return (communityAuthorizations == null) ? Collections.<String>emptySet() : communityAuthorizations;
    }
}
