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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jneuberger on 12/10/13.
 */
public class Community {
    private String communityName;
    private String communityType;
    private String organization;
    private List<String> topics;
    private List<String> regions;
    private List<String> groups;
    private Map<String, Boolean> flags;

    public Community() {
        this.communityType = "";
        this.organization = "";
        this.topics = new ArrayList<String>();
        this.regions = new ArrayList<String>();
        this.groups = new ArrayList<String>();
    }

    public Community(Community other) {
        this.communityType = other.communityType;
        this.organization = other.organization;
        this.topics = new ArrayList<String>(other.topics);
        this.regions = new ArrayList<String>(other.regions);
        this.groups = new ArrayList<String>(other.groups);
    }

    public String getCommunityName() {
        return communityName;
    }

    public void setCommunityName(String communityName) {
        this.communityName = communityName;
    }

    public String getCommunityType() {
        return communityType;
    }

    public void setCommunityType(String communityType) {
        if (communityType != null)
            this.communityType = communityType;
        else
            this.communityType = "";
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

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getRegions() {
        return regions;
    }

    public void setRegions(List<String> regions) {
        this.regions = regions;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        if (groups != null)
            this.groups = groups;
        else
            this.groups = new ArrayList<String>();
    }

    public Map<String, Boolean> getFlags() {
        return flags;
    }

    public void setFlags(Map<String, Boolean> flags) {
        if (flags != null) {
            this.flags = flags;
        } else {
            this.flags = new HashMap<String, Boolean>();
        }
    }

}
