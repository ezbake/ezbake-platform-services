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

import com.google.common.collect.Lists;

import java.util.*;

/**
 * User: jhastings
 * Date: 10/3/13
 * Time: 11:25 AM
 */
public class User {
    private long notValidAfter;

    private String principal;
    private String name;
    private String firstName;
    private String surName;
    private String uid;
    private String company;
    private String phoneNumber;
    private Map<String, String> emails;
    private String organization;
    private List<String> affiliations;
    private Authorizations authorizations;
    private Map<String, List<String>> groups;
    private List<Community> communities;

    public User() {
        this.name = "";
        this.firstName = "";
        this.surName = "";
        this.uid = "";
        this.company = "";
        this.phoneNumber = "";
        this.emails = new HashMap<>();
        this.authorizations = new Authorizations();
        this.groups = new HashMap<>();
        this.communities = new ArrayList<>();
        this.affiliations = Lists.newArrayList();
    }

    public User(User other) {
        this.principal = other.principal;
        this.name = other.name;
        this.firstName = other.firstName;
        this.surName = other.surName;
        this.uid = other.uid;
        this.company = other.company;
        this.phoneNumber = other.phoneNumber;
        this.emails = new HashMap<>(other.emails);
        this.authorizations = new Authorizations(other.authorizations);
        this.groups = new HashMap<>(other.groups);
        this.communities = new ArrayList<>(other.communities);
        this.organization = other.organization;
        this.affiliations = Lists.newArrayList(other.affiliations);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Principal: ").append(principal);
        sb.append(", Name: ").append(name);
        sb.append(", Uid: ").append(uid);
        sb.append(", company: ").append(company);
        sb.append(", phoneNumber: ").append(phoneNumber);
        sb.append(", emails: ").append(emails);
        sb.append(", Authorizations: ").append(authorizations);
        sb.append(", Projects: ").append(groups);
        return sb.toString();
    }

    public long getNotValidAfter() {
        return notValidAfter;
    }

    public void setNotValidAfter(long timestamp) {
        notValidAfter = timestamp;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name != null) {
            this.name = name;
        } else {
            this.name = "";
        }
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        if (firstName != null) {
            this.firstName = firstName;
        } else {
            this.firstName = "";
        }
    }

    public String getSurName() {
        return surName;
    }

    public void setSurName(String surName) {
        if (surName != null) {
            this.surName = surName;
        } else {
            this.surName = "";
        }
    }

     public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        if (uid != null)
            this.uid = uid;
        else
            this.uid = "";
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        if (company == null) {
            company = "";
        }
        this.company = company;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        if (phoneNumber == null) {
            phoneNumber = "";
        }
        this.phoneNumber = phoneNumber;
    }

    public Map<String, String> getEmails() {
        return emails;
    }

    public void setEmails(Map<String, String> emails) {
        if (emails == null) {
            emails = new HashMap<String, String>();
        }
        this.emails = new HashMap<String, String>();
        for (String key : emails.keySet()) {
            this.emails.put(String.valueOf(key), emails.get(key));
        }
    }

    public Map<String, List<String>> getProjects() {
        return groups;
    }

    public void setProjects(Map<String, List<String>> groups) {
        if (groups != null)
            this.groups = groups;
        else
            this.groups = new HashMap<String, List<String>>();
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(Authorizations authorizations) {
        if (authorizations != null)
            this.authorizations = authorizations;
        else
            this.authorizations = new Authorizations();
    }

    public List<Community> getCommunities() {
        return communities;
    }

    public void setCommunities(List<Community> communities) {
        if (communities != null)
            this.communities = communities;
        else
            this.communities = new ArrayList<Community>();
    }

    public String getOrganization() {
        return (organization == null) ? getAuthorizations().getOrganization() : organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public List<String> getAffiliations() {
        return (affiliations == null) ? Collections.<String>emptyList() : affiliations;
    }

    public void setAffiliations(List<String> affiliations) {
        this.affiliations = affiliations;
    }



}
