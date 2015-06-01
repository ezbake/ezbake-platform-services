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

package ezbake.profile.service;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.UserInfo;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.profile.*;
import ezbake.security.api.ua.SearchResult;
import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.api.ua.UserSearchService;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.profile.service.exe.ThreadedUserProfileExecutor;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.thrift.UserNotFoundException;
import ezbake.security.ua.UAModule;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 
 * @author Gary Drocella
 * @date 04/23/2014
 * Time: 1:32pm
 */
public class EzProfileHandler extends EzBakeBaseThriftService implements EzProfile.Iface {
    private Logger log = LoggerFactory.getLogger(EzProfileHandler.class);
    public static final String LOAD_LOCAL_PROPS = "ezprofile.prop.dir";

    public static final String FIRST = "first";
    public static final String LAST = "last";
    public static int QUERY_ALPHANUM_MIN = 3;

    private Properties ezConfig;
    private EzbakeSecurityClient ezClient;
    private UserSearchService searchService;
    private ThreadedUserProfileExecutor exe;

    public EzProfileHandler() {
        this.searchService = null;
        this.ezClient = null;
        this.exe = null;
    }

    @Inject
    public EzProfileHandler(Properties ezProperties, EzbakeSecurityClient securityClient,
                            UserAttributeService uaService, UserSearchService searchService) {
        this.ezConfig = ezProperties;
        this.ezClient = securityClient;
        this.searchService = searchService;
        this.exe = new ThreadedUserProfileExecutor(uaService, Runtime.getRuntime().availableProcessors());
    }

    List<EzProfileHandler> instances = new ArrayList<>();
    @Override
    public TProcessor getThriftProcessor() {
        Properties ezConfiguration = getConfigurationProperties();

        String appConfDir = ezConfiguration.getProperty(LOAD_LOCAL_PROPS);
        if (appConfDir != null) {
            File props = new File(appConfDir);
            if (props.exists() && props.isDirectory()) {
                log.info("Merging ezConfig with properties from dir: {}", appConfDir);
                EzConfiguration dirConfig = null;
                try {
                    dirConfig = new EzConfiguration(new DirectoryConfigurationLoader(props.toPath()));
                    ezConfiguration.putAll(dirConfig.getProperties());
                    log.info("Loaded EzProfile properties from directory: {}", appConfDir);
                } catch (EzConfigurationLoaderException e) {
                    log.error("Unable to load EzProfile properties from directory: {}", appConfDir);
                }
            }
        }

        EzProfileHandler instance = Guice.createInjector(
                new EzProfileModule(ezConfiguration),
                new UAModule(ezConfiguration)
        ) .getInstance(EzProfileHandler.class);
        instances.add(instance);
        return new EzProfile.Processor<>(instance);
    }

    @Override
    public void shutdown() {
        if (instances != null && instances.size() > 0) {
            Iterator<EzProfileHandler> instanceIterator = instances.iterator();
            while(instanceIterator.hasNext()) {
                instanceIterator.next().stopServices();
                instanceIterator.remove();
            }
        }
    }

    private void stopServices() {
        if (ezClient != null) {
            try {
                ezClient.close();
            } catch (IOException e) {
                log.error("Unable to close the EzSecurity Client");
            }
        }
        if (exe != null) {
            EzSecurityRedisCache cache = exe.getUaService().getCache();
            if (cache != null) {
                try {
                    cache.close();
                } catch (IOException e) {
                    log.error("Unable to close the UAService cache");
                }
            }
        }
    }

    /**********************************************************/
    /**              Thrift Service Methods                  **/
    /**********************************************************/

    @Override
    public boolean ping() {
        return true;
    }

    @Override
    public ezbake.profile.SearchResult searchDnByName(EzSecurityToken ezToken, String first, String last) throws TException {
        log.info("searchDnByName request from {} for {} {}", ezToken.getValidity().getIssuedTo(), first, last);
        validateEzSecurityToken(ezToken);

        SearchResult data = searchService.search(first, last);

        return toExternal(data);
    }


    @Override
    public ezbake.profile.SearchResult searchDnByQuery(EzSecurityToken ezToken, String query) throws TException, MalformedQueryException {
        log.info("searchDnByQuery request received from {} query {}", ezToken.getValidity().getIssuedTo(), query);
        validateEzSecurityToken(ezToken);

        if(this.countAlphaNumericChars(query) < QUERY_ALPHANUM_MIN) {
            throw new MalformedQueryException("Expecting a minimum of at least " + QUERY_ALPHANUM_MIN +
                        " alphanumeric characters in entire query string.");
        }

        Map<String, String> nameMap = parseQuery(query);

        SearchResult data = searchService.search(nameMap.get(FIRST), nameMap.get(LAST));

        return toExternal(data);
    }


    @Override
    public ezbake.profile.SearchResult searchProfileByName(EzSecurityToken ezToken, String first, String last) throws TException {
        log.info("searchProfileByName request from {} for First: {}, Last: {}", ezToken.getValidity().getIssuedTo(),
                first, last);
        validateEzSecurityToken(ezToken);

        SearchResult result = searchService.search(first, last);
        ezbake.profile.SearchResult resultUp = new ezbake.profile.SearchResult();

        if(!result.isError()) {
            Map<String,User> map = exe.execute(result.getData());
            resultUp.setProfiles(usersToUserProfiles(map));
            resultUp.setResultOverflow(result.isResultOverflow());
            resultUp.setStatusCode(SearchStatus.OK);
        }
        else {
            resultUp.setStatusCode(SearchStatus.ERROR);
        }

        return resultUp;
    }


    @Override
    public ezbake.profile.SearchResult searchProfileByQuery(EzSecurityToken ezToken, String query) throws TException, MalformedQueryException {
        log.info("searchProfileByQuery request received from {} for {}", ezToken.getValidity().getIssuedTo(), query);
        validateEzSecurityToken(ezToken);

        Map<String,String> nameMap = parseQuery(query);
        SearchResult result = searchService.search(nameMap.get(FIRST), nameMap.get(LAST));
        ezbake.profile.SearchResult resultUp = new ezbake.profile.SearchResult();

        if(!result.isError()) {
            Map<String,User> map = exe.execute(result.getData());
            resultUp.setProfiles(usersToUserProfiles(map));
            resultUp.setResultOverflow(result.isResultOverflow());
            resultUp.setStatusCode(SearchStatus.OK);
        }
        else {
            resultUp.setStatusCode(SearchStatus.ERROR);
        }

        return resultUp;
    }


    @Override
    public UserInfo userProfile(EzSecurityToken ezSecurityToken, String principal) throws TException {
        if (Strings.isNullOrEmpty(principal)) {
            principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
        }
        UserInfo profile = userProfiles(ezSecurityToken, Sets.newHashSet(principal)).get(principal);
        if (profile == null) {
            throw new UserNotFoundException("No user profile found for user " + principal);
        }

        return profile;
    }


    @Override
    public Map<String, UserInfo> userProfiles(EzSecurityToken ezSecurityToken, Set<String> principals) throws TException {
       log.info("userProfiles request from {} for users: {}", ezSecurityToken.getValidity().getIssuedTo(), principals);
        validateEzSecurityToken(ezSecurityToken);

        Map<String, User> profiles = exe.execute(principals);

        return usersToUserInfo(profiles);
    }

    @Override
    public UserProfile getUserProfile(EzSecurityToken ezSecurityToken, String principal) throws EzSecurityTokenException, UserNotFoundException, TException {
        if (Strings.isNullOrEmpty(principal)) {
            principal = ezSecurityToken.getTokenPrincipal().getPrincipal();
        }
        UserProfile profile = getUserProfiles(ezSecurityToken, Sets.newHashSet(principal)).get(principal);
        if (profile == null) {
            throw new UserNotFoundException("No user profile found for user " + principal);
        }

        return profile;
    }

    @Override
    public Map<String, UserProfile> getUserProfiles(EzSecurityToken ezSecurityToken, Set<String> principals) throws EzSecurityTokenException, UserNotFoundException, TException {
        log.info("userProfiles request from {} for users: {}", ezSecurityToken.getValidity().getIssuedTo(), principals);
        validateEzSecurityToken(ezSecurityToken);
        return usersToUserProfiles(exe.execute(principals));
    }


    /**********************************************************/
    /**                       Helpers                        **/
    /**********************************************************/


    private Map<String,String> parseQuery(String query) throws MalformedQueryException {
        Map<String,String> map = new TreeMap<String,String>();
        StringTokenizer tok = new StringTokenizer(query);

        if(tok.countTokens() > 2) {
            throw new MalformedQueryException("Expecting query composed of at most two tokens delimited by white space.");
        }

        if(tok.hasMoreTokens()) {
            String first = tok.nextToken();
            String last = "*";

            if(tok.hasMoreTokens()) {
                last = tok.nextToken();

            }

            map.put(FIRST, first);
            map.put(LAST, last);
        }

        return map;
    }


    private int countAlphaNumericChars(String str) {
        int count = 0;

        int len = str.length();
        for(int i = 0; i < len; i++) {
            String c = str.substring(i, i+1);
            if(c.matches("[A-Za-z0-9]")) {
                count++;
            }
        }

        return count;
    }


    /**
     * Perform validation steps on an EzSecurityToken - additional checks can be added if methods need special access.
     * Will throw an EzSecurityTokenException if there are any validation or other problems
     *
     * @param token an EzSecurityToken received in the request
     * @throws EzSecurityTokenException
     */
    private void validateEzSecurityToken(final EzSecurityToken token) throws EzSecurityTokenException {
        log.info("Validating EzSecurityToken: {}", token.getValidity());
        try {
            ezClient.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            log.error("Invalid EzSecurityToken received: {}", token.getValidity(), e);
            throw e;
        }
    }



    /**********************************************************/
    /**                  Static Methods                      **/
    /**********************************************************/


    private static ezbake.profile.SearchResult toExternal(SearchResult result) {
        ezbake.profile.SearchResult dn = new ezbake.profile.SearchResult();

        dn.setPrincipals(result.getData());
        dn.setResultOverflow(result.isResultOverflow());
        if (result.getStatusCode() == 0) {
            dn.setStatusCode(SearchStatus.OK);
        } else {
            dn.setStatusCode(SearchStatus.ERROR);
        }

        return dn;
    }


    private static Map<String, UserProfile> usersToUserProfiles(Map<String, User> users) {
        Map<String, UserProfile> profiles = new HashMap<>();
        for (Map.Entry<String, User> entry : users.entrySet()) {
            String key = entry.getKey();
            User u = entry.getValue();

            UserProfile up = new UserProfile(u.getPrincipal(), u.getUid());
            up.setFirstName(u.getFirstName());
            up.setLastName(u.getSurName());
            up.setCompanyName(u.getCompany());
            up.setPhoneNumber(u.getPhoneNumber());
            up.setEmails(u.getEmails());
            up.setOrganization(u.getOrganization());
            up.setAffiliations(u.getAffiliations());
            profiles.put(key, up);
        }
        return profiles;
    }


    private static Map<String, UserInfo> usersToUserInfo(Map<String, User> users) {
        Map<String, UserInfo> profiles = new HashMap<>();
        for (Map.Entry<String, User> entry : users.entrySet()) {
            String key = entry.getKey();
            User u = entry.getValue();

            UserInfo up = new UserInfo(u.getPrincipal());

            up.setId(u.getUid());
            up.setName(u.getName());
            up.setFirstName(u.getFirstName());
            up.setLastName(u.getSurName());
            up.setCompany(u.getCompany());
            up.setEmails(u.getEmails());
            up.setPhoneNumbers(new HashMap<String, String>());
            up.getPhoneNumbers().put("IC", u.getPhoneNumber());
            if (u.getAuthorizations() != null) {
                up.setCitizenship(u.getAuthorizations().getCitizenship());
                up.setOrganization(u.getAuthorizations().getOrganization());
            }

            profiles.put(key, up);
        }

        return profiles;
    }
}
