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

package ezbake.security.impl.ua;

import com.google.common.base.CharMatcher;
import ezbake.security.api.ua.SearchResult;
import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * User: jhastings
 * Date: 8/26/14
 * Time: 8:03 AM
 */
public class FileUASearch extends FileUAService implements UserSearchService {
    private static final Logger logger = LoggerFactory.getLogger(FileUASearch.class);

    @Inject
    public FileUASearch(Properties ezProperties) {
        super(ezProperties);
    }

    public FileUASearch(File userFile) {
        super(userFile);
    }

    @Override
    public SearchResult search(String first, String last) {
        SearchResult result = new SearchResult();

        Pattern firstNamePattern = Pattern.compile(CharMatcher.anyOf("*").replaceFrom(first, "\\w*"));
        Pattern lastNamePattern = Pattern.compile(CharMatcher.anyOf("*").replaceFrom(last, "\\w*"));
        logger.debug("Searching for user match first: {} last: {}", firstNamePattern, lastNamePattern);

        userLock.readLock().lock();
        for (Map.Entry<String,User> u : users.entrySet()) {
            String id = u.getKey();
            User user = u.getValue();

            logger.debug("Checking match for {} {}", user.getFirstName(), user.getSurName());
            if (firstNamePattern.matcher(user.getFirstName()).matches() || lastNamePattern.matcher(user.getSurName()).matches()) {
                result.getData().add(id);
                logger.debug("Found match: {}", user.getPrincipal());
            }
        }
        userLock.readLock().unlock();

        return result;
    }

    @Override
    public SearchResult listGroupMembers(String groupName, String projectName) {
        SearchResult result = new SearchResult();

        userLock.readLock().lock();
        for (Map.Entry<String,User> u : users.entrySet()) {
            String id = u.getKey();
            User user = u.getValue();

            if (user.getProjects().containsKey(projectName) && user.getProjects().get(projectName).contains(groupName)) {
                result.getData().add(id);
            }
        }
        userLock.readLock().unlock();

        return result;
    }

}
