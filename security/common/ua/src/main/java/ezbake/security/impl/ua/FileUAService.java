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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserNotFoundException;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.service.sync.NoopRedisCache;
import ezbake.security.common.core.FileWatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * User: jhastings
 * Date: 4/29/14
 * Time: 8:56 AM
 */
public class FileUAService implements UserAttributeService, FileWatcher.FileWatchUpdater {
    private static final Logger log = LoggerFactory.getLogger(FileUAService.class);
    public static final String userFile = "users.json";
    public static final String USERS_FILENAME = "ezbake.security.service.user.file";

    private FileWatcher watchThread;
    protected Map<String, User> users = Collections.emptyMap();
    protected ReadWriteLock userLock;

    NoopRedisCache cache;
    
    public FileUAService() {
        this(new File(userFile));
    }

    @Inject
    public FileUAService(Properties ezConfiguration) {
        this(new File(ezConfiguration.getProperty(USERS_FILENAME, userFile)));
    }

    public FileUAService(File userFile) {
        userLock = new ReentrantReadWriteLock();
        try {
            loadUpdate(new FileInputStream(userFile));
        } catch (FileNotFoundException e) {
            log.info("Users file not found at start-up. Polling will continue watching for file: {}",
                    userFile);
        }
        
        cache = new NoopRedisCache();

        // Start the file watching daemon thread
        watchThread = new FileWatcher(userFile.toPath(), this);
        new Thread(watchThread).start();
    }

    public static Map<String, User> loadJSON(InputStream is) {
        Map<String, User> users = new HashMap<>();
        Gson gson = new Gson();
        try {
            users = gson.fromJson(new InputStreamReader(is), new TypeToken<Map<String, User>>(){}.getType());
            if (users == null) {
                users = new HashMap<>();
            }

            for (Map.Entry<String, User> entry : users.entrySet()) {
                String id = entry.getKey();
                User user = entry.getValue();
                user.setPrincipal(id);
                List<String> split = ImmutableList.copyOf(Splitter.on(' ')
                        .omitEmptyStrings()
                        .trimResults()
                        .split(user.getName()));
                if (split.size() == 2) {
                    user.setFirstName(split.get(0));
                    user.setSurName(split.get(1));
                }
            }
        } catch (ClassCastException e) {
            log.warn("Users file contained the wrong YAML data", e);
        }
        return users;
    }

    private User getUserOrThrow(String principal) throws UserNotFoundException {
        userLock.readLock().lock();
        try {
            User u = users.get(principal);
            if (u == null) {
                throw new UserNotFoundException("No user found for principal: " + principal);
            }

            // Make sure user principal is set
            u.setPrincipal(principal);

            return new User(u);
        } finally {
            userLock.readLock().unlock();
        }
    }

    @Override
    public boolean assertUserStrictFailure(String principal) {
        return assertUser(principal);
    }

    @Override
    public boolean assertUser(String principal) {
        userLock.readLock().lock();
        try {
            return this.users.containsKey(principal);
        } finally {
            userLock.readLock().unlock();
        }
    }

    @Override
    public User getUser(String principal) throws UserNotFoundException {
        return getUserOrThrow(principal);
    }

    @Override
    public User getUserProfile(String principal) throws UserNotFoundException {
        User u = getUserOrThrow(principal);
        u.setCommunities(null);
        u.setAuthorizations(null);
        u.setProjects(null);
        return u;
    }

    @Override
    public Map<String, List<String>> getUserGroups(String principal) throws UserNotFoundException {
        User u = getUserOrThrow(principal);
        return u.getProjects();
    }

    @Override
    public boolean loadUpdate(InputStream is) {
        userLock.writeLock().lock();
        try {
            this.users = loadJSON(is);
        } finally {
            userLock.writeLock().unlock();
        }
        return true;
    }

    @Override
    public void close() {
        if (this.watchThread != null) {
            this.watchThread.stopWatching();
        }
    }


    @Override
    public EzSecurityRedisCache getCache() {
        return this.cache;
    }
}
