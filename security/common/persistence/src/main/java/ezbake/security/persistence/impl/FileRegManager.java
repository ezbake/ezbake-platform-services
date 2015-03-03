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

package ezbake.security.persistence.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbake.security.common.core.FileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.parser.ParserException;

import javax.inject.Inject;
import java.io.*;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.util.*;

/**
 * User: jhastings
 * Date: 12/18/14
 * Time: 2:46 PM
 */
public class FileRegManager implements RegistrationManager, FileWatcher.FileWatchUpdater {

    private static final Logger logger = LoggerFactory.getLogger(FileRegManager.class);
    public static final String REGISTRATION_FILE = "ezsecurity_apps.yaml";
    public static final String REGISTRATION_FILE_PATH = "ezbake.security.service.registration.file";

    private String filePath;
    private FileWatcher watchThread;
    private Map<String, AppPersistenceModel> registrationMap = Maps.newConcurrentMap();
    private boolean shouldLoad = true; // this is a toggle that gets set false by save and true by load

    @Inject
    public FileRegManager(Properties properties) {
        filePath = properties.getProperty(REGISTRATION_FILE_PATH, REGISTRATION_FILE);

        try {
            FileInputStream initialRegistrationIn = new FileInputStream(new File(filePath));
            loadUpdate(initialRegistrationIn);
        } catch (FileNotFoundException e) {
            // Initially, try reading from the root of the classpath
            logger.debug("Trying {}", "/" + REGISTRATION_FILE);
            InputStream classpathInputStream = FileRegManager.class.getResourceAsStream("/"+REGISTRATION_FILE);
            if (classpathInputStream != null) {
                loadUpdate(classpathInputStream);
            } else {
                logger.info("Registrations file not found at start-up. Polling will continue watching for file: {}",
                        filePath);
            }
        }

        // Start the file watching daemon thread
        watchThread = new FileWatcher(Paths.get(filePath), this);
        new Thread(watchThread).start();
    }

    public static Map<String, AppPersistenceModel> loadStream(InputStream is) {
        Map<String, AppPersistenceModel> apps = Maps.newHashMap();

        if (is != null) {
            Yaml yaml = new Yaml();
            try {
                List<AppPersistenceModel> loaded = (List<AppPersistenceModel>) yaml.load(is);
                if (loaded != null) {
                    for (AppPersistenceModel app : loaded) {
                        apps.put(app.getId(), app);
                        logger.debug("Loaded app: {}", app);
                    }
                }
            } catch (ParserException e) {
                logger.debug("Apps parse exception", e);
                logger.warn("EzSecurity Apps file contained invalid YAML. No apps were loaded.");
            } catch (ClassCastException e) {
                logger.warn("EzSecurity Apps file contained invalid YAML. It must be an array. {}", e.getMessage());
            } catch (Exception e) {
                logger.error("Unexpected error parsing apps yaml file: {}", e.getMessage());
            }
        }

        return apps;
    }

    /**
     * Write the registrations back out to the file
     */
    public void save() throws IOException {
        synchronized (getClass()) {
            // Before doing anything we need to acquire the file lock
            FileLock saveLock = lock(new RandomAccessFile(filePath, "rw"));
            if (saveLock == null) {
                throw new IOException("Failed to get a write lock for: " + filePath);
            }
            try {
                // Load first then save
                loadUpdate(new FileInputStream(filePath));
                List<AppPersistenceModel> apps = Lists.newArrayList(registrationMap.values());
                new Yaml().dump(apps, new FileWriter(new File(filePath)));
                logger.debug("Saved apps to file: {}", filePath);
            } finally {
                saveLock.release();
                shouldLoad = false; // The next time it won't load an update
            }
        }
    }

    private static FileLock lock(RandomAccessFile file) throws IOException {
        return file.getChannel().tryLock();
    }

    private AppPersistenceModel get(String id) throws SecurityIDNotFoundException {
        AppPersistenceModel app = registrationMap.get(id);
        if (app == null) {
            throw new SecurityIDNotFoundException("No app found with id: " + id);
        }
        return app;
    }

    private void put(AppPersistenceModel model) {
        registrationMap.put(model.getId(), model);
        try {
            save();
        } catch (IOException e) {
            logger.warn("Failed to save apps file after put");
        }
    }

    private void remove(String id) {
        registrationMap.remove(id);
        try {
            save();
        } catch (IOException e) {
            logger.warn("Failed to save apps file after remove");
        }
    }

    @Override
    public void register(String id, String owner, String appName, String visibilityLevel, List<String> visibility, Set<String> admins, String appDn) throws RegistrationException {
        AppPersistenceModel app = new AppPersistenceModel();
        app.setId(id);
        app.setAppName(appName);
        app.setOwner(owner);
        app.setAuthorizationLevel(visibilityLevel);
        app.setFormalAuthorizations(visibility);
        app.setStatus(RegistrationStatus.PENDING);
        app.setAdmins(admins);
        app.setAppDn(appDn);
        register(app);
    }

    @Override
    public void register(AppPersistenceModel appPersistenceModel) throws RegistrationException {
        put(appPersistenceModel);
    }

    @Override
    public void approve(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        get(id).setStatus(RegistrationStatus.ACTIVE);
    }

    @Override
    public void deny(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        get(id).setStatus(RegistrationStatus.DENIED);
    }

    @Override
    public void delete(String[] auths, String id) throws RegistrationException, SecurityIDNotFoundException {
        remove(id);
    }

    @Override
    public void update(AppPersistenceModel registration, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        registration.setStatus(status);
        put(registration);
    }

    @Override
    public void setStatus(String id, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        get(id).setStatus(status);
    }

    @Override
    public void addAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException {
        get(id).getAdmins().add(admin);

    }

    @Override
    public void removeAdmin(String[] auths, String id, String admin) throws SecurityIDNotFoundException, RegistrationException {
        get(id).getAdmins().remove(admin);
    }

    @Override
    public List<AppPersistenceModel> all(String[] auths, String owner, RegistrationStatus status) throws RegistrationException {
        return Lists.newArrayList(registrationMap.values());
    }

    @Override
    public RegistrationStatus getStatus(String[] auths, String id) throws SecurityIDNotFoundException {
        return get(id).getStatus();
    }

    @Override
    public AppPersistenceModel getRegistration(String[] auths, String id, String owner, RegistrationStatus status) throws RegistrationException, SecurityIDNotFoundException {
        return get(id);
    }

    @Override
    public boolean containsId(String[] auths, String id) throws RegistrationException {
        return registrationMap.containsKey(id);
    }

    @Override
    public boolean loadUpdate(InputStream is) {
        if (!shouldLoad || is == null) {
            shouldLoad = true;
            return true;
        }
        Map<String, AppPersistenceModel> apps = loadStream(is);
        registrationMap.putAll(apps);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (this.watchThread != null) {
            this.watchThread.stopWatching();
        }
    }
}
