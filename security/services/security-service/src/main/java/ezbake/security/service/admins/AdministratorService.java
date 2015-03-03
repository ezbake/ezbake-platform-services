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

package ezbake.security.service.admins;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import ezbake.security.common.core.FileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.parser.ParserException;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * This class will load administrators from a file, and answer the question of whether or not a user is an administrator
 * Admins are expected to be in a YAML file of format:
 *     ----
 *     - Admin1
 *     - Admin2
 *
 * It does so by starting up a thread that reloads the file at a given interval. If it detects that the file has changed
 * (using md5 hash) the list of admins will be reloaded, and if anybody is watching for updates with the blocking:
 * getAdminUpdate is called.
 *
 * User: jhastings
 * Date: 3/25/14
 * Time: 8:11 AM
 */
public class AdministratorService implements FileWatcher.FileWatchUpdater {
    private static Logger log = LoggerFactory.getLogger(AdministratorService.class);

    protected FileWatcher watchThread;
    protected Set<String> admins = new HashSet<>();

    public AdministratorService() {
    }

    @Inject
    public AdministratorService(@Named("Admin File") String adminFile) {

        try {
            admins = loadAdministratorYaml(new FileInputStream(adminFile));
        } catch (FileNotFoundException e) {
            log.info("Administrator file not found at start-up. Polling will continue watching for file: {}",
                    adminFile);
        }

        // Start the file watching daemon thread
        watchThread = new FileWatcher(Paths.get(adminFile), this);
        new Thread(watchThread).start();
    }

    public static AdministratorService fromYAML(String adminYaml) {
        Preconditions.checkNotNull(adminYaml);

        AdministratorService administratorService = new AdministratorService();
        administratorService.loadUpdate(new ByteArrayInputStream(adminYaml.getBytes()));

        return administratorService;
    }

    protected static Set<String> loadAdministratorYaml(InputStream adminStream) {
        Set<String> admins = null;

        if (adminStream != null) {
            Yaml yaml = new Yaml();
            try {
                List<String> loaded = (List<String>)yaml.load(adminStream);
                if (loaded != null) {
                    admins = Sets.newHashSet(loaded);
                }
            } catch (ParserException e) {
                // YAML file was invalid... just leaving admins empty
                log.debug("Admin parse exception", e);
                log.warn("EzSecurity Administrators file contained invalid YAML. No administrators were loaded.");
            } catch (ClassCastException e) {
                log.warn("EzSecurity Administrators file contained invalid YAML. It must be an array. {}",e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error parsing admins yaml file: {}", e.getMessage());
            }
        }

        return admins;
    }

    /**
     * Determines whether or not the user is an administrator
     *
     * @param user user name to query
     * @return true if the user is an administrator
     */
    public boolean isAdmin(String user) {
        log.debug("Checking is admin: {}, admins: {}", user, admins);
        return admins != null && admins.contains(user);
    }

    /**
     * The number of administrators currently registered
     * @return the number of admins
     */
    public int numAdmins() {
        return (admins == null) ? 0 : admins.size();
    }

    /**
     * This updates the administrators that are cached in memory. However, if watching a file the file will
     * overload this
     *
     * @param admins new set of admins
     */
    public void setAdmins(Set<String> admins) {
        this.admins = admins;
    }

    public synchronized Set<String> getAdmins() {
        return admins;
    }

    public boolean hasAdmins() {
        return watchThread.hasFile();
    }

    /**
     * Load the administrators
     * If supported, an md5 sum of the inputstream is computed, and admins are only updated if it is different
     * This function is called by the watch thread
     *
     * @param is input stream containing the admin yaml
     * @return always returns true to continue watching
     */
    @Override
    public synchronized boolean loadUpdate(InputStream is) {
        // Load the file
        log.info("Loading update of administrators");
        Set<String> update = loadAdministratorYaml(is);
        if (update != null) {
            admins = update;
        }
        return true;
    }

    @Override
    public void close() {
        log.info("Stopping watch of admin file");
        if (watchThread != null && watchThread.isRunning()) {
            watchThread.stopWatching();
        }
    }

}
