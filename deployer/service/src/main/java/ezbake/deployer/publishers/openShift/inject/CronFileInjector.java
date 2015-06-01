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

package ezbake.deployer.publishers.openShift.inject;

import ezbake.deployer.impl.Files;
import ezbake.deployer.utilities.Utilities;

import java.io.File;
import java.util.List;

/**
 * This class gets the cron files that need to be injected from the classpath
 */
public class CronFileInjector extends ClasspathResourceInjector {
    public final static File OPENSHIFT_CRON_PATH = Files.get(".openshift", "cron", "daily");
    public final static String OPENSHIFT_CRON_FILES = "ezdeploy.openshift.cron";
    public static final List<String> cronResources = Utilities.getResourcesFromClassPath(CronFileInjector.class,
            OPENSHIFT_CRON_FILES);

    @Override
    public List<String> getResources() {
        return cronResources;
    }

    @Override
    public File getBasePath() {
        return OPENSHIFT_CRON_PATH;
    }
}
