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
 * This searches the classpath {@link #EXTRA_FILES_CLASSPATH} for files to auto inject into every tar files to
 * publish.
 */
public class ExtraFilesInjector extends ClasspathResourceInjector {
    public final static String EXTRA_FILES_CLASSPATH = "ezdeploy.openshift.extraFiles";
    private final static File EXTRA_FILES_BASEPATH = Files.get("/", EXTRA_FILES_CLASSPATH.replace('.', '/'));
    public static final List<String> extraFilesToInject = Utilities.getResourcesFromClassPath(
            ExtraFilesInjector.class, EXTRA_FILES_CLASSPATH);

    /**
     * This returns the list of resources that should be looked up and injected
     *
     * @return a list of classpath resources to inject
     */
    @Override
    public List<String> getResources() {
        return extraFilesToInject;
    }

    /**
     * The path at which the resources should be injected
     *
     * @return the base path for the resources
     */
    @Override
    public File getBasePath() {
        return new File(".");
    }

    @Override
    protected File relativizeResourcePath(String path) {
        return Files.relativize(EXTRA_FILES_BASEPATH, Files.get("/", path));
    }
}
