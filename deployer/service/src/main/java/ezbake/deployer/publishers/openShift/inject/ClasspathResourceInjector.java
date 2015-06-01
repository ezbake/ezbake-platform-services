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

import ezbake.services.deploy.thrift.DeploymentException;

import java.io.File;
import java.util.List;

/**
 * This abstract class implements a simple getInjectableResources that can inject resources to a single path
 */
public abstract class ClasspathResourceInjector extends AbstractResourceInjector {

    /**
     * This returns the list of resources that should be looked up and injected
     * @return a list of classpath resources to inject
     */
    public abstract List<String> getResources();

    /**
     * The path at which the resources should be injected
     * @return the base path for the resources
     */
    public abstract File getBasePath();

    /**
     * This searches the classpath for files to auto inject for all openshift cartridges
     * This will get the prebuild action hooks.
     *
     * @return list of resources to inject
     */
    @Override
    public List<ArtifactResource> getInjectableResources() throws DeploymentException {
        return getInjectableClasspathResources(getResources(), getBasePath());
    }

}
