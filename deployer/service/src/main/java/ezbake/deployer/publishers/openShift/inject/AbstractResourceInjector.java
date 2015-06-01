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

import com.google.common.collect.Lists;
import ezbake.deployer.impl.Files;
import ezbake.services.deploy.thrift.DeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * This abstract class should be implemented by ArtifactResourceInjectors. It includes helper functions
 */
public abstract class AbstractResourceInjector implements ArtifactResourceInjector {
    private static final Logger logger = LoggerFactory.getLogger(AbstractResourceInjector.class);


    protected File relativizeResourcePath(String resourcePath) {
        return new File(Files.get(resourcePath).getName());
    }

    /**
     * Take the given base path and resource path, and return the path to the resource in the artifact package
     *
     * @param basePath base path of the resource
     * @param resourcePath path to the resource in the directory
     * @return a file path to the final artifact path
     */
    protected File getArtifactPathForResource(File basePath, File resourcePath) {
        return Files.resolve(basePath, resourcePath);
    }

    /**
     * This is the helper function that actually injects a list of resources from the classpath into the application
     * This will be placed inside of the git repository for the OpenShift Application
     *
     * @param resources the list of resources to inject
     * @param basePath the base path (relative to the git repository root) 'www' would go to gitProject/www/*
     * @throws ezbake.services.deploy.thrift.DeploymentException on any errors injecting the resources into the application
     */
    protected List<ArtifactResource> getInjectableClasspathResources(List<String> resources, File basePath) throws DeploymentException {
        List<ArtifactResource> injectableResources = Lists.newArrayList();

        for (String resourcePathStr : resources) {
            resourcePathStr = Files.get("/", resourcePathStr).toString();
            InputStream resource = AbstractResourceInjector.class.getResourceAsStream(resourcePathStr);

            final File resourcePath = relativizeResourcePath(resourcePathStr);
            final File artifactPath = getArtifactPathForResource(basePath, resourcePath);
            if (resource == null) {
                logger.warn("Resource file {} couldn't be opened.", resourcePathStr);
            } else {
                logger.info("Adding {} to artifacts", artifactPath.toString());
                ArtifactResource injectable;
                if (executableScripts.contains(resourcePath.toString())) {
                    injectable = new ArtifactResource(artifactPath, resource, executablePerms);
                } else {
                    injectable = new ArtifactResource(artifactPath, resource);
                }
                injectableResources.add(injectable);
            }
        }
        return injectableResources;
    }
}
