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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.utilities.Utilities;
import ezbake.services.deploy.thrift.DeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

/**
 * This class can return the resources that need to be injected for thrift runner applications
 *
 * For the DIY version of thriftrunner apps, injects the thriftrunner binary and the start/stop control scripts from
 * the classpath in order to start the thriftrunner service
 */
public class ThriftRunnerInjector extends AbstractResourceInjector {
    private static final Logger logger = LoggerFactory.getLogger(ThriftRunnerInjector.class);

    private static final File thriftRunnerArtifactPath = Files.get("bin", "thriftrunner.jar");

    public final static String THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES = "ezdeploy.openshift.thriftrunner.action_hooks";
    private static final List<String> thriftRunnerControlScripts = Utilities.getResourcesFromClassPath(
            ThriftRunnerInjector.class, THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES);

    public final static String THRIFT_RUNNER_EXTRA_FILES_CART_FILES = "ezdeploy.openshift.thriftrunner.extraFiles.www";
    private static final List<String> thriftRunnerWWWFiles = Utilities.getResourcesFromClassPath(
            ThriftRunnerInjector.class, THRIFT_RUNNER_EXTRA_FILES_CART_FILES);
    public final static File THRIFT_RUNNER_EXTRA_FILES_PATH = Files.get("www");

    private final Optional<File> pathToThriftRunnerBinaryJar;

    public ThriftRunnerInjector() {
        File trJar = Files.get("thriftrunner.jar");
        if (!Files.exists(trJar)) trJar = null;
        pathToThriftRunnerBinaryJar = Optional.fromNullable(trJar);
    }

    public ThriftRunnerInjector(EzDeployerConfiguration configuration) {
        pathToThriftRunnerBinaryJar = Optional.fromNullable(configuration.getThriftRunnerJar());
    }

    /**
     * Return injectable Thrift runner resources, binary, action hooks, extra files
     *
     * @return the resources to be injected
     * @throws DeploymentException
     */
    @Override
    public List<ArtifactResource> getInjectableResources() throws DeploymentException {
        List<ArtifactResource> resources = Lists.newArrayList();

        InputStream thriftRunner = getThriftRunnerBinary();
        if (thriftRunner != null) {
            resources.add(new ArtifactResource(thriftRunnerArtifactPath, thriftRunner));
        }
        resources.addAll(getInjectableClasspathResources(thriftRunnerControlScripts, OPENSHIFT_ACTION_HOOKS_PATH));
        resources.addAll(getInjectableClasspathResources(thriftRunnerWWWFiles, THRIFT_RUNNER_EXTRA_FILES_PATH));

        return resources;
    }

    /**
     * If configured retrieves the thriftrunner binary from the filesystem
     *
     * @return thriftrunner binary
     * @throws ezbake.services.deploy.thrift.DeploymentException - on any errors reading it
     */
    protected InputStream getThriftRunnerBinary() throws DeploymentException {
        try {
            if (pathToThriftRunnerBinaryJar.isPresent()) {
                return new FileInputStream(pathToThriftRunnerBinaryJar.get());
            } else {
                return null;
            }
        } catch (FileNotFoundException e) {
            logger.error("Could not find thrift runner binary at: " + pathToThriftRunnerBinaryJar.get().toString(), e);
            throw new DeploymentException("Could not find thrift runner binary at: "
                    + pathToThriftRunnerBinaryJar.get().toString() + ":" + e.getMessage());
        }
    }
}
