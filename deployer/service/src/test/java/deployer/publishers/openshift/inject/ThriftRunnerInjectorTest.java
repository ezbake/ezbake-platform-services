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

package deployer.publishers.openshift.inject;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.openShift.inject.ArtifactResource;
import ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector;
import ezbake.deployer.publishers.openShift.inject.ThriftRunnerInjector;
import ezbake.services.deploy.thrift.DeploymentException;

public class ThriftRunnerInjectorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testNoNullResourceStreams() throws DeploymentException {
        ThriftRunnerInjector injector = new ThriftRunnerInjector();
        for (ArtifactResource resource : injector.getInjectableResources()) {
            Assert.assertNotNull(resource.getStream());
        }
    }

    @Test
    public void testResources() throws DeploymentException, IOException {
        ThriftRunnerInjector injector = new ThriftRunnerInjector();
        List<ArtifactResource> resources = injector.getInjectableResources();

        // Expect the thrift runner start, stop, and index.html
        Assert.assertEquals(3, resources.size());
        for (ArtifactResource resource : resources) {
            if (resource.getPath().equals(Files.get(ArtifactResourceInjector.OPENSHIFT_ACTION_HOOKS_PATH, "start"))) {
                // start must be executable
                Assert.assertEquals(ArtifactResourceInjector.executablePerms, resource.getPermissions());
                // Check actual contents
                String expectedStart = IOUtils.toString(
                        ThriftRunnerInjector.class.getResourceAsStream(
                                ("/" +
                                        ThriftRunnerInjector.THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES + ".start")
                                        .replaceAll("\\.", "/")));
                String receivedStart = IOUtils.toString(resource.getStream());
                Assert.assertEquals(expectedStart, receivedStart);
            } else if (resource.getPath()
                    .equals(Files.get(ArtifactResourceInjector.OPENSHIFT_ACTION_HOOKS_PATH, "stop"))) {
                // stop must be executable
                Assert.assertEquals(ArtifactResourceInjector.executablePerms, resource.getPermissions());
                // Check actual contents
                String expectedStop = IOUtils.toString(
                        ThriftRunnerInjector.class.getResourceAsStream(
                                ("/" +
                                        ThriftRunnerInjector.THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES + ".stop")
                                        .replaceAll("\\.", "/")));
                String receivedStop = IOUtils.toString(resource.getStream());
                Assert.assertEquals(expectedStop, receivedStop);
            } else if (resource.getPath()
                    .equals(Files.get(ThriftRunnerInjector.THRIFT_RUNNER_EXTRA_FILES_PATH, "index.html"))) {
                // Check actual contents
                String expectedHtml = IOUtils.toString(
                        ThriftRunnerInjector.class.getResourceAsStream(
                                ("/" +
                                        ThriftRunnerInjector.THRIFT_RUNNER_EXTRA_FILES_CART_FILES.replaceAll("\\.", "/")
                                        + "/index.html")));
                String receivedHtml = IOUtils.toString(resource.getStream());
                Assert.assertEquals(expectedHtml, receivedHtml);
            } else {
                Assert.fail("Unexpected resource found");
            }
        }
    }

    @Test
    public void testWithThriftRunner() throws DeploymentException, IOException {
        ThriftRunnerInjector injector = new ThriftRunnerInjectorMock(); // this only overrides the get thrift runner
        List<ArtifactResource> resource = injector.getInjectableResources();

        ArtifactResource thriftRunnerBinary = resource.get(0);
        Assert.assertEquals(Files.get("bin/thriftrunner.jar"), thriftRunnerBinary.getPath());
        String expected = ThriftRunnerInjectorMock.THRIFT_RUNNER_CONTENTS;
        String received = IOUtils.toString(thriftRunnerBinary.getStream());
        Assert.assertEquals(expected, received);
    }

    @Test
    public void testConfigurableThriftRunner() throws DeploymentException, IOException {
        String thriftRunnerContents = "I'm the thrift runner binary";
        String thriftRunnerPath = writeToTemporaryFile("thriftrunner.jar", thriftRunnerContents);

        // Set up mock
        EzDeployerConfiguration mockConfiguration = EasyMock.createMock(EzDeployerConfiguration.class);
        EasyMock.expect(mockConfiguration.getThriftRunnerJar()).andReturn(new File(thriftRunnerPath)).anyTimes();
        EasyMock.replay(mockConfiguration);

        // Get resources
        ThriftRunnerInjector injector = new ThriftRunnerInjector(mockConfiguration);
        List<ArtifactResource> resource = injector.getInjectableResources();

        // Check thrift runner binary
        ArtifactResource thriftRunnerBinary = resource.get(0);
        Assert.assertEquals(Files.get("bin/thriftrunner.jar"), thriftRunnerBinary.getPath());
        String received = IOUtils.toString(thriftRunnerBinary.getStream());
        Assert.assertEquals(thriftRunnerContents, received);
    }

    /**
     *
     * @return path to the file
     */
    public String writeToTemporaryFile(String fileName, String contents) throws IOException {
        File filePath = Files.get(folder.getRoot(), fileName);
        FileUtils.writeStringToFile(filePath, contents);
        return filePath.getAbsolutePath();
    }
}
