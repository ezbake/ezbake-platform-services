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

import com.google.common.collect.Lists;
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.openShift.inject.ArtifactResource;
import ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector;
import ezbake.deployer.publishers.openShift.inject.SecurityActionHookInjector;
import ezbake.deployer.publishers.openShift.inject.ThriftRunnerInjector;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SecurityActionHookInjectorTest {

    @Test
    public void testGetResources() {
        Assert.assertEquals(
                Lists.newArrayList("ezdeploy/openshift/security/action_hooks/pre_build"),
                new SecurityActionHookInjector().getResources());
    }

    @Test
    public void testNoNullResourceStreams() throws DeploymentException {
        SecurityActionHookInjector injector = new SecurityActionHookInjector();
        for (ArtifactResource resource : injector.getInjectableResources()) {
            Assert.assertNotNull(resource.getStream());
        }
    }

    @Test
    public void testGetBasePath() {
        Assert.assertEquals(
                SecurityActionHookInjector.OPENSHIFT_ACTION_HOOKS_PATH,
                new SecurityActionHookInjector().getBasePath());
    }

    @Test
    public void testResources() throws DeploymentException, IOException {
        SecurityActionHookInjector injector = new SecurityActionHookInjector();
        List<ArtifactResource> resource = injector.getInjectableResources();

        // Expect the thrift runner bin, start, stop, and index.html
        Assert.assertEquals(1, resource.size());

        ArtifactResource preBuild = resource.get(0);
        Assert.assertEquals(Files.get(ArtifactResourceInjector.OPENSHIFT_ACTION_HOOKS_PATH, "pre_build"),
                preBuild.getPath());
        // must be executable
        Assert.assertEquals(ArtifactResourceInjector.executablePerms, preBuild.getPermissions());
        // Check actual contents
        String expected = IOUtils.toString(ThriftRunnerInjector.class.getResourceAsStream(("/" +
                SecurityActionHookInjector.SECURITY_ACTION_HOOKS + ".pre_build").replaceAll("\\.", "/")));
        String received = IOUtils.toString(preBuild.getStream());
        Assert.assertEquals(expected, received);
    }
}
