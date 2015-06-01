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

import org.apache.commons.io.IOUtils;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Test;

import ezbake.deployer.publishers.openShift.inject.ArtifactResource;
import ezbake.deployer.publishers.openShift.inject.ExtraFilesInjector;
import ezbake.services.deploy.thrift.DeploymentException;

public class ExtraFilesInjectorTest {
    private static void assertArtifactResource(ArtifactResource resource, String path) throws IOException {
        Assert.assertNull(resource.getPermissions());

        // Check actual contents
        String expected = IOUtils.toString(
                ExtraFilesInjectorTest.class.getResourceAsStream(
                        ("/" + ExtraFilesInjector.EXTRA_FILES_CLASSPATH + path).replaceAll("\\.", "/")));

        String received = IOUtils.toString(resource.getStream());
        Assert.assertEquals(expected, received);
    }

    @Test
    public void testGetResources() {
        Assert.assertThat(
                new ExtraFilesInjector().getResources(), IsIterableContainingInAnyOrder.containsInAnyOrder(
                        "ezdeploy/openshift/extraFiles/extraFile",
                        "ezdeploy/openshift/extraFiles/extraSubDir/extraFile"));
    }

    @Test
    public void testNoNullResourceStreams() throws DeploymentException {
        ExtraFilesInjector injector = new ExtraFilesInjector();
        for (ArtifactResource resource : injector.getInjectableResources()) {
            Assert.assertNotNull(resource.getStream());
        }
    }

    @Test
    public void testGetBasePath() {
        Assert.assertEquals(new File("."), new ExtraFilesInjector().getBasePath());
    }

    @Test
    public void testResources() throws DeploymentException, IOException {
        ExtraFilesInjector injector = new ExtraFilesInjector();
        List<ArtifactResource> resources = injector.getInjectableResources();

        Assert.assertEquals(2, resources.size());
        for (ArtifactResource resource : resources) {
            if (resource.getPath().equals(new File(".//extraSubDir/extraFile"))) {
                assertArtifactResource(resource, "/extraSubDir/extraFile");
            } else if (resource.getPath().equals(new File("./extraFile"))) {
                assertArtifactResource(resource, "/extraFile");
            } else {
                Assert.fail("Unexpected resource found");
            }
        }
    }
}
