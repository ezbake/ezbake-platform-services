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

package deployer.publishers.artifact;


import deployer.TestUtils;
import ezbake.deployer.publishers.artifact.JavaWebAppArtifactContentsPublisher;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class JavaWebAppArtifactContentsPublisherTest {

    @Test
    public void testGenerateConfigFilesWithArtifactWithStandaloneFile() throws DeploymentException, IOException {
        JavaWebAppArtifactContentsPublisher generator = new JavaWebAppArtifactContentsPublisher();
        Collection<ArtifactDataEntry> expectedEntries = TestUtils.sampleOpenShiftConfiguration();
        Collection<ArtifactDataEntry> entries =
                generator.generateEntries(TestUtils.createSampleOpenShiftDeploymentArtifact());
        Assert.assertEquals(4, entries.size());
        Assert.assertNotEquals("ArtifactDataEntry collections do not differ", expectedEntries, entries);
    }

    @Test
    public void testGenerateConfigFilesWithArtifactWithoutStandaloneFile() throws DeploymentException, IOException {
        JavaWebAppArtifactContentsPublisher generator = new JavaWebAppArtifactContentsPublisher();
        Collection<ArtifactDataEntry> entries =
                generator.generateEntries(TestUtils.createSampleDeploymentArtifact(ArtifactType.WebApp));
        Assert.assertEquals(5, entries.size());
    }
}
