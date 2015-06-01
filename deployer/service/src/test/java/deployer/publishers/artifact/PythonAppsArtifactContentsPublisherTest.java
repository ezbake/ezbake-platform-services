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
import ezbake.deployer.publishers.artifact.PythonAppsArtifactContentsPublisher;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import org.apache.commons.compress.archivers.ArchiveException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class PythonAppsArtifactContentsPublisherTest {

    private final int bufferSize = 0x20000; // ~130K.


    @Test
    public void testGenerateEntries() throws IOException, DeploymentException
    {
        PythonAppsArtifactContentsPublisher publisher = new PythonAppsArtifactContentsPublisher();
        Collection<ArtifactDataEntry> entries = publisher.generateEntries(
                TestUtils.createSampleOpenShiftDeploymentArtifact());

        Assert.assertEquals(entries.size(), 1);

        ArtifactDataEntry entry = (ArtifactDataEntry) entries.toArray()[0];
        Assert.assertEquals("./wsgi.py", entry.getEntry().getName());
    }


    @Test
    public void testWSGIInsertionPreExist() throws IOException, DeploymentException, ArchiveException
    {
        String[] testFileNames = new String[2];
        testFileNames[0] = "manage.py";
        testFileNames[1] = "wsgi.py";
        ByteBuffer artifactByteBuffer = TestUtils.createSampleOpenShiftWebAppTarBallWithEmptyFiles(testFileNames);

        // prepare the deployment artifact
        DeploymentMetadata metadata = new DeploymentMetadata();
        DeploymentArtifact artifact = new DeploymentArtifact(metadata, artifactByteBuffer);

        // test generateEntries
        PythonAppsArtifactContentsPublisher publisher = new PythonAppsArtifactContentsPublisher();
        List<ArtifactDataEntry> entries = (List) publisher.generateEntries(artifact);

        Assert.assertEquals(entries.size(), 1);
        Assert.assertEquals("./wsgi.py", entries.get(0).getEntry().getName());
    }

    @Test
    public void testWSGIInsertion() throws IOException, DeploymentException, ArchiveException
    {
        ByteBuffer artifactByteBuffer = TestUtils.createSampleOpenShiftWebAppTarBall();

        // prepare the deployment artifact
        DeploymentMetadata metadata = new DeploymentMetadata();
        DeploymentArtifact artifact = new DeploymentArtifact(metadata, artifactByteBuffer);

        // test generateEntries
        PythonAppsArtifactContentsPublisher publisher = new PythonAppsArtifactContentsPublisher();
        List<ArtifactDataEntry> entries = (List) publisher.generateEntries(artifact);

        Assert.assertEquals(entries.size(), 1);
        Assert.assertEquals("./wsgi.py", entries.get(0).getEntry().getName());
    }

}
