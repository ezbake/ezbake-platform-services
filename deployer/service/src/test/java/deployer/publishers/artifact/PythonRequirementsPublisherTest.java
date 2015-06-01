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
import ezbake.deployer.publishers.artifact.PythonRequirementsPublisher;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ddaniel on 3/16/15.
 */
public class PythonRequirementsPublisherTest {

    String[] wheels = new String[5];
    String[] packageNames = new String[5];
    String[] packageVersions = new String[5];

    @Before
    public void setUp()
    {
        // setup wheel filepaths
        wheels[0] = "./wheels/cffi-0.8.6-cp27-none-linux_x86_64.whl";
        wheels[1] = "./wheels/coverage-3.7.1-cp27-none-linux_x86_64.whl";
        wheels[2] = "./wheels/cryptography-0.7.2-cp27-none-linux_x86_64.whl";
        wheels[3] = "./wheels/django_crispy_forms-1.4.0-py2-none-any.whl";
        wheels[4] = "./wheels/enum34-1.0.4-py2-none-any.whl";

        // setup the output package names
        packageNames[0] = "cffi";
        packageNames[1] = "coverage";
        packageNames[2] = "cryptography";
        packageNames[3] = "django_crispy_forms";
        packageNames[4] = "enum34";

        // setupt the output package versions
        packageVersions[0] = "0.8.6";
        packageVersions[1] = "3.7.1";
        packageVersions[2] = "0.7.2";
        packageVersions[3] = "1.4.0";
        packageVersions[4] = "1.0.4";
    }

    @Test
    public void testExtractPackageInfo() {
        PythonRequirementsPublisher publisher = new PythonRequirementsPublisher();


        int i = 0;
        for (String line : wheels) {
            String[] parts = line.split("/");
            String basename = parts[parts.length - 1];

            String[] p = publisher.extractPackageInfo(basename);
            Assert.assertEquals(p[0], packageNames[i]);
            Assert.assertEquals(p[1], packageVersions[i]);

            i++;
        }
    }

    @Test
    public void testGenerateEntries() throws DeploymentException, ArchiveException, IOException {

        wheels[0] = "";
        ByteBuffer buffer = TestUtils.createSampleOpenShiftWebAppTarBallWithEmptyFiles(wheels);

        // prepare the deployment artifact
        DeploymentMetadata metadata = new DeploymentMetadata();
        DeploymentArtifact artifact = new DeploymentArtifact(metadata, buffer);

        // generateEntries
        PythonRequirementsPublisher publisher = new PythonRequirementsPublisher();
        List<ArtifactDataEntry> entries = (List) publisher.generateEntries(artifact);

        // initial checks
        Assert.assertEquals(entries.size(), 1);
        ArtifactDataEntry entry = entries.get(0);
        Assert.assertEquals(entry.getEntry().getName(), "requirements.txt");

        // write contents out to file
        String filepath = "/tmp/" + entry.getEntry().getName();
        File requirementsFile = new File(filepath);
        FileOutputStream output = new FileOutputStream(requirementsFile);
        IOUtils.write(entry.getData(), output);

        // read file and check lines
        String[] assertionLines = new String[5];
        assertionLines[0] = "";
        assertionLines[1] = "coverage==3.7.1";
        assertionLines[2] = "cryptography==0.7.2";
        assertionLines[3] = "django_crispy_forms==1.4.0";
        assertionLines[4] = "enum34==1.0.4";

        FileInputStream fis = new FileInputStream(requirementsFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        List<String> requirementContents = new ArrayList<String>();
        while((line = br.readLine()) != null)
        {
            if (line.contains("=="))
                requirementContents.add(line);
        }

        for(int i=1; i < assertionLines.length; i++)
        {
            line = assertionLines[i];
            Assert.assertTrue(line + " not found in: " + requirementContents.toString(),
                    requirementContents.contains(line));
        }
    }
}
