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
import ezbake.deployer.publishers.artifact.CertEntryPublisher;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class CertEntryPublisherTest {

    @Test
    public void testGenerateCerts() throws DeploymentException, IOException {
        SSLCertsService certsService = createMock(SSLCertsService.class);
        CertEntryPublisher generator = new CertEntryPublisher(certsService);

        List<ArtifactDataEntry> expectedEntries = TestUtils.sampleSSL();
        expect(certsService.get(TestUtils.SERVICE_NAME, TestUtils.SECURITY_ID)).andReturn(expectedEntries);
        replay(certsService);

        Collection<ArtifactDataEntry> entries = generator.generateEntries(TestUtils.createSampleDeploymentArtifact(ArtifactType.Thrift));

        verify(certsService);
        Assert.assertEquals(expectedEntries, entries);
    }
}
