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

package deployer.utilities;

import com.google.common.collect.Lists;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.utilities.PackageDeployer;
import ezbake.deployer.utilities.YmlKeys;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;
import ezbake.thrift.ThriftTestUtils;
import org.apache.thrift.TException;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for Package Deployer
 * <p/>
 * Created by sstapleton on 2/7/14.
 */
public class PackageDeployerTest {

    @Test
    public void testdeployPackage() throws TException, URISyntaxException, IOException {

        EzBakeServiceDeployer.Client client = createMock(EzBakeServiceDeployer.Client.class);
        EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken("deployer", "deployer", Lists.newArrayList("U"));

        Capture<ArtifactManifest> capturedArtifact = new Capture<>(CaptureType.ALL);
        Capture<ByteBuffer> capturedBuffer = new Capture<>(CaptureType.ALL);
        Capture<EzSecurityToken> capturedToken = new Capture<>(CaptureType.ALL);

        expect(client.deployService(capture(capturedArtifact), capture(capturedBuffer), capture(capturedToken))).andReturn(null).once();

        replay(client);
        PackageDeployer.deployPackage(client, new File(PackageDeployerTest.class.getResource("/ezbake/deployer/fakeJars/testJar.jar").toURI()),
                new File(this.getClass().getClassLoader().getResource("ezbake/deployer/cli/testSingleThriftService.yml").toURI()),
                "foobarSecurityId", token);
        verify(client);

        assertEquals("foobarAppId", capturedArtifact.getValue().applicationInfo.getApplicationId());
        assertEquals("foobarSecurityId", capturedArtifact.getValue().applicationInfo.getSecurityId());
        assertTrue(capturedBuffer.getValue().array().length > 150);//Checks to see tar has data
    }


    @Test
    public void testCreateManifestWithOverrides() throws TException, URISyntaxException, IOException {
        EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken("deployer", "deployer", Lists.newArrayList("U"));

        HashMap<String, Object> overrides = new HashMap<>();
        overrides.put(YmlKeys.RootManifestKeys.applicationName.getName(), "MyTestAppOverride");
        overrides.put(YmlKeys.RootManifestKeys.securityId.getName(), "123456789Override");
        List<ArtifactManifest> artifactManifests = PackageDeployer.createManifests(new File(this.getClass().getClassLoader().getResource(
                "ezbake/deployer/cli/testSingleThriftService.yml").toURI()), token, overrides);
        assertEquals(1, artifactManifests.size());
        ArtifactManifest manifest = artifactManifests.get(0);
        assertEquals("MyTestAppOverride", manifest.getApplicationInfo().getApplicationId());
        assertEquals("123456789Override", manifest.getApplicationInfo().getSecurityId());
    }

}
