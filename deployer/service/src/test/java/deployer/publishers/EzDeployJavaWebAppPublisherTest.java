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

package deployer.publishers;


import deployer.TestUtils;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.publishers.EzDeployPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.ezdiscovery.ServiceDiscovery;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.thrift.ThriftTestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;


public class EzDeployJavaWebAppPublisherTest {
    public final static String PURGE_NAMESPACE = "ezpurge";
    private TestingServer server;
    private CuratorFramework zkClient;

    @Before
    public void setup() throws Exception {
        System.setProperty("curator-dont-log-connection-problems", "true");
        server = new TestingServer();
        zkClient = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).namespace(PURGE_NAMESPACE)
                .retryPolicy(new RetryNTimes(5, 1000)).build();
        zkClient.start();
    }

    @After
    public void teardown() {
        try {
            server.close();
            zkClient.close();
        } catch (Throwable ignored) {
        }
    }

    @Test
    public void testPublishWithJavaWebAppConfigurationPublisher() throws Exception {
        assertEquals(0, zkClient.getChildren().forPath("").size());
        EzPublisher expectedPublisher = createMock(EzPublisher.class);
        EzPublisher unexpectedPublisher = createMock(EzPublisher.class);

        EzPublisherMapping mapping = createMock(EzPublisherMapping.class);

        Capture<DeploymentArtifact> deploymentArtifactCapture = new Capture<>();
        Capture<EzSecurityToken> tokenCapture = new Capture<>();
        ServiceDiscovery serviceDiscovery = createMock(ServiceDiscovery.class);

        Properties props = new Properties();
        props.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, server.getConnectString());
        EzDeployPublisher publisher = new EzDeployPublisher(mapping, props, serviceDiscovery, null, null);
        expect(mapping.get(anyObject(ArtifactType.class)))
                .andAnswer(TestUtils.singleAnswerFor(ArtifactType.WebApp, expectedPublisher, unexpectedPublisher)).anyTimes();
        serviceDiscovery.setSecurityIdForApplication(TestUtils.APP_NAME, TestUtils.SECURITY_ID);

        expectedPublisher.publish(capture(deploymentArtifactCapture), capture(tokenCapture));

        replay(expectedPublisher, unexpectedPublisher, mapping, serviceDiscovery);
        publisher.publish(
                TestUtils.createSampleOpenShiftDeploymentArtifact(), ThriftTestUtils.generateTestSecurityToken("U"));

        verify(expectedPublisher, unexpectedPublisher, serviceDiscovery);

        TestUtils.assertDeploymentArtifact(deploymentArtifactCapture.getValue(), ArtifactType.WebApp);

        TestUtils.getSampleTarBallChecker(ArtifactType.WebApp).check(deploymentArtifactCapture.getValue().getArtifact());

        assertEquals(1, zkClient.getChildren().forPath("").size());
    }
}
