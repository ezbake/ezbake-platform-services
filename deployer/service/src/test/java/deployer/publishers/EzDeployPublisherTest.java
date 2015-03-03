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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Properties;

import com.google.inject.Guice;
import deployer.ConfigurationModule;
import ezbake.deployer.DeployerModule;
import ezbake.deployer.publishers.local.LocalDeployerModule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.publishers.EzDeployPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.ezdiscovery.ServiceDiscovery;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.thrift.ThriftTestUtils;

import deployer.TestUtils;

@RunWith(Parameterized.class)
public class EzDeployPublisherTest {
    public final static String PURGE_NAMESPACE = "ezpurge";
    @Parameterized.Parameter(value = 0)
    public ArtifactType artifactType;
    private TestingServer server;
    private CuratorFramework zkClient;

    @Parameterized.Parameters(name = "publish artifact {0}")
    public static Collection<Object[]> data() {
        return Lists.newArrayList(
                Iterables.transform(
                        Lists.newArrayList(ArtifactType.values()), new Function<ArtifactType, Object[]>() {
                            @Override
                            public Object[] apply(ArtifactType artifactType) {
                                return new Object[] {artifactType};
                            }
                        }));
    }

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
    public void testPublish() throws Exception {
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
                .andAnswer(TestUtils.singleAnswerFor(artifactType, expectedPublisher, unexpectedPublisher)).anyTimes();
        serviceDiscovery.setSecurityIdForApplication(TestUtils.APP_NAME, TestUtils.SECURITY_ID);

        expectedPublisher.publish(capture(deploymentArtifactCapture), capture(tokenCapture));

        replay(expectedPublisher, unexpectedPublisher, mapping, serviceDiscovery);
        publisher.publish(
                TestUtils.createSampleDeploymentArtifact(artifactType), ThriftTestUtils.generateTestSecurityToken("U"));

        verify(expectedPublisher, unexpectedPublisher, serviceDiscovery);

        TestUtils.assertDeploymentArtifact(deploymentArtifactCapture.getValue(), artifactType);

        TestUtils.getSampleTarBallChecker(artifactType).check(deploymentArtifactCapture.getValue().getArtifact());
        //test to ensure that a purgeable services was registered
        assertEquals(1, zkClient.getChildren().forPath("").size());
    }

    /**
     * Pretty dumb test but just ensure that the forwarding to the PublisherMapping works for unpublish.
     *
     * @throws Exception on any testing errors
     */
    @Test
    public void testUnPublish() throws Exception {
        EzPublisher expectedPublisher = createMock(EzPublisher.class);
        EzPublisher unexpectedPublisher = createMock(EzPublisher.class);

        SSLCertsService certsService = createMock(SSLCertsService.class);
        EzPublisherMapping mapping = createMock(EzPublisherMapping.class);
        ServiceDiscovery serviceDiscovery = createMock(ServiceDiscovery.class);
        Properties props = new Properties();
        props.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, server.getConnectString());
        EzDeployPublisher publisher = new EzDeployPublisher(mapping, props, serviceDiscovery, null, null);
        Capture<DeploymentArtifact> deploymentArtifactCapture = new Capture<>();
        Capture<EzSecurityToken> tokenCapture = new Capture<>();

        expect(mapping.get(anyObject(ArtifactType.class)))
                .andAnswer(TestUtils.singleAnswerFor(artifactType, expectedPublisher, unexpectedPublisher)).anyTimes();
        expectedPublisher.unpublish(capture(deploymentArtifactCapture), capture(tokenCapture));

        replay(expectedPublisher, unexpectedPublisher, mapping, certsService, serviceDiscovery);
        publisher.unpublish(
                TestUtils.createSampleDeploymentArtifact(artifactType), ThriftTestUtils.generateTestSecurityToken("U"));

        verify(expectedPublisher, unexpectedPublisher, certsService, serviceDiscovery);
        //test to ensure there are no purgeable services registered
        assertEquals(0, zkClient.getChildren().forPath("").size());
    }
}
