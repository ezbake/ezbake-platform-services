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

package deployer;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.deployer.DeployerModule;
import ezbake.deployer.EzBakeDeployerHandler;
import ezbake.deployer.EzDeployerStore;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.EzDataSetPublisher;
import ezbake.deployer.publishers.EzDeployPublisher;
import ezbake.deployer.publishers.local.LocalDeployerModule;
import ezbake.local.zookeeper.LocalZookeeper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.deploy.thrift.ApplicationInfo;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.DeploymentStatus;
import ezbake.services.deploy.thrift.WebAppInfo;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;

import org.apache.thrift.TException;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;
import static ezbake.deployer.utilities.ArtifactHelpers.getAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EzBakeDeployerHandlerTest {
    private static final Properties configuration;
    private static final EzDeployerConfiguration ezDeployerConfiguration;
    private static final EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken("deployer", "deployer", Lists.newArrayList("U"));

    static {
        try {
            configuration = new EzConfiguration(
                    new ClasspathConfigurationLoader("/test.properties")).getProperties();
            ezDeployerConfiguration = new EzDeployerConfiguration(configuration);
        } catch (EzConfigurationLoaderException ex) {
            throw new RuntimeException(ex);
        }
    }

    private ArtifactManifest createSampleArtifactManifest() {
        ArtifactManifest artMeta = new ArtifactManifest();
        artMeta.setApplicationInfo(new ApplicationInfo());
        artMeta.setArtifactType(ArtifactType.DataSet);
        artMeta.getApplicationInfo().setApplicationId("foobarAppId");
        artMeta.getApplicationInfo().setServiceId("foobarService");
        List<String> dsets = new ArrayList<>();
        dsets.add("fieldSearchAppId");
        dsets.add("geoSearchAppId");
        artMeta.getApplicationInfo().setDatasets(dsets);

        WebAppInfo webAppInfo = new WebAppInfo();
        webAppInfo.setExternalWebUrl("example.com/foobar");
        artMeta.setWebAppInfo(webAppInfo);
        return artMeta;
    }

    private DeploymentArtifact createSampleDeploymentArtifact(ArtifactManifest artMeta, ByteBuffer bb) {

        DeploymentMetadata meta = new DeploymentMetadata();
        meta.setManifest(artMeta);
        meta.setStatus(DeploymentStatus.Deployed);

        DeploymentArtifact da = new DeploymentArtifact();
        da.setMetadata(meta);
        da.setArtifact(bb);
        return da;
    }

    public static DeploymentArtifact createSampleDeploymentArtifact(String appId, String serviceId) throws IOException {
        final DeploymentArtifact dsArtifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.Thrift);
        dsArtifact.getMetadata().getManifest().getApplicationInfo().setApplicationId(appId);
        dsArtifact.getMetadata().getManifest().getApplicationInfo().setServiceId(serviceId);
        return dsArtifact;
    }

    @Test
    public void testDeployingOfThriftService() throws Exception {

        // create test deployed app that relies on our two test datasets
        final ArtifactManifest artMeta = createSampleArtifactManifest();
        final ByteBuffer sampleTarBall = TestUtils.createSampleAppTarBall(ArtifactType.Thrift);
        final DeploymentArtifact dsArtifact1 = createSampleDeploymentArtifact("", "fieldSearchAppId");
        final DeploymentArtifact dsArtifact2 = createSampleDeploymentArtifact("", "geoSearchAppId");

        // sample publisher asserts that expected names occur
        EzDeployPublisher publisher = createMock(EzDeployPublisher.class);
        EzDeployerStore store = createMock(EzDeployerStore.class);
        Capture<DeploymentArtifact> artifactCapture = new Capture<>(CaptureType.ALL);
        Capture<EzSecurityToken> tokenCapture = new Capture<>();
        publisher.publish(capture(artifactCapture), capture(tokenCapture));
        expectLastCall().times(3);
        store.writeArtifactToStore(artMeta, sampleTarBall);

        expectLastCall().andReturn(createSampleDeploymentArtifact(artMeta, sampleTarBall));
        expectLastCall().once();
        store.getArtifactFromStore("", "fieldSearchAppId");
        expectLastCall().andReturn(dsArtifact1).once();
        store.getArtifactFromStore("", "geoSearchAppId");
        expectLastCall().andReturn(dsArtifact2).once();
        publisher.validate();
        replay(publisher, store);

        ThriftClientPool mockPool = createMock(ThriftClientPool.class);
        expect(mockPool.getSecurityId(anyObject(String.class))).andReturn("client").anyTimes();

        EzBakeDeployerHandler mdh = new EzBakeDeployerHandler(publisher, store, ezDeployerConfiguration,
                new EzbakeSecurityClient(configuration), null, null);
        mdh.setConfigurationProperties(configuration);
        mdh.deployService(artMeta, sampleTarBall, token);
        verify(publisher, store);

//        store.getLatestApplicationMetaDataFromStore("foobarAppId", "foobarService");

        assertEquals(3, artifactCapture.getValues().size());
        assertDeploymentArtifactInCapture(artifactCapture, "fieldSearchAppId",
                "foobarAppId", "fieldSearchAppId");
        assertDeploymentArtifactInCapture(artifactCapture, "geoSearchAppId",
                "foobarAppId", "geoSearchAppId");
    }

    @Test
    public void testDeployingOfService() throws Exception {
        final DeploymentArtifact expectedArtifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.Thrift);
        expectedArtifact.metadata.setStatus(DeploymentStatus.Deployed);

        // sample publisher asserts that expected names occur
        EzDeployPublisher publisher = createMock(EzDeployPublisher.class);
        EzDeployerStore store = createMock(EzDeployerStore.class);
        publisher.publish(expectedArtifact, token);
        expectLastCall().once();
        store.writeArtifactToStore(expectedArtifact.metadata.manifest, expectedArtifact.artifact);
        expectLastCall().andReturn(expectedArtifact);
        expectLastCall().once();
        publisher.validate();
        replay(publisher, store);
        ThriftClientPool mockPool = createMock(ThriftClientPool.class);
        expect(mockPool.getSecurityId(anyObject(String.class))).andReturn("client").anyTimes();

        EzBakeDeployerHandler mdh = new EzBakeDeployerHandler(publisher, store, ezDeployerConfiguration,
                new EzbakeSecurityClient(configuration), null, null);
        mdh.setConfigurationProperties(configuration);
        mdh.deployService(expectedArtifact.metadata.manifest, expectedArtifact.artifact, token);
        verify(publisher, store);
    }

    @Test
    public void testInjection() throws Exception {
        LocalZookeeper zookeeper = null;
        try {
            Injector injector = Guice.createInjector(new DeployerModule(), new LocalDeployerModule(), new ConfigurationModule());
            Properties config = injector.getInstance(Properties.class);
            ZookeeperConfigurationHelper zooConfig = new ZookeeperConfigurationHelper(config);
            int port = Integer.parseInt(zooConfig.getZookeeperConnectionString().split(":")[1]);
            zookeeper = new LocalZookeeper(port);
            injector.getInstance(EzBakeDeployerHandler.class);
        } finally {
            if (zookeeper != null) {
                zookeeper.shutdown();
            }
        }
    }

    @Test
    public void testMultiBindingDatabases() throws Exception {
        LocalZookeeper zookeeper = null;
        try {
            Injector injector = Guice.createInjector(new DeployerModule(), new LocalDeployerModule(), new ConfigurationModule());
            ZookeeperConfigurationHelper zooConfig = new ZookeeperConfigurationHelper(injector.getInstance(Properties.class));
            int port = Integer.parseInt(zooConfig.getZookeeperConnectionString().split(":")[1]);
            zookeeper = new LocalZookeeper(port);
            EzDataSetPublisher publisher = injector.getInstance(EzDataSetPublisher.class);
            assertEquals(6, publisher.possibleSetupsCount());
        } finally {
            if (zookeeper != null) {
                zookeeper.shutdown();
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void getDeploymentArtifact() throws IOException, TException {
        final DeploymentArtifact expected = TestUtils.createSampleDeploymentArtifact(ArtifactType.WebApp);
        final String appId = expected.getMetadata().getManifest().getApplicationInfo().getApplicationId();
        final String serviceId = expected.getMetadata().getManifest().getApplicationInfo().getServiceId();

        EzDeployPublisher publisher = createMock(EzDeployPublisher.class);
        EzDeployerStore store = createMock(EzDeployerStore.class);
        ThriftClientPool mockPool = createMock(ThriftClientPool.class);
        
        expect(store.getArtifactFromStore(appId, serviceId)).andReturn(expected);
        expect(mockPool.getSecurityId(anyObject(String.class))).andReturn("client").anyTimes();

        publisher.validate();
        
        replay(mockPool, publisher, store);
        
        EzBakeDeployerHandler mdh = new EzBakeDeployerHandler(publisher, store, ezDeployerConfiguration,
                new EzbakeSecurityClient(configuration), null, null);
        mdh.setConfigurationProperties(configuration);
        DeploymentArtifact actual = mdh.getLatestVersionOfDeploymentArtifact(appId, serviceId, token);
        Assert.assertEquals(expected, actual);
    }

    private static void assertDeploymentArtifactInCapture(Capture<DeploymentArtifact> artifactCapture, String serviceName,
                                                          String expectedAppId, String expectedServiceId) {
        DeploymentArtifact artifact = Iterables.find(artifactCapture.getValues(), findArtifactByService(serviceName));
        assertNotNull(String.format("Did not find '%s' as a service of published datasets", serviceName), artifact);
        assertEquals(expectedAppId, getAppId(artifact));
        assertEquals(expectedServiceId, getServiceId(artifact));

    }

    private static Predicate<DeploymentArtifact> findArtifactByService(final String service) {
        return new Predicate<DeploymentArtifact>() {
            @Override
            public boolean apply(DeploymentArtifact input) {
                return input.getMetadata().getManifest().getApplicationInfo().getServiceId().equals(service);
            }
        };
    }

}
