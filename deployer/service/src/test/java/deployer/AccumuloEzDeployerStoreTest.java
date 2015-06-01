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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.deployer.AccumuloEzDeployerStore;
import ezbake.deployer.EzDeployerStore;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.LocalFileArtifactWriter;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.DeploymentStatus;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static ezbake.deployer.utilities.ArtifactHelpers.getFqAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccumuloEzDeployerStoreTest {

    private static final Instance instance = new MockInstance();
    private static final Connector connector;

    static {
        try {
            connector = instance.getConnector("root", "");
        } catch (AccumuloException e) {
            throw new RuntimeException(e);
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void beforeClass() throws TableExistsException, AccumuloSecurityException, AccumuloException {
        connector.tableOperations().create("deployments", false);
    }

    @Test
    public void testStagingWorkflow() throws Exception {
        AccumuloEzDeployerStore handler = new MockAccumuloStore(connector, "deployments");
        checkStatusCounts(handler, 0, 0, 0);

        ArtifactManifest manifest = TestUtils.createSampleArtifactManifest(ArtifactType.Thrift);

        //Stage the artifact
        DeploymentArtifact deployed = handler.writeArtifactToStore(
                manifest, TestUtils.createSampleAppTarBall(ArtifactType.Thrift), DeploymentStatus.Staged);
        Thread.sleep(3000);
        assertDeploymentMetadata(deployed.getMetadata(), ArtifactType.Thrift);
        assertRowAddedInAccumulo(ArtifactType.Thrift, DeploymentStatus.Staged);
        checkStatusCounts(handler, 1, 0, 0);
        FluentIterable<DeploymentMetadata> results = handler.getApplicationMetaDataFromStoreForAllVersions(
                ArtifactHelpers.getAppId(manifest), ArtifactHelpers.getServiceId(manifest));
        System.out.println("ALL METADATAS");
        printMetadatas(results);

        //Deploy the staged artifact
        deployed.getMetadata().setStatus(DeploymentStatus.Deployed);
        handler.updateDeploymentMetadata(deployed.getMetadata());
        Thread.sleep(3000);
        assertDeploymentMetadata(deployed.getMetadata(), ArtifactType.Thrift);
        assertRowAddedInAccumulo(ArtifactType.Thrift, DeploymentStatus.Deployed);
        checkStatusCounts(handler, 0, 1, 0);

        //Undeploy the artifact
        deployed.getMetadata().setStatus(DeploymentStatus.Undeployed);
        handler.updateDeploymentMetadata(deployed.getMetadata());
        Thread.sleep(3000);
        assertDeploymentMetadata(deployed.getMetadata(), ArtifactType.Thrift);
        assertRowAddedInAccumulo(ArtifactType.Thrift, DeploymentStatus.Undeployed);
        checkStatusCounts(handler, 0, 0, 1);

        //Clean-up
        handler.removeFromStore(deployed.getMetadata());
        checkStatusCounts(handler, 0, 0, 0);
        printTable();
    }

    /**
     * Prints out our test table.  Ignoring the data row.
     *
     * @throws TableNotFoundException
     */
    protected void printTable() throws TableNotFoundException {
        Scanner scanner = connector.createScanner("deployments", new Authorizations("U", "FOUO"));
        System.out.println(
                Joiner.on("\n\n").join(
                        FluentIterable.from(scanner).filter(
                                new Predicate<Map.Entry<Key, Value>>() {
                                    @Override
                                    public boolean apply(Map.Entry<Key, Value> input) {
                                        return !input.getKey().getColumnQualifier().toString().equals("data");
                                    }
                                }).transform(
                                new Function<Map.Entry<Key, Value>, String>() {
                                    @Override
                                    public String apply(Map.Entry<Key, Value> input) {
                                        return input.getKey().toString() + "=" + input.getValue().toString();
                                    }
                                })));
    }

    private void assertDeploymentMetadata(DeploymentMetadata deployed, ArtifactType type) {
        assertEquals(TestUtils.SERVICE_NAME, getServiceId(deployed));
        assertEquals(TestUtils.USER_NAME, deployed.getManifest().getUser());
        TestUtils.assertAppMetadata(deployed.getManifest(), type);
    }

    private void checkStatusCounts(
            AccumuloEzDeployerStore handler, int expectedStaged, int expectedDeployed, int expectedUndeployed)
            throws TException, DeploymentException {
        FluentIterable<DeploymentMetadata> metadatas =
                handler.getApplicationMetaDataMatching(EzDeployerStore.FieldName.Status, "Staged");
        printMetadatas(metadatas);
        assertEquals("Number of Staged apps was incorrect", expectedStaged, metadatas.size());
        metadatas = handler.getApplicationMetaDataMatching(EzDeployerStore.FieldName.Status, "Deployed");
        printMetadatas(metadatas);
        assertEquals("Number of Deployed apps was incorrect", expectedDeployed, metadatas.size());
        metadatas = handler.getApplicationMetaDataMatching(EzDeployerStore.FieldName.Status, "Undeployed");
        printMetadatas(metadatas);
        assertEquals("Number of Undeployed apps was incorrect", expectedUndeployed, metadatas.size());
    }

    private void printMetadatas(FluentIterable<DeploymentMetadata> metadatas) {
        for (DeploymentMetadata metadata : metadatas) {
            System.out.println(
                    String.format(
                            "Metadata: Status %s of %s %s", metadata.getStatus(),
                            metadata.getManifest().getApplicationInfo().getApplicationId(), metadata.getVersion()));
        }
    }

    private void assertRowAddedInAccumulo(ArtifactType type, DeploymentStatus status) throws Exception {
        TDeserializer deSerializer = new TDeserializer(new TCompactProtocol.Factory());
        Scanner scanner = connector.createScanner("deployments", new Authorizations("U", "FOUO"));
        Set<String> foundIndexes = Sets.newHashSet();
        Set<String> expectedIndexes = Sets.newHashSet(
                AccumuloEzDeployerStore.ARTIFACT_INDEX_APPLICATION_CQ.toString(),
                AccumuloEzDeployerStore.ARTIFACT_INDEX_USER_CQ.toString(),
                AccumuloEzDeployerStore.ARTIFACT_INDEX_SECURITY_CQ.toString(),
                AccumuloEzDeployerStore.ARTIFACT_INDEX_STATUS_CQ.toString());
        for (Map.Entry<Key, Value> i : scanner) {
            String cf = i.getKey().getColumnFamily().toString();
            if (cf.equals(AccumuloEzDeployerStore.ARTIFACT_CF.toString())) {
                assertEquals(getFqAppId(TestUtils.APP_NAME, TestUtils.SERVICE_NAME), i.getKey().getRow().toString());
                assertEquals("application", i.getKey().getColumnFamily().toString());
                String cq = i.getKey().getColumnQualifier().toString();
                Set<String> valid_cq = Sets.newHashSet("data", "metadata");
                if (cq.equals("data")) {
                    DeploymentArtifact deploymentArtifact = new DeploymentArtifact();
                    deSerializer.deserialize(deploymentArtifact, i.getValue().get());
                    TestUtils.assertDeploymentArtifact(deploymentArtifact, type);
                } else if (cq.equals("metadata")) {
                    DeploymentMetadata artifact = new DeploymentMetadata();
                    deSerializer.deserialize(artifact, i.getValue().get());
                    assertDeploymentMetadata(artifact, type);
                } else {
                    printTable();
                    assertTrue("CF must be on of these values: " + valid_cq, valid_cq.contains(cq));
                }
            } else if (cf.startsWith(AccumuloEzDeployerStore.ARTIFACT_INDEX_CF.toString())) {
                String cq = i.getKey().getColumnQualifier().toString();
                if (cf.endsWith(AccumuloEzDeployerStore.ARTIFACT_INDEX_APPLICATION_CQ.toString())) {
                    assertEquals(TestUtils.APP_NAME, i.getKey().getRow().toString());
                    assertEquals(getFqAppId(TestUtils.APP_NAME, TestUtils.SERVICE_NAME), i.getValue().toString());
                    foundIndexes.add(AccumuloEzDeployerStore.ARTIFACT_INDEX_APPLICATION_CQ.toString());
                } else if (cf.endsWith(AccumuloEzDeployerStore.ARTIFACT_INDEX_USER_CQ.toString())) {
                    assertEquals(TestUtils.USER_NAME, i.getKey().getRow().toString());
                    assertEquals(getFqAppId(TestUtils.APP_NAME, TestUtils.SERVICE_NAME), i.getValue().toString());
                    foundIndexes.add(AccumuloEzDeployerStore.ARTIFACT_INDEX_USER_CQ.toString());
                } else if (cf.endsWith(AccumuloEzDeployerStore.ARTIFACT_INDEX_SECURITY_CQ.toString())) {
                    assertEquals(TestUtils.SECURITY_ID, i.getKey().getRow().toString());
                    assertEquals(getFqAppId(TestUtils.APP_NAME, TestUtils.SERVICE_NAME), i.getValue().toString());
                    foundIndexes.add(AccumuloEzDeployerStore.ARTIFACT_INDEX_SECURITY_CQ.toString());
                } else if (cf.endsWith(AccumuloEzDeployerStore.ARTIFACT_INDEX_STATUS_CQ.toString())) {
                    assertEquals(status.toString(), i.getKey().getRow().toString());
                    assertEquals(getFqAppId(TestUtils.APP_NAME, TestUtils.SERVICE_NAME), i.getValue().toString());
                    foundIndexes.add(AccumuloEzDeployerStore.ARTIFACT_INDEX_STATUS_CQ.toString());
                } else {
                    printTable();
                    fail("Unknown CF/CQ written to accumulo table: " + cf + "/" + cq);
                }
            } else if (cf.equals("VERSION")) {
                String cq = i.getKey().getColumnQualifier().toString();
                String rowId = i.getKey().getRow().toString();
                assertEquals("VERSION", cq);
                assertEquals("00000VERSION", rowId);
                assertEquals("2", i.getValue().toString());
            } else {
                printTable();
                fail("Unknown CF written to accumulo table: " + cf);
            }
        }
        assertEquals(expectedIndexes, foundIndexes);
    }

    public static class MockAccumuloStore extends AccumuloEzDeployerStore {
        public MockAccumuloStore(Connector connector, String deployments)
                throws IOException, DeploymentException, EzConfigurationLoaderException {
            super(
                    connector, deployments, new EzDeployerConfiguration(
                            new EzConfiguration(
                                    new ClasspathConfigurationLoader("/test.properties")).getProperties()),
                    new LocalFileArtifactWriter());
        }

        @Override
        protected long createVersionNumber() {
            TestUtils.sampleVersion = super.createVersionNumber();
            return TestUtils.sampleVersion;
        }
    }
}
