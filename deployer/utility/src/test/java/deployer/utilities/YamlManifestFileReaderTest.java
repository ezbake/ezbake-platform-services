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

import com.google.common.collect.Sets;
import ezbake.deployer.utilities.UserProvider;
import ezbake.deployer.utilities.YamlManifestFileReader;
import ezbake.deployer.utilities.YmlKeys;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.Language;
import ezbake.services.deploy.thrift.ResourceReq;
import ezbake.services.deploy.thrift.ResourceRequirements;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static ezbake.deployer.utilities.ArtifactHelpers.getAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getDataSets;
import static ezbake.deployer.utilities.ArtifactHelpers.getLanguage;
import static ezbake.deployer.utilities.ArtifactHelpers.getSecurityId;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class YamlManifestFileReaderTest {

    public static final String RESOURCE_BASE = "ezbake/deployer/cli/";

    private UserProvider getUserProvider() {
        UserProvider userProvider = EasyMock.createMock(UserProvider.class);
        EasyMock.expect(userProvider.getUser()).andReturn("foobarUser").anyTimes();
        EasyMock.replay(userProvider);
        return userProvider;
    }

    @Test
    public void testReaderOfExternalAzkabanManifest() throws IOException {
        final YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        final List<ArtifactManifest> artifacts;

        try (InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testAzkabanExternal.yml")) {
            artifacts = reader.readFile(ymlIn);
        }

        assertEquals(1, artifacts.size());

        final ArtifactManifest manifest = artifacts.get(0);
        assertEquals("externalAzTest", getAppId(manifest));
        assertEquals(ArtifactType.Batch, manifest.getArtifactType());
        assertEquals("foobarUser", manifest.getUser());
        assertEquals("externalJobTest", getServiceId(manifest));
        assertEquals("foobarSecurityId", getSecurityId(manifest));
    }

    @Test
    public void testReaderOfInternalAzkabanManifest() throws IOException {
        final YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        final List<ArtifactManifest> artifacts;

        try (InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testAzkabanInternal.yml")) {
            artifacts = reader.readFile(ymlIn);
        }

        assertEquals(1, artifacts.size());

        final ArtifactManifest manifest = artifacts.get(0);
        assertEquals("internalAzTest", getAppId(manifest));
        assertEquals(ArtifactType.Batch, manifest.getArtifactType());
        assertEquals("foobarUser", manifest.getUser());
        assertEquals("internalJobTest", getServiceId(manifest));
        assertEquals("foobarSecurityId", getSecurityId(manifest));
        assertEquals("09/18/2014", manifest.getBatchJobInfo().getStartDate());
        assertEquals("5,00,pm,utc", manifest.getBatchJobInfo().getStartTime());
        assertEquals("1d", manifest.getBatchJobInfo().getRepeat());
        assertEquals("optional_name_here", manifest.getBatchJobInfo().getFlowName());

    }

    @Test
    public void testReaderOfSingleThriftServiceManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testSingleThriftService.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("foobarAppId", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("foobarServiceId", getServiceId(art));
            assertEquals("foobarSecurityId", getSecurityId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            Set<String> dsets = Sets.newHashSet(getDataSets(art));
            assertEquals(Sets.newHashSet("dataset1", "dataset2"), dsets);
            assertEquals(true, art.artifactInfo.isPurgeable());
            assertFalse("Database Info should not be set if no database is provided", art.isSetDatabaseInfo());
        }
    }

    @Test
    public void testReaderOfAnotherSingleThriftServiceManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "anotherSingleThrift.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("core", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("blobstore", getServiceId(art));
            assertEquals("blah", getSecurityId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            assertEquals(true, art.artifactInfo.isPurgeable());
            assertNull(getDataSets(art));
            assertTrue("DatabaseInfo should be set since Database was specified", art.isSetDatabaseInfo());
        }
    }

    @Test
    public void testReaderWithOverrides() throws IOException {
        HashMap<String, Object> overrides = new HashMap<>();
        overrides.put(YmlKeys.RootManifestKeys.securityId.getName(), "123456789");
        overrides.put(YmlKeys.RootManifestKeys.applicationName.getName(), "MyAppName");

        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider(), overrides);
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "anotherThriftMinFields.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("MyAppName", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("blobstore", getServiceId(art));
            assertEquals("123456789", getSecurityId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            assertNull(getDataSets(art));
        }
    }

    @Test
    public void testReaderWithCommonService() throws IOException {
        HashMap<String, Object> overrides = new HashMap<>();
        overrides.put(YmlKeys.RootManifestKeys.securityId.getName(), "123456789");
        overrides.put(YmlKeys.RootManifestKeys.applicationName.getName(), "MyAppName");

        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider(), overrides);
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "commonservice.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("common_services", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("blobstore", getServiceId(art));
            assertEquals("123456789", getSecurityId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            assertNull(getDataSets(art));
        }
    }

    @Test
    public void testReaderOfSingleWebServiceManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testSingleWebApp.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        ArtifactManifest art = artifacts.get(0);
        assertEquals("foobarAppId", getAppId(art));
        assertEquals(ArtifactType.WebApp, art.getArtifactType());
        assertEquals(Language.Java, getLanguage(art));
        assertEquals(3, art.getScaling().getNumberOfInstances());
        assertEquals("foobarUser", art.getUser());
        assertEquals("myWebService", getServiceId(art));
        assertEquals("foobarSecurityId", getSecurityId(art));
        ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
        assertEquals(ResourceReq.small, rr.getCpu());
        assertEquals(ResourceReq.small, rr.getMem());
        assertEquals(ResourceReq.small, rr.getDisk());
        Set<String> dsets = Sets.newHashSet(getDataSets(art));
        assertEquals(Sets.newHashSet("dataset1", "dataset2"), dsets);
        assertEquals(Sets.newHashSet("myApp.properties"), Sets.newHashSet(art.getArtifactInfo().getConfig()));
        assertEquals(Sets.newHashSet("auth3"), Sets.newHashSet(art.getApplicationInfo().getAuths()));
        assertEquals("MyApp.war", art.getArtifactInfo().getBin());
        assertEquals(true, art.getWebAppInfo().isStickySession());
        assertEquals(200, art.getWebAppInfo().getTimeout());
        assertEquals(100, art.getWebAppInfo().getUploadFileSize());
        assertEquals(true, art.getWebAppInfo().isChunkedTransferEncodingDisabled());
        assertEquals(true, art.getWebAppInfo().isWebsocketSupportDisabled());
        assertEquals(true, art.getArtifactInfo().isSystemLogfileDisabled());
    }

    @Test
    public void testDefaultWebManifestValues() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testSingleWebApp2.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        ArtifactManifest art = artifacts.get(0);
        assertEquals(false, art.getWebAppInfo().isChunkedTransferEncodingDisabled());
        assertEquals(5, art.getWebAppInfo().getUploadFileSize());
        assertEquals(false, art.getWebAppInfo().isStickySession());
        assertEquals(60, art.getWebAppInfo().getTimeout());
        assertEquals(2, art.getWebAppInfo().getTimeoutRetries());
        assertEquals(false, art.getWebAppInfo().isWebsocketSupportDisabled());
        assertEquals(false, art.getArtifactInfo().isSystemLogfileDisabled());
    }

    @Test
    public void testReaderOfSingleDatasetManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testDataset.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("foobarAppId", getAppId(art));
            assertEquals(ArtifactType.DataSet, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("foobarDataset", getServiceId(art));
            assertEquals("foobarSecurityId", getSecurityId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            assertEquals("MongoDB", art.getDatabaseInfo().getDatabaseType());
        }
    }
/*
      @Test
    public void testReaderOfSingleCustomServiceManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testSingleThriftService.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("foobarAppId", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("foobarServiceId", getServiceId(art));
            assertEquals("foobarSecurityId", getServiceId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            Set<String> dsets = Sets.newHashSet(getDataSets(art));
            assertEquals(Sets.newHashSet("dataset1", "dataset2"), dsets);
        }
    }


    @Test
    public void testReaderOfSingleFrackServiceManifest() throws IOException {
        YamlManifestFileReader reader = new YamlManifestFileReader(getUserProvider());
        InputStream ymlIn = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "testSingleThriftService.yml");
        List<ArtifactManifest> artifacts = reader.readFile(ymlIn);

        assertEquals(1, artifacts.size());

        for (ArtifactManifest art : artifacts) {
            assertEquals("foobarAppId", getAppId(art));
            assertEquals(ArtifactType.Thrift, art.getArtifactType());
            assertEquals(Language.Java, getLanguage(art));
            assertEquals(3, art.getScaling().getNumberOfInstances());
            assertEquals("foobarUser", art.getUser());
            assertEquals("foobarServiceId", getServiceId(art));
            assertEquals("foobarSecurityId", getServiceId(art));
            ResourceRequirements rr = art.getArtifactInfo().getResourceRequirements();
            assertEquals(ResourceReq.small, rr.getCpu());
            assertEquals(ResourceReq.small, rr.getMem());
            assertEquals(ResourceReq.small, rr.getDisk());
            Set<String> dsets = Sets.newHashSet(getDataSets(art));
            assertEquals(Sets.newHashSet("dataset1", "dataset2"), dsets);
        }
    }
    */
}
