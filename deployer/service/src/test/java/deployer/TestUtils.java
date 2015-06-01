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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import deployer.publishers.TarFileChecker;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.reverseproxy.thrift.UpstreamServerRegistration;
import ezbake.services.deploy.thrift.*;
import ezbake.thrift.ThriftTestUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static ezbake.deployer.utilities.ArtifactHelpers.getLanguage;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestUtils {
    public static final String SAMPLE_SSL_DATA = "Not actually a JKS file for sake of easiness of test";
    public static final String SAMPLE_JAR_DATA = "This is not really a jar for the sake of easiness of test";
    public static final String SAMPLE_CONF_DATA = "sample.key=sample value\nsample.pass=blah blah";
    public static final String SAMPLE_STANDALONE_DATA = "<?xml version='1.0' encoding='UTF-8'?><server><subsystem xmlns=urn:jboss:domain:jsr77:1.0/></server>";
    public static final File GIT_TEST_DIR = getATempDirectory();
    private static final short NUMBER_INSTANCES = 1;
    public static long sampleVersion = 406252800;
    public static String APP_NAME = "sampleapp";
    public static String SERVICE_NAME = "sampleservice";
    public static String USER_NAME = "sample.user";
    public static String SECURITY_ID = "sampleSecurityId";
    public static String CONFIG_DIRECTORY = Utilities.CONFIG_DIRECTORY;

    public static String getOpenShiftAppName() {
        return SERVICE_NAME;
    }

    public static String buildOpenShiftAppName(int instanceNum) {
        return SERVICE_NAME + "xx" + Integer.toString(instanceNum);
    }

    public static String getOpenShiftDomainName() {
        return APP_NAME;
    }

    public static DeploymentMetadata createSampleApplicationMetadata(ArtifactType type) {
        return createSampleApplicationMetadata(type, NUMBER_INSTANCES);
    }

    public static DeploymentMetadata createSampleApplicationMetadata(ArtifactType type, short numInstances) {
        return new DeploymentMetadata(createSampleArtifactManifest(type, numInstances), Long.toString(sampleVersion));
    }

    public static ArtifactManifest createSampleArtifactManifest(ArtifactType type) {
        return createSampleArtifactManifest(type, NUMBER_INSTANCES);
    }

    public static ArtifactManifest createSampleArtifactManifest(ArtifactType type, short numInstances) {
        ArtifactManifest metadata = new ArtifactManifest();
        metadata.setUser(USER_NAME);
        metadata.setArtifactType(type);
        ArtifactInfo info = new ArtifactInfo();
        info.setLanguage(Language.Java);
        info.setPurgeable(true);
        metadata.setArtifactInfo(info);
        metadata.setScaling(new Scaling().setNumberOfInstances(numInstances));
        ApplicationInfo applicationInfo = new ApplicationInfo();
        applicationInfo.setServiceId(SERVICE_NAME);
        applicationInfo.setApplicationId(APP_NAME);
        applicationInfo.setSecurityId(SECURITY_ID);
        metadata.setApplicationInfo(applicationInfo);
        switch (type) {
            case DataSet:
                metadata.setDatabaseInfo(new DatabaseInfo());
                metadata.getDatabaseInfo().setDatabaseType("MongoDB");
                break;
            case WebApp:
                WebAppInfo webAppInfo = new WebAppInfo();
                metadata.setWebAppInfo(webAppInfo);
                break;
        }
        return metadata;
    }

    public static ByteBuffer createSampleAppTarBall(ArtifactType type) throws IOException, ArchiveException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8096);
        CompressorOutputStream gzs = new GzipCompressorOutputStream(bos);
        ArchiveOutputStream aos = new TarArchiveOutputStream(gzs);
        {
            TarArchiveEntry nextEntry = new TarArchiveEntry(CONFIG_DIRECTORY + "/app.conf");
            byte[] sampleConf = SAMPLE_CONF_DATA.getBytes();
            nextEntry.setSize(sampleConf.length);
            aos.putArchiveEntry(nextEntry);
            IOUtils.write(sampleConf, aos);
            aos.closeArchiveEntry();
        }
        if (type != ArtifactType.WebApp) {
            TarArchiveEntry nextEntry = new TarArchiveEntry("bin/myApplication.jar");
            byte[] jarData = SAMPLE_JAR_DATA.getBytes();
            nextEntry.setSize(jarData.length);
            aos.putArchiveEntry(nextEntry);
            IOUtils.write(jarData, aos);
            aos.closeArchiveEntry();
        } else {
            TarArchiveEntry nextEntry = new TarArchiveEntry("deployments/ROOT.war");
            byte[] jarData = SAMPLE_JAR_DATA.getBytes();
            nextEntry.setSize(jarData.length);
            aos.putArchiveEntry(nextEntry);
            IOUtils.write(jarData, aos);
            aos.closeArchiveEntry();
        }
        aos.finish();
        gzs.close();
        bos.flush();
        return ByteBuffer.wrap(bos.toByteArray());
    }


    public static ByteBuffer createSampleOpenShiftWebAppTarBall() throws IOException, ArchiveException {
        ByteArrayInputStream bis = new ByteArrayInputStream(createSampleAppTarBall(ArtifactType.WebApp).array());
        CompressorInputStream cis = new GzipCompressorInputStream(bis);
        ArchiveInputStream ais = new TarArchiveInputStream(cis);

        ByteArrayOutputStream bos = new ByteArrayOutputStream(bis.available() + 2048);
        CompressorOutputStream cos = new GzipCompressorOutputStream(bos);
        ArchiveOutputStream aos = new TarArchiveOutputStream(cos);

        ArchiveEntry nextEntry;
        while ((nextEntry = ais.getNextEntry()) != null) {
            aos.putArchiveEntry(nextEntry);
            IOUtils.copy(ais, aos);
            aos.closeArchiveEntry();
        }
        ais.close();
        cis.close();
        bis.close();

        TarArchiveEntry entry = new TarArchiveEntry(Paths.get(".openshift", CONFIG_DIRECTORY, "/standalone.xml").toFile());
        byte[] xmlData = SAMPLE_STANDALONE_DATA.getBytes();
        entry.setSize(xmlData.length);
        aos.putArchiveEntry(entry);
        IOUtils.write(xmlData, aos);
        aos.closeArchiveEntry();

        aos.finish();
        cos.close();
        bos.flush();
        return ByteBuffer.wrap(bos.toByteArray());
    }


    public static ByteBuffer createSampleOpenShiftWebAppTarBallWithEmptyFiles(String[] filepaths) throws IOException, ArchiveException {
        ByteArrayInputStream bis = new ByteArrayInputStream(createSampleAppTarBall(ArtifactType.WebApp).array());
        CompressorInputStream cis = new GzipCompressorInputStream(bis);
        ArchiveInputStream ais = new TarArchiveInputStream(cis);

        ByteArrayOutputStream bos = new ByteArrayOutputStream(bis.available() + 2048);
        CompressorOutputStream cos = new GzipCompressorOutputStream(bos);
        ArchiveOutputStream aos = new TarArchiveOutputStream(cos);

        ArchiveEntry nextEntry;
        while ((nextEntry = ais.getNextEntry()) != null) {
            aos.putArchiveEntry(nextEntry);
            IOUtils.copy(ais, aos);
            aos.closeArchiveEntry();
        }
        ais.close();
        cis.close();
        bis.close();

        TarArchiveEntry entry = new TarArchiveEntry(Paths.get(".openshift", CONFIG_DIRECTORY, "/standalone.xml").toFile());
        byte[] xmlData = SAMPLE_STANDALONE_DATA.getBytes();
        entry.setSize(xmlData.length);
        aos.putArchiveEntry(entry);
        IOUtils.write(xmlData, aos);

        for(int i=0; i < filepaths.length; i++)
        {
            String filepath = filepaths[i];
            TarArchiveEntry emptyEntry = new TarArchiveEntry(Paths.get(filepath).toFile());
            byte[] emptyData = "".getBytes();
            emptyEntry.setSize(emptyData.length);
            aos.putArchiveEntry(emptyEntry);
            IOUtils.write(emptyData, aos);
            aos.closeArchiveEntry();
        }

        aos.finish();
        cos.close();
        bos.flush();
        return ByteBuffer.wrap(bos.toByteArray());
    }

    public static DeploymentArtifact createSampleDeploymentArtifact(ArtifactType type, short number)
            throws IOException {
        try {
            return new DeploymentArtifact(createSampleApplicationMetadata(type, number), createSampleAppTarBall(type));
        } catch (ArchiveException e) {
            throw new IOException(e);
        }
    }

    public static DeploymentArtifact createSampleDeploymentArtifact(ArtifactType type) throws IOException {
        try {
            return new DeploymentArtifact(createSampleApplicationMetadata(type), createSampleAppTarBall(type));
        } catch (ArchiveException e) {
            throw new IOException(e);
        }
    }

    public static DeploymentArtifact createSampleOpenShiftDeploymentArtifact() throws IOException {
        try {
            return new DeploymentArtifact(createSampleApplicationMetadata(ArtifactType.WebApp),
                    createSampleOpenShiftWebAppTarBall());
        } catch (ArchiveException e) {
            throw new IOException(e);
        }
    }

    public static List<ArtifactDataEntry> sampleOpenShiftConfiguration() {
        return Lists.newArrayList(
                new ArtifactDataEntry(new TarArchiveEntry(Paths.get(CONFIG_DIRECTORY, "standalone.xml").toString()),
                        SAMPLE_STANDALONE_DATA.getBytes())
        );
    }

    public static List<ArtifactDataEntry> sampleSSL() {
        byte[] sampleCert = SAMPLE_SSL_DATA.getBytes();
        return Lists.newArrayList(
                new ArtifactDataEntry(new TarArchiveEntry(CONFIG_DIRECTORY + "/ssl/" + SERVICE_NAME + ".jks"), sampleCert));
    }

    public static void assertAppMetadata(ArtifactManifest metadata, ArtifactType artifactType) {
        assertEquals(TestUtils.SERVICE_NAME, getServiceId(metadata));
        assertEquals(artifactType, metadata.getArtifactType());
        assertEquals(Language.Java, getLanguage(metadata));
    }

    public static void assertDeploymentArtifact(DeploymentArtifact deploymentArtifact, ArtifactType artifactType) {
        assertEquals(TestUtils.SERVICE_NAME, getServiceId(deploymentArtifact));
        assertEquals(TestUtils.USER_NAME, deploymentArtifact.getMetadata().getManifest().getUser());
        assertAppMetadata(deploymentArtifact.getMetadata().getManifest(), artifactType);
    }

    public static TarFileChecker getSampleTarBallChecker(final ArtifactType type) {
        return new TarFileChecker() {
            @Override
            public void verify(TarArchiveEntry entry, InputStream inputStream) throws Exception {
                String fileName = entry.getName();
                if (fileName.equals(CONFIG_DIRECTORY + "/app.conf")) {
                    verifyConf(inputStream);
                } else if (fileName.equals("bin/myApplication.jar") && type != ArtifactType.WebApp) {
                    verifyApplicationJar( inputStream);
                } else if (fileName.equals("deployments/ROOT.war") && type == ArtifactType.WebApp) {
                    verifyApplicationJar(inputStream);
                } else if (fileName.equals(CONFIG_DIRECTORY + "/application.properties")) {
                    verifyAppProperties(inputStream);
                } else if (fileName.equals(CONFIG_DIRECTORY + "/ssl/" + TestUtils.SERVICE_NAME + ".jks")) {
                    verifySSLCert(inputStream);
                } else if (type == ArtifactType.WebApp && fileName.equals(".openshift/config/standalone.xml")) {
                    verifyStandaloneFile(inputStream);
                } else {
                    fail("Unexpected file in tar ball: " + fileName);
                }
            }

            private void verifyAppProperties(InputStream inputStream) throws IOException {
                Properties expected = new Properties();
                expected.put("ezbake.security.ssl.dir", CONFIG_DIRECTORY + "/ssl/" + TestUtils.SECURITY_ID);
                expected.put("service.name", TestUtils.SERVICE_NAME);
                expected.put("ezbake.security.app.id", TestUtils.SECURITY_ID);
                expected.put("application.name", TestUtils.APP_NAME);
                expected.put("ezbake.application.version", Long.toString(TestUtils.sampleVersion));

                Properties data = new Properties();
                data.load(inputStream);

                assertEquals(expected, data);
            }

            private void verifySSLCert(InputStream inputStream) throws IOException {
                String data = IOUtils.toString(inputStream);
                assertEquals(data, TestUtils.SAMPLE_SSL_DATA);
            }

            private void verifyApplicationJar(InputStream inputStream) throws IOException {
                String data = IOUtils.toString(inputStream);
                assertEquals(data, TestUtils.SAMPLE_JAR_DATA);
            }

            private void verifyConf(InputStream inputStream) throws IOException {
                String data = IOUtils.toString(inputStream);
                assertEquals(data, TestUtils.SAMPLE_CONF_DATA);
            }

            private void verifyStandaloneFile(InputStream inputStream) throws  IOException {
                assertEquals(IOUtils.toString(inputStream), TestUtils.SAMPLE_STANDALONE_DATA);
            }
        };
    }

    public static IAnswer<EzPublisher> singleAnswerFor(
            final ArtifactType type, final EzPublisher matched, final EzPublisher unmatched) {
        return new IAnswer<EzPublisher>() {
            @Override
            public EzPublisher answer() throws Throwable {
                ArtifactType passedType = (ArtifactType) getCurrentArguments()[0];
                return passedType.equals(type) ? matched : unmatched;
            }
        };
    }

    public static File getATempDirectory() {
        try {
            File testOutputDir = Files.resolve(getTargetDir(), "test-output");
            if (!Files.exists(testOutputDir)) {
                Files.createDirectories(testOutputDir);
            }
            return Files.createTempDirectory(testOutputDir, "git-test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File getTargetDir() throws IOException {
        File currentPath = Files.get("").getAbsoluteFile();
        while (currentPath != null && currentPath.getName() != null && !currentPath.getName().equals("target")) {
            File targetMaybe = Files.resolve(currentPath, "target");
            if (Files.exists(targetMaybe)) {
                return targetMaybe;
            }
            currentPath = currentPath.getParentFile();
        }
        if (currentPath == null || currentPath.toString().equals("/")) {
            // target Not found anywhere up the tree.
            currentPath = Files.resolve(Files.get("").getAbsoluteFile(), "target");
            Files.createDirectories(currentPath);
        }
        return currentPath;
    }

    public static UpstreamServerRegistration eqRegistration(
            final String appNamePrefix, final String upstreamHostAndPort, final String upstreamPath) {
        EasyMock.reportMatcher(
                new IArgumentMatcher() {

                    @Override
                    public boolean matches(Object actual) {
                        if (!(actual instanceof UpstreamServerRegistration)) {
                            return false;
                        }
                        UpstreamServerRegistration registration = (UpstreamServerRegistration) actual;
                        return registration.getAppName().startsWith(appNamePrefix) &&
                                registration.getUpstreamHostAndPort().equals(upstreamHostAndPort) &&
                                registration.getUpstreamPath().equals(upstreamPath) &&
                                !registration.isSetUserFacingUrlPrefix();
                    }

                    @Override
                    public void appendTo(StringBuffer buffer) {
                        UpstreamServerRegistration expected = new UpstreamServerRegistration();
                        expected.setAppName(appNamePrefix);
                        expected.setUpstreamHostAndPort(upstreamHostAndPort);
                        expected.setUpstreamPath(upstreamPath);
                        buffer.append(expected);
                    }
                });
        return null;
    }

    public static List<RhcApplication> getExistingInstances(
            AppDirs[] theGits, final IApplication instance, final IDomain domain) {
        return Lists.newArrayList(
                Iterables.transform(
                        Lists.newArrayList(theGits), new Function<AppDirs, RhcApplication>() {
                            @Override
                            public RhcApplication apply(AppDirs input) {
                                return new RhcApplication(instance, domain, input.testDir, null);
                            }
                        }));
    }

    public static AppDirs[] makeNumberGits(int number, String baseName) throws GitAPIException {
        List<AppDirs> gits = Lists.newArrayListWithCapacity(number);
        for (int i = 0; i < number; i++) {
            File remoteTestDir = Files.resolve(GIT_TEST_DIR, baseName + "xx" + i + ".git");
            File testDir = Files.resolve(GIT_TEST_DIR, baseName + "xx" + i);
            Files.createDirectories(testDir);
            Git remoteGit = Git.init().setBare(true).setDirectory(remoteTestDir).call();

            Git git = Git.cloneRepository().setURI(remoteTestDir.toURI().toString()).setDirectory(testDir).call();
            gits.add(new AppDirs(remoteGit, git, testDir));
        }
        return gits.toArray(new AppDirs[gits.size()]);
    }

    public static EzSecurityToken getTestEzSecurityToken() {
        List<String> auths = new ArrayList<String>();
        auths.add("U");

        return ThriftTestUtils.generateTestSecurityToken("SecurityClientTest", "SecurityClientTest", auths);
    }

    public static class AppDirs {
        public Git remoteGit;
        public Git gitRepo;
        public File testDir;

        public AppDirs(Git remoteGit, Git gitRepo, File testDir) {
            this.remoteGit = remoteGit;
            this.gitRepo = gitRepo;
            this.testDir = testDir;
        }
    }
}
