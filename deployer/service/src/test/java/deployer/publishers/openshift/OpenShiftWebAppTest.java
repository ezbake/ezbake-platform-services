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

 package deployer.publishers.openshift;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.openshift.client.ApplicationScale;
import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import com.openshift.client.IEnvironmentVariable;
import com.openshift.client.IUser;
import com.openshift.client.cartridge.EmbeddableCartridge;
import com.openshift.client.cartridge.IEmbeddedCartridge;
import com.openshift.client.cartridge.StandaloneCartridge;
import com.openshift.internal.client.GearProfile;
import deployer.TestUtils;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzReverseProxyRegister;
import ezbake.deployer.publishers.artifact.JavaWebAppArtifactContentsPublisher;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.reverseproxy.thrift.EzReverseProxy;
import ezbake.reverseproxy.thrift.EzReverseProxyConstants;
import ezbake.reverseproxy.thrift.UpstreamServerRegistration;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.eclipse.jgit.api.Git;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpenShiftWebAppTest {
    private static final Logger log = LoggerFactory.getLogger(RhcApplicationGitRepoModificationsTest.class);

    @Test
    public void testWithExisting0() throws Exception {
        testSingleInstance(0);
    }

    @Test
    public void testWithExisting1() throws Exception {
        testSingleInstance(1);
    }

    @Test
    public void testWithExisting3() throws Exception {
        testSingleInstance(3);
    }

    @Test
    public void testDefaultJbossLogging() throws Exception {
        DeploymentArtifact deploymentArtifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.WebApp);
        deploymentArtifact.getMetadata().getManifest().getArtifactInfo().setSystemLogfileDisabled(true);
        File testDir = runSingleInstanceTest(2, deploymentArtifact);
        assertGitRepositoryFilesForUpdateGitRepoTest(testDir, true);
    }

    private <T extends TServiceClient> ThriftClientPool createMockPool(int timesRequested, String appName, String clientName, T client) throws TException {
        ThriftClientPool mockPool = createMock(ThriftClientPool.class);
        expect(mockPool.getClient(eq(appName), eq(clientName), anyObject(Class.class))).andReturn(client).times(timesRequested);
        mockPool.returnToPool(client);
        expectLastCall().times(timesRequested);
        replay(mockPool);
        return mockPool;
    }

    private void testSingleInstance(int existing) throws Exception {
        DeploymentArtifact deploymentArtifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.WebApp);
        File testDir = runSingleInstanceTest(existing, deploymentArtifact);
        assertGitRepositoryFilesForUpdateGitRepoTest(testDir, false);
    }

    private File runSingleInstanceTest(int existing, DeploymentArtifact deploymentArtifact) throws Exception {
        String strExisting = Integer.toString(existing);
        File remoteTestDir = Files.resolve(TestUtils.GIT_TEST_DIR, strExisting, "webappviaOpenshift.git");
        File testDir = Files.resolve(TestUtils.GIT_TEST_DIR, strExisting, "webappviaOpenshift");
        System.out.println(testDir);
        Files.createDirectories(testDir);

        ArtifactHelpers.addFilesToArtifact(deploymentArtifact,
                new JavaWebAppArtifactContentsPublisher().generateEntries(deploymentArtifact));

        TestUtils.AppDirs[] theGits = TestUtils.makeNumberGits(existing, existing + "/webappviaOpenshift");

        Git.init().setBare(true).setDirectory(remoteTestDir).call();

        Git git = Git.cloneRepository()
                .setURI(remoteTestDir.toURI().toString())
                .setDirectory(testDir).call();

        Rhc rhc = createMock(Rhc.class);

        IApplication instance = createMock(IApplication.class);
        IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);
        EzReverseProxy.Client ezReverseProxyClient = createMock(EzReverseProxy.Client.class);

        ThriftClientPool clientPool = createMockPool((existing >= 2 ? existing : 1), "EzBakeFrontend",
                EzReverseProxyConstants.SERVICE_NAME, ezReverseProxyClient);
        EzReverseProxyRegister ezReverseProxyRegister = new EzReverseProxyRegister(new EzDeployerConfiguration(new Properties()), clientPool);


        expect(instance.getName()).andReturn("UnitTestApplication").anyTimes();
        expect(instance.getApplicationUrl()).andReturn("http://unit-test.local/").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();
        if (existing == 0 || existing == 1) {
            expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(),
                    TestUtils.getOpenShiftDomainName())).andReturn(Collections.EMPTY_LIST).once();
        } else {
            expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(),
                    TestUtils.getOpenShiftDomainName())).andReturn(
                    TestUtils.getExistingInstances(theGits, instance, domain)).once();
            instance.stop();
            expectLastCall().times(existing - 1);
            instance.destroy();
            expectLastCall().times(existing - 1);
            ezReverseProxyClient.removeUpstreamServerRegistration(eqRegistration("example.local/" + TestUtils.SERVICE_NAME + "/",
                    TestUtils.SERVICE_NAME, "unit-test.local:443", ""));
            expectLastCall().times(existing - 1);
        }

        ezReverseProxyClient.addUpstreamServerRegistration(eqRegistration("example.local/" + TestUtils.SERVICE_NAME + "/",
                TestUtils.SERVICE_NAME, "unit-test.local:443", ""));

        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(0), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("jbossas"), ApplicationScale.NO_SCALE, GearProfile.SMALL)).andReturn(
                new RhcApplication(git, instance, domain, testDir, null)
        ).once();


        expect(instance.getEnvironmentVariable("OPENSHIFT_GEAR_DNS"))
                .andReturn(envVariableValue("OPENSHIFT_GEAR_DNS", "unit-test.local")).anyTimes();
        expect(instance.getEnvironmentVariable("OPENSHIFT_JAVA_THRIFTRUNNER_TCP_PROXY_PORT"))
                .andReturn(envVariableValue("OPENSHIFT_JAVA_THRIFTRUNNER_TCP_PROXY_PORT", "32456")).anyTimes();
        expect(instance.getEnvironmentVariables()).andReturn(new HashMap<String, IEnvironmentVariable>()).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME))
                .andReturn(envVariableValue("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME)).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME))
                .andReturn(envVariableValue("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME)).anyTimes();
        expect(instance.getEmbeddedCartridges()).andReturn(new ArrayList<IEmbeddedCartridge>()).anyTimes();

        IEmbeddedCartridge cart = createMock(IEmbeddedCartridge.class);
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("logstash"))).andReturn(cart).anyTimes();
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("cron"))).andReturn(cart).anyTimes();
        replay(instance, domain, mockUser, ezReverseProxyClient, rhc);

        EzOpenShiftPublisher publisher = new EzOpenShiftPublisherMock(rhc, ezReverseProxyRegister);
        publisher.publish(deploymentArtifact, ThriftTestUtils.generateTestSecurityToken("U"));
        verify(instance, domain, mockUser, ezReverseProxyClient, rhc, clientPool);
        return testDir;
    }

    private IEnvironmentVariable envVariableValue(String name, String value) {
        IEnvironmentVariable environmentVariable = createMock(IEnvironmentVariable.class);
        expect(environmentVariable.getName()).andReturn(name).anyTimes();
        expect(environmentVariable.getValue()).andReturn(value).anyTimes();
        replay(environmentVariable);
        return environmentVariable;
    }

    private void assertGitRepositoryFilesForUpdateGitRepoTest(final File testDir, boolean excludeLogback) throws IOException {
        final File gitDbDir = Files.resolve(testDir, ".git");
        final File AppJar = Files.get("deployments", "ROOT.war");
        final File AppConf = Files.get(TestUtils.CONFIG_DIRECTORY, "app.conf");
        final File versionFile = Files.get("deployed_version.txt");
        final File standaloneXml = Files.get(".openshift", "config", "standalone.xml");
        final File java7Marker = Files.get(".openshift", "markers", "java7");
        final File prebuildHook = Files.get(".openshift", "action_hooks", "pre_build");
        final File tokenFile = Files.get("config", "openshift.properties");
        final File logbackXml = Files.get("config", "logback.xml");
        final File prestartHook = Files.get(".openshift", "action_hooks", "pre_start_jbossas");
        final File prerestartHook = Files.get(".openshift", "action_hooks", "pre_restart_jbossas");
        final File crontab = Files.get(".openshift", "cron", "daily", "cron");

        Set<File> expectedFiles = Sets.newHashSet(AppJar, AppConf, versionFile, standaloneXml,
                java7Marker, prebuildHook, tokenFile, prestartHook, prerestartHook, crontab);
        if (!excludeLogback) {
            expectedFiles.add(logbackXml);
        }

        FileCollector col = new FileCollector(testDir, gitDbDir, expectedFiles);


        assertTrue("Unexpected files remains in gitRepo: " + Joiner.on(",").join(col.foundUnexpectedFiles),
                col.foundUnexpectedFiles.isEmpty());
        log.info("Found these files: " + col.foundExpectedFiles);
        assertEquals("This is not really a jar for the sake of easiness of test",
                FileUtils.readFileToString(Files.resolve(testDir, AppJar)));
        assertEquals("sample.key=sample value\nsample.pass=blah blah",
                FileUtils.readFileToString(Files.resolve(testDir, AppConf)));
        assertEquals(Long.toString(TestUtils.sampleVersion), FileUtils.readFileToString(Files.resolve(testDir, versionFile)));
        assertTrue("Pre Build script should be executable", Files.isExecutable(Files.resolve(testDir, prebuildHook)));
        assertTrue("Pre Start script should be executable", Files.isExecutable(Files.resolve(testDir, prestartHook)));
        assertTrue("Pre Restart script should be executable", Files.isExecutable(Files.resolve(testDir, prerestartHook)));
        assertFalse("standalone script should not be executable", Files.isExecutable(Files.resolve(testDir, standaloneXml)));
    }

    private static UpstreamServerRegistration eqRegistration(final String userFacingUrlPrefix,
                                                             final String appNamePrefix,
                                                             final String upstreamHostAndPort,
                                                             final String upstreamPath) {
        EasyMock.reportMatcher(new IArgumentMatcher() {

            @Override
            public boolean matches(Object actual) {
                if (!(actual instanceof UpstreamServerRegistration)) {
                    return false;
                }
                UpstreamServerRegistration registration = (UpstreamServerRegistration) actual;
                return StringUtils.startsWith(registration.getAppName(), appNamePrefix) &&
                        StringUtils.equals(registration.getUpstreamHostAndPort(), upstreamHostAndPort) &&
                        StringUtils.equals(registration.getUpstreamPath(), upstreamPath) &&
                        StringUtils.equals(registration.getUserFacingUrlPrefix(), userFacingUrlPrefix);
            }

            @Override
            public void appendTo(StringBuffer buffer) {
                UpstreamServerRegistration expected = new UpstreamServerRegistration();
                expected.setUserFacingUrlPrefix(userFacingUrlPrefix);
                expected.setAppName(appNamePrefix);
                expected.setUpstreamHostAndPort(upstreamHostAndPort);
                expected.setUpstreamPath(upstreamPath);
                buffer.append(expected);
            }
        });
        return null;
    }
}
