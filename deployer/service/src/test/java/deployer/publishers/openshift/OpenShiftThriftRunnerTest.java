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
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzReverseProxyRegister;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftTestUtils;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static deployer.TestUtils.AppDirs;
import static deployer.TestUtils.CONFIG_DIRECTORY;
import static deployer.TestUtils.GIT_TEST_DIR;
import static deployer.TestUtils.createSampleDeploymentArtifact;
import static deployer.TestUtils.getExistingInstances;
import static deployer.TestUtils.makeNumberGits;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OpenShiftThriftRunnerTest {
    private static final Logger log = LoggerFactory.getLogger(RhcApplicationGitRepoModificationsTest.class);


    @Test
    public void testWithExisting0() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        testSingleInstance(0);
    }

    @Test
    public void testWithExisting1() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        testSingleInstance(1);
    }

    @Test
    public void testWithExisting3() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        testSingleInstance(3);
    }

    private void testSingleInstance(int existing) throws DeploymentException, GitAPIException, IOException {
        String strExisting = Integer.toString(existing);
        File remoteTestDir = Files.resolve(GIT_TEST_DIR, strExisting, "thriftRunnerviaOpenshift.git");
        File testDir = Files.resolve(GIT_TEST_DIR, strExisting, "thriftRunnerviaOpenshift");
        System.out.println(testDir);
        Files.createDirectories(testDir);

        AppDirs[] theGits = makeNumberGits(existing, existing + "/thriftRunnerviaOpenshift");

        Git.init().setBare(true).setDirectory(remoteTestDir).call();

        Git git = Git.cloneRepository()
                .setURI(remoteTestDir.toURI().toString())
                .setDirectory(testDir).call();

        Rhc rhc = createMock(Rhc.class);

        IApplication instance = createMock(IApplication.class);
        IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);
        EzReverseProxyRegister ezReverseProxyRegister = createMock(EzReverseProxyRegister.class);


        expect(instance.getName()).andReturn("UnitTestApplication").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();
        if (existing == 0 || existing == 1) {
            expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(), TestUtils.getOpenShiftDomainName()))
                    .andReturn(Collections.EMPTY_LIST).once();
        } else {
            expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(), TestUtils.getOpenShiftDomainName()))
                    .andReturn(getExistingInstances(theGits, instance, domain)).once();
            instance.stop();
            expectLastCall().times(existing - 1);
            instance.destroy();
            expectLastCall().times(existing - 1);
        }

        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(0), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL)).andReturn(
                new RhcApplication(git, instance, domain, testDir, null)
        ).once();

        expect(instance.getEnvironmentVariables()).andReturn(new HashMap<String, IEnvironmentVariable>()).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME))
                .andReturn(envVariableValue("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME)).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME))
                .andReturn(envVariableValue("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME)).anyTimes();
        expect(instance.getEmbeddedCartridges()).andReturn(new ArrayList<IEmbeddedCartridge>()).anyTimes();

        IEmbeddedCartridge cart = createMock(IEmbeddedCartridge.class);
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("logstash"))).andReturn(cart).anyTimes();
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("cron"))).andReturn(cart).anyTimes();

        replay(instance, domain, mockUser, ezReverseProxyRegister, rhc);

        EzOpenShiftPublisher publisher = new EzOpenShiftPublisherMock(rhc, ezReverseProxyRegister);
        DeploymentArtifact deploymentArtifact = createSampleDeploymentArtifact(ArtifactType.Thrift);
        publisher.publish(deploymentArtifact, ThriftTestUtils.generateTestSecurityToken("U"));
        verify(instance, domain, mockUser, ezReverseProxyRegister, rhc);
        assertGitRepositoryFilesForUpdateGitRepoTest(testDir);
    }

    @Test
    public void testMultipleInstances() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        Rhc rhc = createMock(Rhc.class);

        AppDirs[] theGits = makeNumberGits(3, "openshiftMultipleInstances");

        final IApplication instance = createMock(IApplication.class);
        final IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);
        EzReverseProxyRegister ezReverseProxyRegister = createMock(EzReverseProxyRegister.class);

        expect(instance.getName()).andReturn("UnitTestApplication").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();


        expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(), TestUtils.getOpenShiftDomainName()))
                .andReturn(getExistingInstances(theGits, instance, domain)).once();

        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(0), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[0].gitRepo, instance, domain, theGits[0].testDir, null));
        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(1), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[1].gitRepo, instance, domain, theGits[1].testDir, null));
        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(2), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[2].gitRepo, instance, domain, theGits[2].testDir, null));

        expect(instance.getEnvironmentVariables()).andReturn(new HashMap<String, IEnvironmentVariable>()).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME))
                .andReturn(envVariableValue("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME)).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME))
                .andReturn(envVariableValue("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME)).anyTimes();
        expect(instance.getEmbeddedCartridges()).andReturn(new ArrayList<IEmbeddedCartridge>()).anyTimes();

        IEmbeddedCartridge cart = createMock(IEmbeddedCartridge.class);
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("logstash"))).andReturn(cart).anyTimes();
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("cron"))).andReturn(cart).anyTimes();

        replay(instance, domain, mockUser, ezReverseProxyRegister, rhc);
        EzOpenShiftPublisher publisher = new EzOpenShiftPublisherMock(rhc, ezReverseProxyRegister);
        DeploymentArtifact deploymentArtifact = createSampleDeploymentArtifact(ArtifactType.Thrift, (short) 3);
        publisher.publish(deploymentArtifact, ThriftTestUtils.generateTestSecurityToken("U"));
        for (AppDirs a : theGits) {
            assertGitRepositoryFilesForUpdateGitRepoTest(a.testDir);
        }
    }

    @Test
    public void testMultipleInstancesUnDeploy() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        Rhc rhc = createMock(Rhc.class);

        AppDirs[] theGits = makeNumberGits(3, "growup/openshiftMultipleInstances");

        final IApplication instance = createMock(IApplication.class);
        final IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);
        EzReverseProxyRegister ezReverseProxyRegister = createMock(EzReverseProxyRegister.class);

        expect(instance.getName()).andReturn("sampleappdshsampleservice").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();


        expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(), TestUtils.getOpenShiftDomainName()))
                .andReturn(getExistingInstances(theGits, instance, domain)).once();
        instance.stop();
        expectLastCall().times(3);
        instance.destroy();
        expectLastCall().times(3);

        replay(instance, domain, mockUser, ezReverseProxyRegister, rhc);
        EzOpenShiftPublisher publisher = new EzOpenShiftPublisherMock(rhc, ezReverseProxyRegister);
        DeploymentArtifact deploymentArtifact = createSampleDeploymentArtifact(ArtifactType.Thrift, (short) 3);
        publisher.unpublish(deploymentArtifact, ThriftTestUtils.generateTestSecurityToken("U"));
        for (AppDirs a : theGits) {
            assertFalse(a.testDir.toString() + " still exist after being deleted", a.testDir.exists());
        }
    }

    @Test
    public void testMultipleInstancesGrowUp() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        Rhc rhc = createMock(Rhc.class);

        AppDirs[] theGits = makeNumberGits(3, "growup/openshiftMultipleInstances");
        AppDirs[] initialGits = Arrays.copyOf(theGits, 2);

        final IApplication instance = createMock(IApplication.class);
        final IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);
        EzReverseProxyRegister ezReverseProxyRegister = createMock(EzReverseProxyRegister.class);

        expect(instance.getName()).andReturn("UnitTestApplication").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();


        expect(rhc.listApplicationInstances(TestUtils.getOpenShiftAppName(), TestUtils.getOpenShiftDomainName()))
                .andReturn(getExistingInstances(initialGits, instance, domain)).once();

        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(0), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[0].gitRepo, instance, domain, theGits[0].testDir, null));
        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(1), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[1].gitRepo, instance, domain, theGits[1].testDir, null));
        expect(rhc.getOrCreateApplication(TestUtils.buildOpenShiftAppName(2), TestUtils.getOpenShiftDomainName(),
                new StandaloneCartridge("java-thriftrunner"), ApplicationScale.NO_SCALE, GearProfile.SMALL))
                .andReturn(new RhcApplication(theGits[2].gitRepo, instance, domain, theGits[2].testDir, null));

        expect(instance.getEnvironmentVariables()).andReturn(new HashMap<String, IEnvironmentVariable>()).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME))
                .andReturn(envVariableValue("EZBAKE_APPLICATION_NAME", TestUtils.APP_NAME)).anyTimes();
        expect(instance.addEnvironmentVariable("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME))
                .andReturn(envVariableValue("EZBAKE_SERVICE_NAME", TestUtils.SERVICE_NAME)).anyTimes();
        expect(instance.getEmbeddedCartridges()).andReturn(new ArrayList<IEmbeddedCartridge>()).anyTimes();

        IEmbeddedCartridge cart = createMock(IEmbeddedCartridge.class);
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("logstash"))).andReturn(cart).anyTimes();
        expect(instance.addEmbeddableCartridge(new EmbeddableCartridge("cron"))).andReturn(cart).anyTimes();

        replay(instance, domain, mockUser, ezReverseProxyRegister, rhc);
        EzOpenShiftPublisher publisher = new EzOpenShiftPublisherMock(rhc, ezReverseProxyRegister);
        DeploymentArtifact deploymentArtifact = createSampleDeploymentArtifact(ArtifactType.Thrift, (short) 3);
        publisher.publish(deploymentArtifact, ThriftTestUtils.generateTestSecurityToken("U"));
        for (AppDirs a : theGits) {
            assertGitRepositoryFilesForUpdateGitRepoTest(a.testDir);
        }
    }


    private void assertGitRepositoryFilesForUpdateGitRepoTest(final File testDir) throws IOException {
        final File gitDbDir = Files.resolve(testDir, ".git");
        final File AppJar = Files.get("bin", "myApplication.jar");
        final File thriftRunnerJar = Files.get("bin", "thriftrunner.jar");
        final File startScript = Files.get(".openshift", "action_hooks", "start");
        final File stopScript = Files.get(".openshift", "action_hooks", "stop");
        final File prebuildHook = Files.get(".openshift", "action_hooks", "pre_build");
        final File AppConf = Files.get(CONFIG_DIRECTORY, "app.conf");
        final File versionFile = Files.get("deployed_version.txt");
        final File webFile = Files.get("www", "index.html");
        final File tokenFile = Files.get("config", "openshift.properties");
        final File crontab = Files.get(".openshift", "cron", "daily", "cron");

        FileCollector col = new FileCollector(testDir, gitDbDir, AppJar, AppConf, versionFile, thriftRunnerJar,
                startScript, stopScript, prebuildHook, webFile, tokenFile, crontab);


        assertTrue("Unexpected files remains in gitRepo: " + Joiner.on(",").join(col.foundUnexpectedFiles),
                col.foundUnexpectedFiles.isEmpty());
        log.info("Found these files: " + col.foundExpectedFiles);
        assertEquals("This is not really a jar for the sake of easiness of test",
                FileUtils.readFileToString(Files.resolve(testDir, AppJar)));
        assertEquals("I'm a binary",
                FileUtils.readFileToString(Files.resolve(testDir, thriftRunnerJar)));
        assertEquals("sample.key=sample value\nsample.pass=blah blah",
                FileUtils.readFileToString(Files.resolve(testDir, AppConf)));
        assertEquals(Long.toString(TestUtils.sampleVersion), FileUtils.readFileToString(Files.resolve(testDir, versionFile)));
        assertTrue("Start script should be executable", Files.isExecutable(Files.resolve(testDir, startScript)));
        assertTrue("Stop script should be executable", Files.isExecutable(Files.resolve(testDir, stopScript)));
        assertTrue("Pre Build script should be executable", Files.isExecutable(Files.resolve(testDir, prebuildHook)));
        assertThat("File should contains a start script", FileUtils.sizeOf(Files.resolve(testDir, startScript)),
                Matchers.greaterThan(1900L));
        assertThat("File should contains a www index.html", FileUtils.sizeOf(Files.resolve(testDir, webFile)),
                Matchers.greaterThan(50L));
        assertThat("File should contains a stop script", FileUtils.sizeOf(Files.resolve(testDir, stopScript)),
                Matchers.greaterThan(100L));
    }

    private IEnvironmentVariable envVariableValue(String name, String value) {
        IEnvironmentVariable environmentVariable = createMock(IEnvironmentVariable.class);
        expect(environmentVariable.getName()).andReturn(name).anyTimes();
        expect(environmentVariable.getValue()).andReturn(value).anyTimes();
        replay(environmentVariable);
        return environmentVariable;
    }

}
