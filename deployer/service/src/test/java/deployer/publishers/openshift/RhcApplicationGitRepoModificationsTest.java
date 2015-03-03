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
import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import com.openshift.client.IUser;
import deployer.TestUtils;
import ezbake.deployer.impl.BasicFileAttributes;
import ezbake.deployer.impl.FileVisitResult;
import ezbake.deployer.impl.FileVisitor;
import ezbake.deployer.impl.Files;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RhcApplicationGitRepoModificationsTest {
    private static final Logger log = LoggerFactory.getLogger(RhcApplicationGitRepoModificationsTest.class);
    public static final File GIT_TEST_DIR = TestUtils.getATempDirectory();

    @Test
    public void shouldUpdateGitRepoWithModifications() throws IOException, GitAPIException, ArchiveException, DeploymentException {
        File remoteTestDir = Files.resolve(GIT_TEST_DIR, "shouldUpdateGitRepoWithModificationsOrigin.git");
        File testDir = Files.resolve(GIT_TEST_DIR, "shouldUpdateGitRepoWithModifications");
        Files.createDirectories(testDir);

        Git remoteGit = Git.init().setBare(true).setDirectory(remoteTestDir).call();

        Git git = Git.cloneRepository()
                .setURI(remoteTestDir.toString())
                .setDirectory(testDir).call();
        populateWithInitialFiles(git, testDir);

        IApplication instance = createMock(IApplication.class);
        IDomain domain = createMock(IDomain.class);
        IUser mockUser = createMock(IUser.class);

        expect(instance.getName()).andReturn("UnitTestApplication").anyTimes();
        expect(domain.getUser()).andReturn(mockUser).anyTimes();
        expect(mockUser.getRhlogin()).andReturn("UnitTestuser").anyTimes();

        replay(instance, domain, mockUser);

        RhcApplication app = new RhcApplication(git, instance, domain, testDir, null);
        ByteBuffer sampleTarBall = TestUtils.createSampleAppTarBall(ArtifactType.Thrift);
        app.updateWithTarBall(sampleTarBall.array(), "v1.0");
        app.addStreamAsFile(Files.get(TestUtils.CONFIG_DIRECTORY, "extraInfo.conf"), new ByteArrayInputStream("Extra information".getBytes()));
        app.publishChanges();
        verify(instance, domain, mockUser);
        assertGitRepositoryFilesForUpdateGitRepoTest(testDir);
        File ensuredDir = Files.resolve(GIT_TEST_DIR, "shouldUpdateGitRepoWithModificationsEnsured");
        Git.cloneRepository()
                .setURI(remoteTestDir.toURI().toString())
                .setDirectory(ensuredDir).call();
        assertGitRepositoryFilesForUpdateGitRepoTest(ensuredDir);
    }

    private void assertGitRepositoryFilesForUpdateGitRepoTest(final File testDir) throws IOException {
        final File gitDbDir = Files.resolve(testDir, ".git");
        final File AppJar = Files.get("bin", "myApplication.jar");
        final File AppConf = Files.get(TestUtils.CONFIG_DIRECTORY, "app.conf");
        final File extraInfo = Files.get(TestUtils.CONFIG_DIRECTORY, "extraInfo.conf");
        final File versionFile = Files.get("deployed_version.txt");

        final Set<File> expectedFiles = Sets.newHashSet(AppJar, AppConf, extraInfo, versionFile);
        final Set<File> foundUnexpectedFiles = Sets.newHashSet();
        Files.walkFileTree(testDir, new FileVisitor<File>() {
            @Override
            public FileVisitResult preVisitDirectory(File dir, BasicFileAttributes attrs) throws IOException {
                if (dir.equals(gitDbDir))
                    return FileVisitResult.SKIP_SUBTREE;
                else
                    return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(File file, BasicFileAttributes attrs) throws IOException {
                File relFile = Files.relativize(testDir, file);
                if (!expectedFiles.contains(relFile)) {
                    foundUnexpectedFiles.add(relFile);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(File file, IOException exc) throws IOException {
                System.out.println("visitFileFailed(" + file + ")");
                return FileVisitResult.CONTINUE;

            }

            @Override
            public FileVisitResult postVisitDirectory(File dir, IOException exc) throws IOException {
                if (dir.equals(gitDbDir))
                    return FileVisitResult.SKIP_SUBTREE;
                else
                    return FileVisitResult.CONTINUE;
            }
        });
        assertTrue("Unexpected files remains in gitRepo: " + Joiner.on(",").join(foundUnexpectedFiles),
                foundUnexpectedFiles.isEmpty());

        assertEquals("This is not really a jar for the sake of easiness of test",
                FileUtils.readFileToString(Files.resolve(testDir, AppJar)));
        assertEquals("sample.key=sample value\nsample.pass=blah blah",
                FileUtils.readFileToString(Files.resolve(testDir, AppConf)));
        assertEquals("Extra information",
                FileUtils.readFileToString(Files.resolve(testDir, extraInfo)));
        assertEquals("v1.0", FileUtils.readFileToString(Files.resolve(testDir, versionFile)));
    }

    private void populateWithInitialFiles(Git git, File testDir) throws IOException, GitAPIException {
        log.debug("Git Repo Dir: " + git.getRepository().getDirectory().toString());
        log.debug("Test dir: " + testDir.toString());
        FileUtils.writeStringToFile(Files.resolve(testDir, "README"), "This is from the RhcApplication unit test.");
        File aDir = Files.resolve(testDir, "a-dir");
        Files.createDirectories(aDir);
        FileUtils.writeStringToFile(Files.resolve(aDir, "source.json"), "{'hello': 'world'}");
        FileUtils.writeStringToFile(Files.resolve(aDir, "AnotherFile.txt"), "A file about nothing");
        FileUtils.writeStringToFile(Files.resolve(testDir, "deployed_version.txt"), "v0.33333333333333333");
        log.info(git.add().addFilepattern(".").call().toString());
        log.info(git.commit()
                .setAuthor("Unit Test", "unit.test@email.com")
                .setMessage("Initial files")
                .setAll(true)
                .call().toString());
    }


}
