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

package ezbake.deployer.publishers.openShift;

import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import ezbake.deployer.impl.BasicFileAttributes;
import ezbake.deployer.impl.FileVisitResult;
import ezbake.deployer.impl.Files;
import ezbake.deployer.impl.PosixFilePermission;
import ezbake.deployer.impl.SimpleFileVisitor;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import static org.apache.commons.io.FileUtils.openOutputStream;

/**
 * The Rhc(Red Hat Cloud, e.g. OpenShift) application context.  This is the information related to the artifact for
 * deployment to OpenShift.  It has the Git Repository pointer, the Application pointer to within OpenShift and the
 * directory to where the application is stored on local disk (for modification).
 */
public class RhcApplication {
    private static final Logger log = LoggerFactory.getLogger(RhcApplication.class);

    private Git gitRepo;
    private IApplication applicationInstance;
    private IDomain applicationDomain;
    private File appTmpDir;
    private CredentialsProvider gitCredentialsProvider;

    public RhcApplication(Git gitRepo, IApplication applicationInstance, IDomain applicationDomain, File appTmpDir, CredentialsProvider gitCredentialsProvider) throws DeploymentException {
        setGitRepo(gitRepo);
        this.applicationInstance = applicationInstance;
        this.applicationDomain = applicationDomain;
        this.appTmpDir = appTmpDir;
        this.gitCredentialsProvider = gitCredentialsProvider;
    }

    public RhcApplication(IApplication applicationInstance, IDomain applicationDomain, File appTmpDir, CredentialsProvider gitCredentialsProvider) {
        this.applicationInstance = applicationInstance;
        this.applicationDomain = applicationDomain;
        this.appTmpDir = appTmpDir;
        this.gitCredentialsProvider = gitCredentialsProvider;
    }

    /**
     * Get the Git Repository object for this RhcApplication artifact
     * If the gitRepository hasn't been initialized yet, it will clone the git repository via {@link #getOrCreateGitRepo()}
     *
     * @return Git Repository object for this RhcApplication artifact
     */
    public Git getGitRepo() throws DeploymentException {
        if (gitRepo == null)
            return getOrCreateGitRepo();
        else
            return gitRepo;
    }

    /**
     * Sets the git repository object.  Probably not useful but here for Java Bean completeness
     *
     * @param gitRepo - git repository to set it to
     */
    public void setGitRepo(Git gitRepo) throws DeploymentException {
        this.gitRepo = gitRepo;

        try {
            StoredConfig config = gitRepo.getRepository().getConfig();
            config.setBoolean(ConfigConstants.CONFIG_CORE_SECTION, null,
                    ConfigConstants.CONFIG_KEY_FILEMODE, true);
            config.save();
        } catch (IOException e) {
            log.error("There was an error saving the  git configuration to disk", e);
            throw new DeploymentException("Could not save git configuration: " + e.getMessage());
        }
    }

    /**
     * Get the application instance of the OpenShift application for this artifact
     *
     * @return application instance of the OpenShift application for this artifact
     */
    public IApplication getApplicationInstance() {
        return applicationInstance;
    }

    /**
     * Sets the application instance for OpenShift for this artifact.  Probably not useful but here for Java Bean completeness
     *
     * @param applicationInstance - application instance to set it to
     */
    public void setApplicationInstance(IApplication applicationInstance) {
        this.applicationInstance = applicationInstance;
    }

    /**
     * get the OpenShift domain for this application.
     *
     * @return - The OpenShift domain this application is on.
     */
    public IDomain getApplicationDomain() {
        return applicationDomain;
    }

    /**
     * Set the OpenShift domain for this application. Probably not useful but here for Java Bean completeness
     *
     * @param applicationDomain - Domain to set it to
     */
    public void setApplicationDomain(IDomain applicationDomain) {
        this.applicationDomain = applicationDomain;
    }

    /**
     * Get the directory to which this OpenShift application is checked out to on local disk.
     * This directory isn't guaranteed to persist across different sessions or launches of the deployer.
     *
     * @return The application temp directory. (A git repository)
     */
    public File getAppTmpDir() {
        return appTmpDir;
    }

    /**
     * Sets the application temp directory.  This will not copy anything over to the new location.  This is probably
     * not useful but here for Java Bean completeness
     *
     * @param appTmpDir
     */
    public void setAppTmpDir(File appTmpDir) {
        this.appTmpDir = appTmpDir;
    }

    /**
     * Get the application name from OpenShift.  Should match the application name given to the artifact.
     *
     * @return The application name
     */
    public String getApplicationName() {
        return this.applicationInstance.getName();
    }

    /**
     * Tell this class to update the local copy of the artifact to REPLACE COMPLETELY with the artifact given by the
     * artifact tar.gz byte[] given.  This will not preserve any old files in the repository and will slam the entire
     * directory.  Readying the new artifact to be published to openshift via the {@link #publishChanges()} method.
     * <p/>
     * This method assumes anything that will belong into the repository is already in the tar file (no SSL certs are added)
     *
     * @param artifact - byte[] containing the tar.gz data for the artifact.
     * @param version  - Version of the application to stamp this binary with.  This way we can track the version easily.
     * @throws DeploymentException - On any errors updating the artifact. Including either git errors or OpenShift errors.
     */
    public void updateWithTarBall(byte[] artifact, String version) throws DeploymentException {
        clearProjectDirectory();
        extractTarGzFile(artifact);
        addVersionToWorkingTree(version);
        addFilesToGitWorkingTree();
    }


    public void addStreamAsFile(File p, InputStream ios) throws DeploymentException {
        addStreamAsFile(p, ios, null);
    }

    public void addStreamAsFile(File p, InputStream ios, Set<PosixFilePermission> filePermissions) throws DeploymentException {
        File resolvedPath = Files.resolve(getGitRepoPath(), p);
        if (Files.isDirectory(resolvedPath)) {
            throw new DeploymentException("Directory exist by the name of file wishing to write to: " + resolvedPath.toString());
        }

        try {
            Files.createDirectories(resolvedPath.getParent());
            OutputStream oos = new FileOutputStream(resolvedPath);
            boolean permissions = (filePermissions != null && !filePermissions.isEmpty());
            DirCache cache = null;
            try {
                IOUtils.copy(ios, oos);
                if (permissions)
                    Files.setPosixFilePermissions(resolvedPath, filePermissions);
                cache = gitRepo.add().addFilepattern(p.toString()).call();
                if (permissions) {
                    // Add executable permissions
                    cache.lock();
                    // Most of these mthods throw an IOException so we will catch that and unlock
                    DirCacheEntry entry = cache.getEntry(0);
                    log.debug("Setting executable permissions for: " + entry.getPathString());
                    entry.setFileMode(FileMode.EXECUTABLE_FILE);
                    cache.write();
                    cache.commit();
                }
            } catch (IOException e) {
                log.error("[" + getApplicationName() + "]" + "Error writing to file: " + resolvedPath.toString(), e);
                // Most of the DirCache entries should just thow IOExceptions
                if (cache != null) {
                    cache.unlock();
                }
                throw new DeploymentException("Error writing to file: " + resolvedPath.toString());
            } catch (GitAPIException e) {
                log.error("[" + getApplicationName() + "]" + "Error writing to file: " + resolvedPath.toString(), e);
                throw new DeploymentException("Error writing to file: " + resolvedPath.toString());
            } finally {
                IOUtils.closeQuietly(oos);
            }
        } catch (IOException e) {
            log.error("[" + getApplicationName() + "]" + "Error opening file for output: " + resolvedPath.toString(), e);
            throw new DeploymentException("Error opening file for output: " + resolvedPath.toString());
        }

    }


    /**
     * Updates the OpenShift application with the changes locally applied. (git commit, git Push)
     * This will cause the new app to be deployed to openshift.  After this the new version should be visible.
     *
     * @throws DeploymentException On any error deploying the application.
     */
    public void publishChanges() throws DeploymentException {

        try {
            RevCommit commit = gitRepo.commit().setMessage("RhcApplication: publishing newest version")
                    .setAuthor(applicationDomain.getUser().getRhlogin(), applicationDomain.getUser().getRhlogin())
                    .call();

            log.info("[" + getApplicationName() + "][GIT-COMMIT] committed changes to local git repo. " + commit.toString());
            PushCommand pushCommand = gitRepo.push();
            if (gitCredentialsProvider != null) pushCommand.setCredentialsProvider(gitCredentialsProvider);
            for (PushResult result : pushCommand.setForce(true).setTimeout(100000).call()) {
                if (!result.getMessages().isEmpty())
                    log.info("[" + getApplicationName() + "][GIT-PUSH] " + result.getMessages());
                log.info("[" + getApplicationName() + "][GIT-PUSH] Successfully pushed application.");
            }
        } catch (GitAPIException e) {
            log.error("[" + getApplicationName() + "] Error adding updated files to deployment git repo", e);
            throw new DeploymentException(e.getMessage());
        }
    }

    /**
     * Calls delete on every file in this application directory.  (basically git rm -r .)  This should allow for only
     * new files to be seen in the project directory.  This is automatically called by @{link #updateWithTarBall}, this
     * is here to allow a more flexible updating of the project if needed.
     *
     * @throws DeploymentException - on any git error while removing.
     */
    public void clearProjectDirectory() throws DeploymentException {

        final File gitDbPath = gitRepo.getRepository().getDirectory();
        final File gitRepoPath = gitDbPath.getParentFile();
        try {

            Files.walkFileTree(gitRepoPath, new SimpleFileVisitor<File>() {
                public FileVisitResult visitFile(File file, BasicFileAttributes attrs)
                        throws IOException {
                    File rel = Files.relativize(gitRepoPath, file);
                    try {
                        gitRepo.rm().addFilepattern(rel.toString()).call();
                    } catch (GitAPIException e) {
                        throw new IOException(e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult visitFileFailed(File file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult postVisitDirectory(File dir, IOException exc) throws IOException {
                    //println("postVisitDirectory(${dir})");
                    if (exc == null) {
                        if (dir.equals(gitDbPath))
                            return FileVisitResult.SKIP_SUBTREE;
                        return FileVisitResult.CONTINUE;
                    } else {
                        // directory iteration failed; propagate exception
                        throw exc;
                    }
                }

                public FileVisitResult preVisitDirectory(File dir, BasicFileAttributes attrs) throws IOException {
                    //println("preVisitDirectory(${dir})");
                    if (dir.equals(gitDbPath))
                        return FileVisitResult.SKIP_SUBTREE;
                    else
                        return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.error("[" + getApplicationName() + "]Error cleaning out directory: " + gitRepoPath.toString(), e);
            throw new DeploymentException(e.getMessage());
        }
    }

    private File getGitRepoPath() {
        return getGitRepoDir();
    }

    private File getGitRepoDir() {
        return gitRepo.getRepository().getDirectory().getParentFile();
    }

    private void addVersionToWorkingTree(String version) throws DeploymentException {

        try {
            FileUtils.writeStringToFile(new File(getGitRepoDir(), "deployed_version.txt"), version);
        } catch (IOException e) {
            log.error("[" + getApplicationName() + "] Error writing version file to deployment git repo", e);
            throw new DeploymentException(e.getMessage());
        }
    }

    private void extractTarGzFile(byte[] artifact) throws DeploymentException {
        writeTarFileToProjectDirectory(artifact);
    }


    private void addFilesToGitWorkingTree() throws DeploymentException {
        try {
            gitRepo.add().addFilepattern(".").call();
        } catch (GitAPIException e) {
            log.error("[" + getApplicationName() + "] Error adding updated files to deployment git repo", e);
            throw new DeploymentException(e.getMessage());
        }
    }

    private void writeTarFileToProjectDirectory(byte[] artifact) throws DeploymentException {
        final File gitDbPath = gitRepo.getRepository().getDirectory();
        final File projectDir = gitDbPath.getParentFile();
        try {
            CompressorInputStream uncompressedInput = new GzipCompressorInputStream(new ByteArrayInputStream(artifact));
            TarArchiveInputStream inputStream = new TarArchiveInputStream(uncompressedInput);

            // copy the existing entries
            TarArchiveEntry nextEntry;
            while ((nextEntry = (TarArchiveEntry) inputStream.getNextEntry()) != null) {
                File fileToWrite = new File(projectDir, nextEntry.getName());
                if (nextEntry.isDirectory()) {
                    fileToWrite.mkdirs();
                } else {
                    File house = fileToWrite.getParentFile();
                    if (!house.exists()) {
                        house.mkdirs();
                    }
                    copyInputStreamToFile(inputStream, fileToWrite);
                    Files.setPosixFilePermissions(fileToWrite, nextEntry.getMode());
                }
            }
        } catch (IOException e) {
            log.error("[" + getApplicationName() + "]" + e.getMessage(), e);
            throw new DeploymentException(e.getMessage());
        }
    }

    /**
     * From apache common's FileUtils.... However, they CLOSE the input stream even though its not documented as such!
     *
     * @param source      - the source stream, to which will not be closed
     * @param destination - destination file to write to
     * @throws IOException - on error writing to output stream
     */
    public static void copyInputStreamToFile(InputStream source, File destination) throws IOException {
        FileOutputStream output = openOutputStream(destination);
        try {
            IOUtils.copy(source, output);
            output.close(); // don't swallow close Exception if copy completes normally
        } finally {
            IOUtils.closeQuietly(output);
        }
    }

    /**
     * This will make sure that a directory is on disk for the git repository.  It will clone the git repo to the directory
     * if needed, or git pull the new contents if required.
     *
     * @return git repository created/retrieved from OpenShift with the remote pointing to OpenShift
     * @throws DeploymentException - on any exception getting it from git
     */
    public Git getOrCreateGitRepo() throws DeploymentException {
        String gitUrl = applicationInstance.getGitUrl();

        Files.createDirectories(appTmpDir);
        File gitDir = new File(appTmpDir, ".git");
        if (gitRepo != null || gitDir.exists() && gitDir.isDirectory()) {
            try {
                Git git = gitRepo != null ? gitRepo : Git.open(appTmpDir);
                // stash to get to a clean state of git dir so that we can pull without conflicts
                git.stashCreate();
                git.stashDrop();
                PullCommand pullCommand = git.pull();
                if (gitCredentialsProvider != null) pullCommand.setCredentialsProvider(gitCredentialsProvider);
                PullResult result = pullCommand.call();
                if (!result.isSuccessful())
                    throw new DeploymentException("Git pull was not successful: " + result.toString());
                setGitRepo(git);
                return git;
            } catch (IOException | GitAPIException e) {
                log.error("Error opening existing cached git repo", e);
                throw new DeploymentException("Error opening existing cached git repo: " + e.getMessage());
            }
        } else {
            try {
                log.info("Cloning to " + appTmpDir.toString());
                CloneCommand cloneCommand = Git.cloneRepository()
                        .setURI(gitUrl)
                        .setTimeout(10000)
                        .setDirectory(appTmpDir);
                if (gitCredentialsProvider != null) cloneCommand.setCredentialsProvider(gitCredentialsProvider);
                gitRepo = cloneCommand.call();
                log.info("Cloned to directory: " + gitRepo.getRepository().getDirectory().getParentFile().getAbsolutePath());
                return gitRepo;
            } catch (GitAPIException | JGitInternalException e) {
                log.error("Error cloning repository from OpenShift", e);
                throw new DeploymentException("Error cloning repository from OpenShift: " + e.getMessage());
            }
        }
    }

    public void delete() throws DeploymentException {
        // Call stop to run the stop script
        applicationInstance.stop();
        // Call destory to remove the app from OpenShift
        applicationInstance.destroy();
        gitRepo = null;
        if (Files.exists(appTmpDir)) {
            try {
                Files.deleteRecursively(appTmpDir);
            } catch (IOException e) {
                log.error("Unable to clean up directory: " + appTmpDir.getAbsolutePath(), e);
                throw new DeploymentException("Unable to clean up directory: " + appTmpDir.getAbsolutePath());
            }
        }
    }
}
