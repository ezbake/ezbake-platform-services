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

 package ezbake.deployer.publishers;

import com.google.inject.Inject;
import ezbake.azkaban.manager.AuthenticationManager;
import ezbake.azkaban.manager.ProjectManager;
import ezbake.azkaban.manager.ScheduleManager;
import ezbake.azkaban.manager.UploadManager;
import ezbake.azkaban.manager.result.AuthenticationResult;
import ezbake.azkaban.manager.result.ManagerResult;
import ezbake.azkaban.manager.result.SchedulerResult;
import ezbake.azkaban.manager.result.UploaderResult;
import ezbake.azkaban.submitter.util.UnzipUtil;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.BatchJobInfo;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.azkaban.AzkabanConfigurationHelper;
import ezbakehelpers.hdfs.HDFSHelper;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Publisher that will publish jobs to Azkaban to be scheduled to run on HDFS
 */
public class EzAzkabanPublisher implements EzPublisher {

    private static final Logger log = LoggerFactory.getLogger(EzAzkabanPublisher.class);
    private static final String unzipDir = "/tmp/";
    final AzkabanConfigurationHelper azConf;
    private Properties props;

    @Inject
    public EzAzkabanPublisher(Properties configuration) {
        props = configuration;
        azConf = new AzkabanConfigurationHelper(configuration);
    }

    /**
     * This will publish the artifact to Azkaban for scheduled running.  The artifact should be of the format
     * <p/>
     * <p/>
     * The artifact at this point in time will already have included the SSL certs.
     * <p/>
     * Its up to the publisher to reorganize the tar file if needed for its PaaS
     *
     * @param artifact    The artifact to deploy
     * @param callerToken - The token of the user or application that initiated this call
     * @throws DeploymentException - On any exceptions
     */
    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        File unzippedPack = null;
        File azkabanZip = null;
        ZipOutputStream zipOutputStream = null;
        String flowName;
        final BatchJobInfo jobInfo = artifact.getMetadata().getManifest().getBatchJobInfo();

        // Get the Azkaban authentication token
        final AuthenticationResult authenticatorResult;
        try {
            authenticatorResult = new AuthenticationManager(new URI(azConf.getAzkabanUrl()),
                    azConf.getUsername(), azConf.getPassword()).login();
        } catch (URISyntaxException e) {
            throw new DeploymentException(e.getMessage());
        }

        if (authenticatorResult.hasError()) {
            log.error("Could not log into Azkaban: " + authenticatorResult.getError());
            throw new DeploymentException(authenticatorResult.getError());
        }

        log.info("Successfully logged into Azkaban. Now creating .zip to upload");

        try {
            // Unzip the artifact
            unzippedPack = UnzipUtil.unzip(new File(unzipDir), ByteBuffer.wrap(artifact.getArtifact()));
            log.info("Unzipped artifact to: " + unzippedPack.getAbsolutePath());

            // Create a .zip file to submit to Azkaban
            azkabanZip = File.createTempFile("ezbatch_", ".zip");
            log.info("Created temporary zip file: " + azkabanZip.getCanonicalPath());
            zipOutputStream = new ZipOutputStream(new FileOutputStream(azkabanZip));

            // Copy the configs from the artifact to the top level of the zip.  This should contain the Azkaban
            // .jobs and .properties
            final String configDir = UnzipUtil.getConfDirectory(unzippedPack).get();
            final File configDirFile = new File(configDir);
            for (File f : FileUtils.listFiles(configDirFile, TrueFileFilter.TRUE, TrueFileFilter.TRUE)) {
                zipOutputStream.putNextEntry(new ZipArchiveEntry(f.getCanonicalPath().replaceFirst(configDir, "")));
                IOUtils.copy(new FileInputStream(f), zipOutputStream);
                zipOutputStream.closeEntry();
            }
            log.info("Copied configs to the .zip");

            // Copy the jars from bin/ in the artifact to lib/ in the .zip file and other things to the jar as needed
            final String dirPrefix = unzippedPack.getAbsolutePath() + "/bin/";
            for (File f : FileUtils.listFiles(new File(dirPrefix), TrueFileFilter.TRUE, TrueFileFilter.TRUE)) {
                zipOutputStream.putNextEntry(new ZipArchiveEntry(f.getCanonicalPath().replaceFirst(dirPrefix, "lib/")));

                final JarInputStream jarInputStream = new JarInputStream(new FileInputStream(f));
                final JarOutputStream jarOutputStream = new JarOutputStream(zipOutputStream);

                JarEntry je;
                while ((je = jarInputStream.getNextJarEntry()) != null) {
                    jarOutputStream.putNextEntry(je);
                    IOUtils.copy(jarInputStream, jarOutputStream);
                    jarOutputStream.closeEntry();
                }
                log.info("Created Jar file");

                // Add the SSL certs to the jar
                final String sslPath = UnzipUtil.getSSLPath(configDirFile).get();
                for (File sslFile : FileUtils.listFiles(new File(sslPath), TrueFileFilter.TRUE, TrueFileFilter.TRUE)) {
                    if (sslFile.isFile()) {
                        jarOutputStream.putNextEntry(new JarArchiveEntry("ssl/" + sslFile.getName()));
                        IOUtils.copy(new FileInputStream(sslFile), jarOutputStream);
                        jarOutputStream.closeEntry();
                    }
                }
                log.info("Added SSL certs to jar");

                // Add the application.properties to the jar file so the jobs can read it
                final File appProps = new File(configDir, "application.properties");
                final Properties adjustedProperties = new Properties();
                adjustedProperties.load(new FileInputStream(appProps));
                adjustedProperties.setProperty("ezbake.security.ssl.dir", "/ssl/");
                jarOutputStream.putNextEntry(new JarArchiveEntry("application.properties"));
                adjustedProperties.store(jarOutputStream, null);
                jarOutputStream.closeEntry();

                jarOutputStream.finish();
                zipOutputStream.closeEntry();
            }

            // Check to see if there are any .job files.  If there aren't, this is an external job and we need to create
            // one for the .zip file
            final Collection<File> jobFiles = FileUtils.listFiles(configDirFile, new String[]{"job"}, false);
            if (jobFiles.isEmpty()) {
                // If there are no job files present then we need to create one for the user
                final StringBuilder sb = new StringBuilder(
                        "type=hadoopJava\n" +
                                "job.class=ezbatch.amino.api.EzFrameworkDriver\n" +
                                "classpath=./lib/*\n" +
                                "main.args=-d /ezbatch/amino/config");

                for (File xmlConfig : FileUtils.listFiles(configDirFile, new String[]{"xml"}, false)) {
                    sb.append(" -c ").append(xmlConfig.getName());
                }

                zipOutputStream.putNextEntry(new ZipEntry("Analytic.job"));
                IOUtils.copy(new StringReader(sb.toString()), zipOutputStream);
                zipOutputStream.closeEntry();
                log.info("There was no .job file so one was created for the .zip");
                flowName = "Analytic";
            } else {
                flowName = jobInfo.getFlowName();
                if (flowName == null) {
                    log.warn("Manifest did not contain flow_name. Guessing what it should be");
                    flowName = FilenameUtils.getBaseName(jobFiles.toArray(new File[jobFiles.size()])[0].getName());
                    log.info("Guessing the flow name should be:" + flowName);
                }
            }

            zipOutputStream.finish();
            log.info("Finished creating .zip");

            // Now that we've created the zip to upload, attempt to create a project for it to be uploaded to. Every .zip
            // file needs to be uploaded to a project, and the project may or may not already exist.
            final String projectName = ArtifactHelpers.getAppId(artifact) + "_" + ArtifactHelpers.getServiceId(artifact);
            final ProjectManager projectManager = new ProjectManager(authenticatorResult.getSessionId(), new URI(azConf.getAzkabanUrl()));
            final ManagerResult managerResult = projectManager.createProject(projectName, "EzBatch Deployed");

            // If the project already exists, it will return an error, but really it's not a problem
            if (managerResult.hasError()) {
                if (!managerResult.getMessage().contains("already exists")) {
                    log.error("Could not create project: " + managerResult.getMessage());
                    throw new DeploymentException(managerResult.getMessage());
                } else {
                    log.info("Reusing the existing project: " + projectName);
                }
            } else {
                log.info("Created new project: " + projectName);
                log.info("Path: " + managerResult.getPath());
            }

            // Upload the .zip file to the project
            final UploadManager uploader = new UploadManager(authenticatorResult.getSessionId(),
                    azConf.getAzkabanUrl(), projectName, azkabanZip);
            final UploaderResult uploaderResult = uploader.uploadZip();

            if (uploaderResult.hasError()) {
                log.error("Could not upload the zip file: " + uploaderResult.getError());
                throw new DeploymentException(uploaderResult.getError());
            }

            log.info("Successfully submitted zip file to Azkaban");

            // Schedule the jar to run.  If the start times aren't provided, it will run in 2 minutes

            final ScheduleManager scheduler = new ScheduleManager(authenticatorResult.getSessionId(),
                    new URI(azConf.getAzkabanUrl()));

            // Add the optional parameters if they are present
            if (jobInfo.isSetStartDate()) {
                scheduler.setScheduleDate(jobInfo.getStartDate());
            }
            if (jobInfo.isSetStartTime()) {
                scheduler.setScheduleTime(jobInfo.getStartTime());
            }
            if (jobInfo.isSetRepeat()) {
                scheduler.setPeriod(jobInfo.getRepeat());
            }

            final SchedulerResult schedulerResult = scheduler.scheduleFlow(projectName, flowName, uploaderResult.getProjectId());
            if (schedulerResult.hasError()) {
                log.error("Failure to schedule job: " + schedulerResult.getError());
                throw new DeploymentException(schedulerResult.getError());
            }

            log.info("Successfully scheduled flow: " + flowName);

        } catch (Exception ex) {
            log.error("No Nos!", ex);
            throw new DeploymentException(ex.getMessage());
        } finally {
            IOUtils.closeQuietly(zipOutputStream);
            FileUtils.deleteQuietly(azkabanZip);
            FileUtils.deleteQuietly(unzippedPack);
        }
    }

    /**
     * This will remove the artifact from the given implementation of PaaS
     * <p/>
     * The artifact at this point will be unavailable for use by consumers.
     *
     * @param artifact    - The artifact to remove from deployment
     * @param callerToken - The token of the user or application that initiated this call
     * @throws DeploymentException - On any exceptions
     */
    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        final String projectName = ArtifactHelpers.getAppId(artifact) + "_" + ArtifactHelpers.getServiceId(artifact);
        log.info("Unpublishing the project '{}'", projectName);

        // Get the Azkaban authentication token
        final AuthenticationResult authenticatorResult;
        final URI azkabanUri;
        try {
            azkabanUri = new URI(azConf.getAzkabanUrl());
            authenticatorResult = new AuthenticationManager(azkabanUri, azConf.getUsername(), azConf.getPassword()).login();
            if (authenticatorResult.hasError()) {
                log.error("Could not log into Azkaban: " + authenticatorResult.getError());
                throw new DeploymentException(authenticatorResult.getError());
            }

            final ProjectManager projectManager = new ProjectManager(authenticatorResult.getSessionId(), azkabanUri);

            // Attempt to remove the project from Azkaban
            final String projectId = projectManager.removeProject(projectName);
            log.info("Removed project '{}' with ID <{}> from Azkaban", projectName, projectId);

            // Figure out where the project actually resides
            // TODO - make this more robust
            final FileSystem fs = HDFSHelper.getFileSystemFromProperties(props);
            Path projectDir = new Path("/ezbatch/amino/analytics/" + projectId);
            log.info("Looking for old data in {}", projectDir);
            if (!fs.exists(projectDir)) {
                projectDir = new Path("/ezbatch/amino/bitmaps/" + projectId);
                log.info("Data not found there. Looking in {}", projectDir);
                if (!fs.exists(projectDir)) {
                    log.info("Could not find any directories on HDFS for the project");
                    projectDir = null;
                }
            }

            // Remove the directory
            if (projectDir != null) {
                // TODO - verify that the directory isn't currently in use. (it shouldn't be because of the removeProject())
                fs.delete(projectDir, true);
                log.info("Erased '{}' from HDFS", projectDir.toString());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new DeploymentException(e.getMessage());
        }
    }

    /**
     * Validate the publisher's environment including attempting to do a health check on the publisher's service if
     * available for this publisher.
     *
     * @throws DeploymentException   - On any health issues of the service
     * @throws IllegalStateException - On any illegal state for the current publisher
     */
    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        /*
        final AuthenticationManager authenticator;
        try {
            authenticator = new AuthenticationManager(new URI(azConf.getAzkabanUrl()),
                    azConf.getUsername(), azConf.getPassword());
            final AuthenticationResult result = authenticator.login();
            if (result.hasError()){
                throw new DeploymentException(result.getError());
            }
        } catch (URISyntaxException e) {
            log.error(e.getMessage());
            throw new DeploymentException(e.getMessage());
        }
        */
    }
}
