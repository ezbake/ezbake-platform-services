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

package ezbake.deployer.publishers.ezcentos;

import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.local.BaseLocalPublisher;
import ezbake.deployer.publishers.local.LocalDeployment;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.ApplicationInfo;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class EzCentosThriftRunnerPublisher extends BaseLocalPublisher {
    private static final Logger logger = LoggerFactory.getLogger(EzCentosThriftRunnerPublisher.class);
    private final String ezcentosBuildPacksDir = "/vagrant/buildpacks/via_ezdeployer/";

    @Inject
    public EzCentosThriftRunnerPublisher(EzDeployerConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {

        final String appName = ArtifactHelpers.getAppId(artifact);
        final String serviceName = ArtifactHelpers.getServiceId(artifact);

        try {
            final String jarFile = getArtifactPath(artifact);
            final String configPath = getConfigPath(artifact);
            final String command = getStartCommand(artifact, jarFile, appName, serviceName);
            logger.info("Executing Publish command {}", command);
            Process process = Runtime.getRuntime().exec(command);
            dumpIO(process);

            deployments.put(appName + "_" + serviceName, new LocalDeployment(process, jarFile, configPath));

        } catch (Exception ex) {
            throw new DeploymentException("IOException " + ex.getMessage());
        }
    }

    private String getNewBuildPacksDirName(String appName, String serviceName) {
        return ezcentosBuildPacksDir + appName + "-" + serviceName;
    }

    private String getStartCommand(DeploymentArtifact artifact, String jarFile, String appName, String serviceName) throws IOException {
        final String startServiceAdvancedScript = "sudo /vagrant/scripts/startServiceAdvanced.sh";

        /* Create a directory for the app deployment in:
         * /vagrant/buildpacks/via_ezdeployer/
         * For example:
         * /vagrant/buildpacks/via_ezdeployer/myapp-ezmongo/
         * for the ezmongo deployment for myapp application.
         */
        final String newBuildPackDirName = getNewBuildPacksDirName(appName, serviceName);
        final File newBuildPackDir = new File(newBuildPackDirName);
        FileUtils.deleteDirectory(newBuildPackDir);
        newBuildPackDir.mkdir();

        // Copy the manifest file
        copyFiles(artifact, newBuildPackDir, ".yml");

        // Create the config dir in the new buildpack dir
        final String newBuildPackConfigDirName = newBuildPackDirName + "/config";
        final File newBuildPackConfigDir = new File(newBuildPackConfigDirName);
        newBuildPackConfigDir.mkdir();

        // Copy the ezconfig properties files
        // Notice we pass in newBuildPackDir and not newBuildPackConfigDir since
        // the "entry" in the .tar file already has "config/" in the file name.
        copyFiles(artifact, newBuildPackDir, ".properties");

        return String.format("%s %s %s %s %s %s", startServiceAdvancedScript,
                serviceName, appName, jarFile, "0", newBuildPackDirName);
    }

    /**
     * Copies files from the artifact .tar file
     * that end in the suffix to the specified destination.
     *
     * @param artifact
     * @param destination
     * @param suffix
     * @return Paths that the files were copied to
     * @throws IOException
     */
    private List<String> copyFiles(DeploymentArtifact artifact, File destination, String suffix) throws IOException {
        TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(artifact.getArtifact())));
        List<String> filePaths = new ArrayList<>();
        try {
            TarArchiveEntry entry = tarIn.getNextTarEntry();

            while (entry != null) {
                if (entry.getName().endsWith(suffix)) {
                    String newFilePath = destination + File.separator + entry.getName();
                    FileOutputStream fos = new FileOutputStream(newFilePath);
                    try {
                        IOUtils.copy(tarIn, fos);
                    } finally {
                        IOUtils.closeQuietly(fos);
                    }
                    filePaths.add(newFilePath);
                }
                entry = tarIn.getNextTarEntry();
            }
        } finally {
            IOUtils.closeQuietly(tarIn);
        }

        return filePaths;
    }

    /**
     * Calls the /vagrant/scripts/stopServiceAdvanced.sh script in EzCentos
     * to stop the running service.
     * Also deletes the /vagrant/buildpacks/via_ezdeployer/<service> directory
     * which is used by EzCentos' start/stopService scripts for the deployed service.
     *
     * @param artifact
     * @param deployment
     * @throws DeploymentException
     */
    @Override
    protected void stopProcess(DeploymentArtifact artifact, LocalDeployment deployment) throws DeploymentException {
        final ApplicationInfo applicationInfo = artifact.getMetadata().getManifest().getApplicationInfo();
        final String appName = applicationInfo.applicationId;
        final String serviceName = applicationInfo.serviceId;
        final String stopServiceAdvancedScript = "sudo /vagrant/scripts/stopServiceAdvanced.sh";
        final String command = String.format("%s %s %s", stopServiceAdvancedScript, serviceName, appName);
        logger.info("Executing Stop Process command {}", command);
        try {
            Process process = Runtime.getRuntime().exec(command);
            dumpIO(process);

            // Delete the /vagrant/buildpacks/via_ezdeployer/<service> directory
            final File newBuildPackDir = new File(getNewBuildPacksDirName(appName, serviceName));
            FileUtils.deleteDirectory(newBuildPackDir);
        } catch (IOException e) {
            throw new DeploymentException("IOException " + e.getMessage());
        }
    }
}
