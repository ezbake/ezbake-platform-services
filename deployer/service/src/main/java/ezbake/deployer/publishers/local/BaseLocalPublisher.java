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

package ezbake.deployer.publishers.local;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public abstract class BaseLocalPublisher implements EzPublisher {
    protected final EzDeployerConfiguration configuration;
    protected final HashMap<String, LocalDeployment> deployments = Maps.newHashMap();
    final String ezDeployBinDirectory;

    public BaseLocalPublisher(EzDeployerConfiguration configuration) {
        this.configuration = configuration;
        ezDeployBinDirectory = this.configuration.getEzConfiguration().getProperty("bin.directory");
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        LocalDeployment deployment = deployments.get(artifact.getMetadata().getManifest().getApplicationInfo().getApplicationId() +
                "_" + artifact.getMetadata().getManifest().getApplicationInfo().getServiceId());
        if (deployment != null) {
            stopProcess(artifact, deployment);
            File jar = new File(deployment.artifactFile);
            jar.delete();
            if (deployment.configPath != null) {
                File config = new File(deployment.configPath);
                if (config.exists()) {
                    try {
                        FileUtils.deleteDirectory(config);
                    } catch (IOException ex) {
                        //don't care
                    }
                }
            }
        }
    }

    protected void stopProcess(DeploymentArtifact artifact, LocalDeployment deployment) throws DeploymentException {
        deployment.process.destroy();
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        //no validation needed
    }

    protected String getArtifactPath(DeploymentArtifact artifact) throws Exception {
        TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(artifact.getArtifact())));
        String jarPath = null;
        try {
            TarArchiveEntry entry = tarIn.getNextTarEntry();

            while (entry != null) {
                if (entry.getName().startsWith("bin/") || entry.getName().endsWith(".war")) {
                    boolean isWar = entry.getName().endsWith(".war");
                    File tmpFile = File.createTempFile(Files.getNameWithoutExtension(entry.getName()),
                            isWar ? ".war" : ".jar");
                    FileOutputStream fos = new FileOutputStream(tmpFile);
                    try {
                        IOUtils.copy(tarIn, fos);
                    } finally {
                        IOUtils.closeQuietly(fos);
                    }
                    jarPath = tmpFile.getAbsolutePath();
                    break;
                }
                entry = tarIn.getNextTarEntry();
            }
        } finally {
            IOUtils.closeQuietly(tarIn);
        }
        return jarPath;
    }

    protected String getConfigPath(DeploymentArtifact artifact) throws Exception {
        TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(artifact.getArtifact())));
        File tmpDir = new File("user-conf", ArtifactHelpers.getServiceId(artifact));
        tmpDir.mkdirs();
        String configPath = tmpDir.getAbsolutePath();

        try {
            TarArchiveEntry entry = tarIn.getNextTarEntry();

            while (entry != null) {
                if (entry.getName().startsWith("config/")) {
                    File tmpFile = new File(configPath, Files.getNameWithoutExtension(entry.getName()) + ".properties");
                    FileOutputStream fos = new FileOutputStream(tmpFile);
                    try {
                        IOUtils.copy(tarIn, fos);
                    } finally {
                        IOUtils.closeQuietly(fos);
                    }
                }
                entry = tarIn.getNextTarEntry();
            }
        } finally {
            IOUtils.closeQuietly(tarIn);
        }
        return configPath;
    }

    protected void dumpIO(Process process) {
        inheritIO(process.getInputStream(), System.out);
        inheritIO(process.getErrorStream(), System.err);
    }

    private void inheritIO(final InputStream src, final PrintStream dest) {
        new Thread(new Runnable() {
            public void run() {
                Scanner sc = new Scanner(src);
                while (sc.hasNextLine()) {
                    dest.println(sc.nextLine());
                }
            }
        }).start();
    }

}
