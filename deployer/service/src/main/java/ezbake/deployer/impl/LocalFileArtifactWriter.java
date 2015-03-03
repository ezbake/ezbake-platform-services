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

package ezbake.deployer.impl;

import ezbake.deployer.ArtifactWriter;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFileArtifactWriter implements ArtifactWriter {
    private static final Logger log = LoggerFactory.getLogger(LocalFileArtifactWriter.class);
    private final String baseDirectory;
    private final TSerializer serializer = new TSerializer();

    public LocalFileArtifactWriter() {
        baseDirectory = System.getProperty("java.io.tmpdir") + File.separator;
    }

    @Override
    public void writeArtifact(DeploymentMetadata metadata, DeploymentArtifact artifact) throws DeploymentException {
        File directory = new File(buildDirectoryPath(metadata));
        directory.mkdirs();
        File artifactBinary = new File(buildFilePath(metadata));
        log.info("Writing artifact to {}", artifactBinary.getAbsolutePath());
        try {
            byte[] bytes = serializer.serialize(artifact);
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(artifactBinary, false));
            bos.write(bytes);
            bos.close();
        } catch (TException ex) {
            log.error("Failed serialization", ex);
            throw new DeploymentException("Failed to serialize the artifact before writing. " + ex.getMessage());
        } catch (IOException ex) {
            log.error("IO Failure", ex);
            throw new DeploymentException("IO Failure writing artifact. " + ex.getMessage());
        }
    }

    @Override
    public DeploymentArtifact readArtifact(DeploymentMetadata metadata) throws DeploymentException {
        File artifactFile = new File(buildFilePath(metadata));
        DeploymentArtifact artifact = new DeploymentArtifact();
        if (artifactFile.exists()) {
            TDeserializer deserializer = new TDeserializer();
            try {
                byte[] fileBytes = FileUtils.readFileToByteArray(artifactFile);
                deserializer.deserialize(artifact, fileBytes);
            } catch (Exception ex) {
                log.error("Failed reading artifact", ex);
                throw new DeploymentException("Failed to read artifact file from disk." + ex.getMessage());
            }
        } else {
            log.warn("The artifact {} {} could not be loaded from disk.  Only metadata is available",
                    ArtifactHelpers.getAppId(metadata), ArtifactHelpers.getServiceId(metadata));
            artifact.setMetadata(metadata);
        }
        return artifact;
    }

    @Override
    public void deleteArtifact(DeploymentMetadata metadata) throws DeploymentException {
        File artifact = new File(buildFilePath(metadata));
        if (artifact.exists()) {
            artifact.delete();
        } else {
            log.warn("File {} didn't exist to delete", artifact.getAbsoluteFile());
        }
    }

    private String buildDirectoryPath(DeploymentMetadata metadata) {
        String appName = ArtifactHelpers.getAppId(metadata);
        String serviceName = ArtifactHelpers.getServiceId(metadata);
        String filePath = baseDirectory + File.separator + appName + File.separator + serviceName;
        return filePath;
    }

    private String buildFilePath(DeploymentMetadata metadata) {
        String directory = buildDirectoryPath(metadata);
        String filePath = directory + File.separator + metadata.getVersion() + ".artifact";
        return filePath;
    }
}
