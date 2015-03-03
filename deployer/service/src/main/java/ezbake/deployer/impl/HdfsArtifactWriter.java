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

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import ezbake.deployer.ArtifactWriter;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HdfsArtifactWriter implements ArtifactWriter {

    private static final Logger logger = LoggerFactory.getLogger(HdfsArtifactWriter.class);
    private final String EZDEPLOYER_ARTIFACT_BASE_PATH = "/ezdeployer/artifacts/";

    FileSystem fs;

    @Inject
    public HdfsArtifactWriter(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public void writeArtifact(DeploymentMetadata metadata, DeploymentArtifact artifact) throws DeploymentException {
        String fqn = ArtifactHelpers.getFqAppId(metadata);
        String version = metadata.getVersion();
        try {
            FSDataOutputStream output = fs.create(createPath(fqn, version), true);
            TSerializer serializer = new TSerializer();
            byte[] artifactBytes = serializer.serialize(artifact);
            output.write(artifactBytes);
            output.sync();
            output.close();
        } catch (IOException e) {
            logger.error("Unable to write to " + fqn + " on HDFS", e);
            throw new DeploymentException("Unable to write to " + fqn + " on HDFS!" + e.getMessage());
        } catch (TException e) {
            logger.error("Unable to serialize object!", e);
            throw new DeploymentException("Unable able to serialize object!" + e.getMessage());
        }
    }

    @Override
    public DeploymentArtifact readArtifact(DeploymentMetadata metadata) throws DeploymentException {
        Path artifactPath = createPath(ArtifactHelpers.getFqAppId(metadata), metadata.getVersion());
        DeploymentArtifact artifact = new DeploymentArtifact();
        try {
            if (!fs.exists(artifactPath)) {
                throw new DeploymentException("Could not find artifact at " + artifactPath.toString());
            }
            FSDataInputStream input = fs.open(artifactPath);
            byte[] artifactBytes = ByteStreams.toByteArray(input);
            input.close();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(artifact, artifactBytes);
        } catch (IOException e) {
            logger.error("Could not read data from : " + artifactPath.toString(), e);
            throw new DeploymentException(e.getMessage());
        } catch (TException e) {
            logger.error("Could not deserialize artifact!", e);
            throw new DeploymentException(e.getMessage());
        }

        return artifact;
    }

    @Override
    public void deleteArtifact(DeploymentMetadata metadata) throws DeploymentException {
        Path artifactPath = createPath(ArtifactHelpers.getFqAppId(metadata), metadata.getVersion());
        try {
            fs.delete(artifactPath, false);
        } catch (IOException e) {
            logger.error("Unable to delete " + artifactPath.toString() + " from HDFS!", e);
            throw new DeploymentException(e.getMessage());
        }
    }

    private Path createPath(String fqn, String version) {
        return new Path(EZDEPLOYER_ARTIFACT_BASE_PATH + fqn + "/" + version + "/artifact");
    }
}
