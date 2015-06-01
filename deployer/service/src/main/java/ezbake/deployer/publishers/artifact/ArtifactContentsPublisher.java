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

package ezbake.deployer.publishers.artifact;

import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static ezbake.deployer.utilities.Utilities.getResourcesFromClassPath;


/**
 * This interface will serve as a way to inject files into an artifact
 */
public abstract class ArtifactContentsPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ArtifactContentsPublisher.class);

    /**
     * Process generated Archive entries for the deployment artifact
     *
     * @param artifact artifact to be processed
     * @return a list of tar archive entries for the artifact
     */
    public abstract Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException;

    protected Collection<ArtifactDataEntry> collectConfigurationResources(
            DeploymentArtifact artifact, String resourceClassPath, java.nio.file.Path artifactBasePath,
            ArtifactDataEntryResourceCreator creator) throws DeploymentException {
        return collectConfigurationResources(artifact, resourceClassPath, artifactBasePath, creator, false);
    }

    protected Collection<ArtifactDataEntry> collectConfigurationResources(
            DeploymentArtifact artifact, String resourceClassPath, java.nio.file.Path artifactBasePath,
            ArtifactDataEntryResourceCreator creator, boolean forceOverwrite) throws DeploymentException {
        java.util.List<ezbake.deployer.utilities.ArtifactDataEntry> artifactCollection = com.google.common.collect.Lists.newArrayList();
        final java.util.List<String> resources = getResourcesFromClassPath(this.getClass(), resourceClassPath);
        final java.nio.file.Path resourceDir = java.nio.file.Paths.get(resourceClassPath.replaceAll("\\.", "/"));

        for (String resource : resources) {
            try {
                java.nio.file.Path artifactPath = java.nio.file.Paths.get(artifactBasePath.toString(),
                        resourceDir.relativize(java.nio.file.Paths.get(resource)).toString());

                if (forceOverwrite ||
                        !doesResourceAlreadyExistInArtifact(artifactPath, artifactBasePath, artifact.getArtifact())) {
                    logger.info("Adding " + artifactPath.toString() + " because it doesn't exist.");
                    artifactCollection.add(creator.createFromClassPathResource(artifactPath.toString(), resource));
                }
            } catch (java.io.IOException e) {
                throw new DeploymentException(e.getMessage());
            }
        }
        return artifactCollection;
    }

    protected boolean doesResourceAlreadyExistInArtifact(java.nio.file.Path artifactPath, java.nio.file.Path artifactBasePath,
                                                       byte[] artifact) throws  DeploymentException {

        try (org.apache.commons.compress.archivers.tar.TarArchiveInputStream tais = new org.apache.commons.compress.archivers.tar.TarArchiveInputStream(
                new org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream(new java.io.ByteArrayInputStream(artifact)))) {

            org.apache.commons.compress.archivers.ArchiveEntry nextEntry;
            while ((nextEntry = tais.getNextEntry()) != null) {

                if (nextEntry.isDirectory()) continue;

                java.nio.file.Path entryPath = java.nio.file.Paths.get(nextEntry.getName());
                if (!entryPath.startsWith(artifactBasePath)) continue;


                if (entryPath.compareTo(artifactPath) == 0) {
                    return true;
                }
            }
        } catch (java.io.IOException e) {
            throw new DeploymentException(e.getMessage());
        }
        return false;
    }
}
