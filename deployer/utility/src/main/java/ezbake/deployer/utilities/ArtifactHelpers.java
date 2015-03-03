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

package ezbake.deployer.utilities;

import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.Language;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class ArtifactHelpers {
    private static final Logger log = LoggerFactory.getLogger(ArtifactHelpers.class);

    private ArtifactHelpers() {
    }

    public static ArtifactType getType(ArtifactManifest manifest) {
        return manifest.getArtifactType();
    }

    public static String getExternalWebUrl(DeploymentArtifact artifact) {
        if (artifact.getMetadata().getManifest().getWebAppInfo() != null)
            return artifact.getMetadata().getManifest().getWebAppInfo().getExternalWebUrl();
        else
            return null;

    }

    public static ArtifactType getType(DeploymentMetadata deploymentMetadata) {
        return getType(deploymentMetadata.getManifest());
    }

    public static ArtifactType getType(DeploymentArtifact deploymentArtifact) {
        return getType(deploymentArtifact.getMetadata());
    }

    public static String getAppId(ArtifactManifest manifest) {
        return manifest.getApplicationInfo().getApplicationId();
    }

    public static String getAppId(DeploymentMetadata deploymentMetadata) {
        return getAppId(deploymentMetadata.getManifest());
    }

    public static String getAppId(DeploymentArtifact deploymentArtifact) {
        return getAppId(deploymentArtifact.getMetadata());
    }

    public static String getNamespace(DeploymentArtifact deploymentArtifact) {
        if (getAppId(deploymentArtifact).equals("common_services")) {
            return getServiceId(deploymentArtifact);
        }
        return getAppId(deploymentArtifact);
    }

    public static String getServiceId(ArtifactManifest manifest) {
        return manifest.getApplicationInfo().getServiceId();
    }

    public static String getServiceId(DeploymentMetadata deploymentMetadata) {
        return getServiceId(deploymentMetadata.getManifest());
    }

    public static String getServiceId(DeploymentArtifact deploymentArtifact) {
        return getServiceId(deploymentArtifact.getMetadata());
    }

    public static String getFqAppId(String appId, String serviceId) {
        if (appId != null)
            return appId + "-" + serviceId;
        else
            return serviceId;
    }

    public static String getFqAppId(ArtifactManifest manifest) {
        if (manifest.getApplicationInfo().isSetApplicationId())
            return getAppId(manifest) + "-" + getServiceId(manifest);
        else
            return getServiceId(manifest);
    }

    public static String getFqAppId(DeploymentMetadata deploymentMetadata) {
        return getFqAppId(deploymentMetadata.getManifest());
    }

    public static String getFqAppId(DeploymentArtifact deploymentArtifact) {
        return getServiceId(deploymentArtifact.getMetadata());
    }

    public static String getSecurityId(ArtifactManifest manifest) {
        return manifest.getApplicationInfo().getSecurityId();
    }

    public static String getSecurityId(DeploymentMetadata deploymentMetadata) {
        return getSecurityId(deploymentMetadata.getManifest());
    }

    public static String getSecurityId(DeploymentArtifact deploymentArtifact) {
        return getSecurityId(deploymentArtifact.getMetadata());
    }

    public static Language getLanguage(ArtifactManifest manifest) {
        return manifest.getArtifactInfo().getLanguage();
    }

    public static Language getLanguage(DeploymentMetadata deploymentMetadata) {
        return getLanguage(deploymentMetadata.getManifest());
    }

    public static Language getLanguage(DeploymentArtifact deploymentArtifact) {
        return getLanguage(deploymentArtifact.getMetadata());
    }

    public static List<String> getDataSets(ArtifactManifest manifest) {
        return manifest.getApplicationInfo().getDatasets();
    }

    public static List<String> getDataSets(DeploymentMetadata deploymentMetadata) {
        return getDataSets(deploymentMetadata.getManifest());
    }

    public static List<String> getDataSets(DeploymentArtifact deploymentArtifact) {
        return getDataSets(deploymentArtifact.getMetadata());
    }

    public static void addFilesToArtifact(DeploymentArtifact artifact, Iterable<ArtifactDataEntry> injectFiles) throws DeploymentException {
        CompressorInputStream uncompressedInput = null;
        ArchiveInputStream input = null;
        ByteArrayOutputStream bos = null;
        try {
            uncompressedInput = new GzipCompressorInputStream(new ByteArrayInputStream(artifact.getArtifact()));
            input = new TarArchiveInputStream(uncompressedInput);
            bos = new ByteArrayOutputStream(artifact.getArtifact().length);
            appendFilesInTarArchive(input, bos, injectFiles);
            artifact.setArtifact(bos.toByteArray());
        } catch (IOException ex) {
            log.error("IOException while attempting to add files to a deployment artifact.", ex);
            throw new DeploymentException(ex.getMessage());
        } finally {
            IOUtils.closeQuietly(uncompressedInput);
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(bos);
        }
    }

    /**
     * Append to the given ArchiveInputStream writing to the given outputstream, the given entries to add.
     * This will duplicate the InputStream to the Output.
     *
     * @param inputStream - archive input to append to
     * @param output      - what to copy the modified archive to
     * @param filesToAdd  - what entries to append.
     */
    private static void appendFilesInTarArchive(ArchiveInputStream inputStream, OutputStream output, Iterable<ArtifactDataEntry> filesToAdd) throws DeploymentException {
        ArchiveStreamFactory asf = new ArchiveStreamFactory();

        try {
            HashMap<String, ArtifactDataEntry> newFiles = new HashMap<>();
            for (ArtifactDataEntry entry : filesToAdd) {
                newFiles.put(entry.getEntry().getName(), entry);
            }
            GZIPOutputStream gzs = new GZIPOutputStream(output);
            TarArchiveOutputStream aos = (TarArchiveOutputStream) asf.createArchiveOutputStream(ArchiveStreamFactory.TAR, gzs);
            aos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            // copy the existing entries
            ArchiveEntry nextEntry;
            while ((nextEntry = inputStream.getNextEntry()) != null) {
                //If we're passing in the same file, don't copy into the new archive
                if (!newFiles.containsKey(nextEntry.getName())) {
                    aos.putArchiveEntry(nextEntry);
                    IOUtils.copy(inputStream, aos);
                    aos.closeArchiveEntry();
                }
            }

            for (ArtifactDataEntry entry : filesToAdd) {
                aos.putArchiveEntry(entry.getEntry());
                IOUtils.write(entry.getData(), aos);
                aos.closeArchiveEntry();
            }
            aos.finish();
            gzs.finish();
        } catch (ArchiveException | IOException e) {
            log.error(e.getMessage(), e);
            throw new DeploymentException(e.getMessage());
        }
    }


}
