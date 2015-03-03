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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ezbake.deployer.utilities.ArtifactHelpers.getFqAppId;

/**
 * This class is designed to have the deployPackage method called which will take in a package file (war, jar or other type),
 * a yml file, an optional list of config files and an ezbake client.  It will tar these files up and deploy the package.
 * <p/>
 * Created by sstapleton on 2/7/14.
 */
public class PackageDeployer {

    /**
     * Create a list of ArtifactManifest objects given a manifest yml file
     *
     * @param manifestFile The yml file
     * @param token        The ezsecurity token
     * @return List of ArtifactManifest objects
     * @throws IOException Throws if the file cannot be read
     */
    public static List<ArtifactManifest> createManifests(File manifestFile, EzSecurityToken token) throws IOException {
        YamlManifestFileReader ymlReader = new YamlManifestFileReader(new SecurityTokenUserProvider(token));
        return ymlReader.readFile(manifestFile);
    }

    /**
     * Create a list of ArtifactManifest objects given a manifest yml file
     *
     * @param manifestFile The yml file
     * @param token        The ezsecurity token
     * @param overrides    List of yml key entries and corresponding values
     * @return List of ArtifactManifest objects
     * @throws IOException Throws if the file cannot be read
     */
    public static List<ArtifactManifest> createManifests(File manifestFile, EzSecurityToken token,
                                                         Map<String, Object> overrides) throws IOException, IllegalStateException {
        return new YamlManifestFileReader(new SecurityTokenUserProvider(token), overrides).readFile(manifestFile);
    }

    /**
     * Builds an deployment artifact
     *
     * @param artifactMetadata      The metadata from the manifest
     * @param packageFile           The file containing the artifact or package to be deployed
     * @param packageFileName       The file name of the package
     * @param manifestFile          The yml file
     * @param configFiles           A Map of configuration files and their corresponding file names
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @return A serialized tar file of everything to be deployed
     * @throws IOException         If the files cannot be read
     * @throws DeploymentException If the tar file cannot be created
     */
    public static ByteBuffer buildArtifact(ArtifactManifest artifactMetadata, File packageFile, String packageFileName,
                                           File manifestFile, Map<File, String> configFiles,
                                           String applicationSecurityId) throws IOException, DeploymentException {
        List<ArtifactDataEntry> injectFiles = new ArrayList<>();
        //check if is a web app, if so send war to deployments directory else send to bin directory
        if (artifactMetadata.getArtifactType() == ArtifactType.WebApp) {
            // If a bin name is given, respect it. Otherwise, default to ROOT.war
            String warName = artifactMetadata.getArtifactInfo().isSetBin() ?
                    artifactMetadata.getArtifactInfo().getBin() : "ROOT.war";

            injectFiles.add(new ArtifactDataEntry(new TarArchiveEntry(
                    String.format("deployments/%s", warName)), FileUtils.readFileToByteArray(packageFile)));
        } else {
            injectFiles.add(new ArtifactDataEntry(new TarArchiveEntry(
                    String.format("bin/%s", packageFileName)),
                    FileUtils.readFileToByteArray(packageFile)));
        }

        //add the yml file
        injectFiles.add(new ArtifactDataEntry(new TarArchiveEntry(
                getFqAppId(artifactMetadata) + "-manifest.yml"),
                FileUtils.readFileToByteArray(manifestFile)));

        //if any option config files add them to the config directory
        for (Map.Entry<File, String> file : configFiles.entrySet()) {
            injectFiles.add(new ArtifactDataEntry(new TarArchiveEntry(
                    String.format("%s/%s", Utilities.CONFIG_DIRECTORY, file.getValue())),
                    FileUtils.readFileToByteArray(file.getKey())));
        }

        if (applicationSecurityId != null) {
            // Assumption: override manifest's application security id with one from registration form
            // given this is passed to constructor and not based on actual application from manifest
            // only one sec id will be set for n number of manifest as only one manifest is currently supported
            // per deployment
            artifactMetadata.applicationInfo.setSecurityId(applicationSecurityId);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Utilities.appendFilesInTarArchive(output, injectFiles);
        return ByteBuffer.wrap(output.toByteArray());
    }

    /**
     * Builds a deployment artifact
     *
     * @param artifactMetadata      The metadata from the manifest
     * @param manifestFile          The yml file 
     * @param archive               The file containing the artifact already in the form of tar.gz that does not contain manifest file
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @return A serialized tar file of everything to be deployed
     * @throws IOException          If the files cannot be read
     * @throws DeploymentException  If the tar file cannot be created
     */
    public static ByteBuffer buildArtifact(ArtifactManifest artifactMetadata, File manifestFile, File archive, String applicationSecurityId) throws IOException, DeploymentException {
        List<ArtifactDataEntry> injectFiles = new ArrayList<>();

        //add the yml file
        injectFiles.add(new ArtifactDataEntry(new TarArchiveEntry(
                getFqAppId(artifactMetadata) + "-manifest.yml"),
                FileUtils.readFileToByteArray(manifestFile)));

        if (applicationSecurityId != null) {
            // Assumption: override manifest's application security id with one from registration form
            // given this is passed to constructor and not based on actual application from manifest
            // only one sec id will be set for n number of manifest as only one manifest is currently supported
            // per deployment
            artifactMetadata.applicationInfo.setSecurityId(applicationSecurityId);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Utilities.appendFilesInTarArchive(output, FileUtils.readFileToByteArray(archive), injectFiles);
        return ByteBuffer.wrap(output.toByteArray());
    }
    
    /**
     * Builds an deployment artifact
     *
     * @param artifactMetadata      The metadata from the manifest
     * @param packageFile           The file containing the artifact or package to be deployed
     * @param manifestFile          The yml file
     * @param configFiles           A iterable of configuration files
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @return A serialized tar file of everything to be deployed
     * @throws IOException         If the files cannot be read
     * @throws DeploymentException If the tar file cannot be created
     */
    public static ByteBuffer buildArtifact(ArtifactManifest artifactMetadata, File packageFile, File manifestFile,
                                           Iterable<File> configFiles, String applicationSecurityId) throws IOException, DeploymentException {
        return buildArtifact(artifactMetadata, packageFile, packageFile.getName(), manifestFile,
                getConfigFileNames(configFiles), applicationSecurityId);
    }

    
    /**
     * Deploys the package file through ezdeployer
     *
     * @param client                The client for the ezdeployer
     * @param packageFile           The artifact or package to deploy
     * @param manifestFile          The manifest yml file
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @param token                 The ezsecurity token
     * @throws IOException         If the files cannot be read
     * @throws TException          An error trying to deploy
     * @throws DeploymentException An error trying to deploy or create the tar file
     */
    public static void deployPackage(EzBakeServiceDeployer.Client client, File packageFile, File manifestFile,
                                     String applicationSecurityId, EzSecurityToken token)
            throws IOException, TException, DeploymentException {
        deployPackage(client, packageFile, manifestFile, Collections.<File>emptyList(), applicationSecurityId, token);
    }

    /**
     * Deploys the package file through ezdeployer
     *
     * @param client                The client for the ezdeployer
     * @param packageFile           The artifact or package to deploy
     * @param packageFileName       The name of the package file
     * @param manifestFile          The manifest yml file
     * @param configFiles           A Map of configuration files and their corresponding file names
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @param token                 The ezsecurity token
     * @throws IOException         If the files cannot be read
     * @throws TException          An error trying to deploy
     * @throws DeploymentException An error trying to deploy or create the tar file
     */
    public static void deployPackage(EzBakeServiceDeployer.Client client, File packageFile, String packageFileName,
                                     File manifestFile, Map<File, String> configFiles, String applicationSecurityId,
                                     EzSecurityToken token) throws IOException, DeploymentException, TException {
        List<ArtifactManifest> manifests = createManifests(manifestFile, token);
        //yml files may have multiple manifests in them in the future.  For now this will be ran once.
        for (ArtifactManifest artifactMetadata : manifests) {
            ByteBuffer artifact = buildArtifact(artifactMetadata, packageFile, packageFileName, manifestFile,
                    configFiles, applicationSecurityId);
            client.deployService(artifactMetadata, artifact, token);
        }

    }

    /**
     * Deploys the package file through ezdeployer
     *
     * @param client                The client for the ezdeployer
     * @param packageFile           The artifact or package to deploy
     * @param manifestFile          The manifest yml file
     * @param configFiles           A iterable of configuration files
     * @param applicationSecurityId The application's security id, or null to use the value in the manifest
     * @param token                 The ezsecurity token
     * @throws IOException         If the files cannot be read
     * @throws TException          An error trying to deploy
     * @throws DeploymentException An error trying to deploy or create the tar file
     */
    public static void deployPackage(EzBakeServiceDeployer.Client client, File packageFile, File manifestFile,
                                     Iterable<File> configFiles, String applicationSecurityId, EzSecurityToken token)
            throws IOException, DeploymentException, TException {
        deployPackage(client, packageFile, packageFile.getName(), manifestFile, getConfigFileNames(configFiles),
                applicationSecurityId, token);
    }

    /**
     * Only undeploy if it's a frack pipeline
     *
     * @param client       The deployer client
     * @param manifestFile The manifest file
     * @param token        The ezsecurity token
     * @throws IOException
     * @throws DeploymentException
     * @throws TException
     */
    public static void conditionalUndeployPackage(EzBakeServiceDeployer.Client client, File manifestFile,
                                                  EzSecurityToken token)
            throws IOException, DeploymentException, TException {
        undeployPackage(client, manifestFile, true, token);
    }

    /**
     * Undeploy
     *
     * @param client       The deployer client
     * @param manifestFile The manifest file
     * @param token        The ezsecurity token
     * @throws IOException
     * @throws DeploymentException
     * @throws TException
     */
    public static void undeployPackage(EzBakeServiceDeployer.Client client, File manifestFile, EzSecurityToken token)
            throws IOException, DeploymentException, TException {
        undeployPackage(client, manifestFile, false, token);
    }


    private static void undeployPackage(EzBakeServiceDeployer.Client client, File manifestFile, boolean frackOnly,
                                        EzSecurityToken token) throws IOException, DeploymentException, TException {
        List<ArtifactManifest> manifests = createManifests(manifestFile, token);
        //yml files may have multiple manifests in them in the future.  For now this will be ran once.
        for (ArtifactManifest artifactMetadata : manifests) {
            if (artifactMetadata.isSetFrackServiceInfo() || !frackOnly) {
                client.undeploy(artifactMetadata.getApplicationInfo().getApplicationId(),
                        artifactMetadata.getApplicationInfo().getServiceId(), token);
                if (artifactMetadata.isSetFrackServiceInfo()) {
                    //Sleep 35 seconds if this is a frack pipeline to give it time to undeploy
                    try {
                        Thread.sleep(35 * 1000);
                    } catch (InterruptedException ex) {
                        //do nothing
                    }
                }
            }
        }
    }

    private static Map<File, String> getConfigFileNames(Iterable<File> configFiles) {
        Map<File, String> map = new HashMap();
        for (File configFile : configFiles) {
            map.put(configFile, configFile.getName());
        }
        return map;
    }

}
