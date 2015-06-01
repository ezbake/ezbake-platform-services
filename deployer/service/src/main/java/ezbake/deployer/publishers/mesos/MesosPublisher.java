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

package ezbake.deployer.publishers.mesos;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.ResourceReq;
import ezbake.services.deploy.thrift.ResourceRequirements;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;

/**
 * Publishes to a Mesos cluster using the Marathon REST API.
 *
 * @author ehu
 */
public class MesosPublisher implements EzPublisher {
    /**
     * Standard logger
     */
    private static Logger log = LoggerFactory.getLogger(MesosPublisher.class);

    /**
     * REST client to Marathon server.
     */
    private MarathonClient marathonClient;

    private EzDeployerConfiguration config;

    /**
     * Constructor checks that the expected mesos configuration file is present on the classpath.
     *
     * @throws DeploymentException
     */
    public MesosPublisher(EzDeployerConfiguration config) throws DeploymentException {
        this.config = config;
        marathonClient = new MarathonClient(config.getMarathonBasePath());
    }

    /**
     * Mesos implementation of the publish method. This impl will copy over the binary array artifact
     * into a tar.gz file to the web deploy directory. Then it will launch into Mesos based on Artifact type.
     */
    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        try {
            // get meta data
            DeploymentMetadata app = artifact.getMetadata();
            ArtifactManifest meta = app.getManifest();
            ResourceRequirements resReq = meta.getArtifactInfo().getResourceRequirements();
            ArtifactType artType = meta.getArtifactType();

            String appId = meta.getApplicationInfo().getApplicationId();

            // get the mapped value from the enum to actual numbers
            int cpus = config.getMesosCpuSmall();
            if (resReq.getCpu().equals(ResourceReq.medium))
                cpus = config.getMesosCpuMedium();
            else if (resReq.getCpu().equals(ResourceReq.large))
                cpus = config.getMesosCpuLarge();

            int mem = config.getMesosMemSmall();
            if (resReq.getMem().equals(ResourceReq.medium))
                mem = config.getMesosMemMedium();
            else if (resReq.getMem().equals(ResourceReq.large))
                mem = config.getMesosMemLarge();

            int minInstance = meta.getScaling().getNumberOfInstances();

            // copy tar.gz file to webdeploy directory
            String appBundleName = appId + ".tar.gz";
            FileOutputStream fout = new FileOutputStream(config.getMarathonWebDeployDir() + "/" + appBundleName);
            fout.write(artifact.getArtifact());
            fout.close();

            // set the uri of the app tar.gz file
            String uri = config.getMarathonAssetUrl() + "/" + appBundleName;
            String[] uris = {uri};


            // if it's a Thrift or DataSet type, use ThriftRunner
            if (artType.equals(ArtifactType.Thrift) || artType.equals(ArtifactType.DataSet)) {
                // get the filepath to where the thrift runner executable jar is 
                String thriftRunnerPath = config.getMesosThriftRunnerPath();

                // scan for executable jar path
                String jarPath = getJarPath(artifact);
                if (jarPath == null)
                    throw new Exception("Unable to find jar file in bin directory for app: " + appId);

                // build command to send to Marathon
                // the $PORT is an environment variable allocated at runtime by
                // Marathon
                String cmd = "java -jar " + thriftRunnerPath + " " +
                        "-p $PORT " +
                        "-j " + jarPath + " " +
                        "-s " + appId;

                // launch the new app 
                marathonClient.launchApp(appId, cmd, cpus, mem, minInstance, uris);
            } else if (artType.equals(ArtifactType.Custom)) {
                // build command
                String cmd = meta.getCustomServiceInfo().getControlScripts().getStartScript();
                marathonClient.launchApp(appId, cmd, cpus, mem, minInstance, uris);
            }
        } catch (Exception e) {
            log.error("Error publishing to mesos", e);
            throw new DeploymentException(ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        throw new DeploymentException("Not implemented for mesos");
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        marathonClient.validate();
    }

    /**
     * Gets the path including file name to the executable Jar file of the app by searching to the bin folder.
     *
     * @param artifact - the artifact to get the jar path from
     * @return - The jar path to the executable jar
     * @throws Exception
     */
    private String getJarPath(DeploymentArtifact artifact) throws Exception {
        TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(artifact.getArtifact())));

        String jarPath = null;
        TarArchiveEntry entry = tarIn.getNextTarEntry();

        while (entry != null) {
            if (entry.getName().equalsIgnoreCase("bin/")) {
                entry = tarIn.getNextTarEntry();
                jarPath = entry.getName();
                break;
            }
            entry = tarIn.getNextTarEntry();
        }

        tarIn.close();

        return jarPath;
    }
}
