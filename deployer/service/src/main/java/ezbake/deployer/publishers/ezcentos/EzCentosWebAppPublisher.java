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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class EzCentosWebAppPublisher extends BaseLocalPublisher {
    private static final Logger logger = LoggerFactory.getLogger(EzCentosWebAppPublisher.class);

    @Inject
    public EzCentosWebAppPublisher(EzDeployerConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        String appName = ArtifactHelpers.getAppId(artifact);
        String serviceName = ArtifactHelpers.getServiceId(artifact);
        String jbossDeploymentDir = this.configuration.getEzConfiguration().getProperty("jboss.deployment.directory");

        try {
            String warFile = getArtifactPath(artifact);
            /* NOTE: As of EzCentos box file v0.1.3, the EZCONFIGURATION_DIR is exported in:
               /usr/share/jboss-as/bin/standalone.conf :
               export EZCONFIGURATION_DIR=/vagrant/buildpacks/testweb
             */

            // copy .war file to JBoss deployment directory
            String copyWarCommand = String.format("sudo cp %s %s", warFile, jbossDeploymentDir);
            logger.info("Copy .war to JBoss: Executing command: {}", copyWarCommand);
            Process process = Runtime.getRuntime().exec(copyWarCommand);
            dumpIO(process);

            // check if JBoss is running or not - start it if necessary
            String pid = checkJBossStatus();
            if (pid == null) {
                String startJBossCommand = "sudo service jboss start";
                logger.info("Start JBoss: Executing command: {}", startJBossCommand);
                process = Runtime.getRuntime().exec(startJBossCommand);
                dumpIO(process);
            } else {
                logger.info("JBoss is already running - pid: {}", pid);
            }
            deployments.put(appName + "_" + serviceName, new LocalDeployment(process, warFile, null));

        } catch (Exception ex) {
            throw new DeploymentException("IOException " + ex.getMessage());
        }
    }

    /**
     * @return the JBoss pid; null if it's not running.
     * @throws IOException
     */
    private String checkJBossStatus() throws IOException {
        String pid = null;
        String checkJBossStatusCommand = "sudo service jboss status";
        Process process = Runtime.getRuntime().exec(checkJBossStatusCommand);

        // Example output from this command is: jboss-as is running (pid 16984)
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null;
        if ((line = input.readLine()) != null &&
                line.startsWith("jboss-as is running")) {

            // get the pid by just getting the number from the string
            pid = line.replaceAll("\\D+", "");
        }
        return pid;
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        ApplicationInfo appInfo = artifact.getMetadata().getManifest().getApplicationInfo();
        LocalDeployment deployment = deployments.get(appInfo.getApplicationId() + "_" + appInfo.getServiceId());
        if (deployment != null) {
            // delete the .war in /tmp
            File tmpWar = new File(deployment.artifactFile);
            if (tmpWar.delete()) {
                logger.info("Deleted tmpWar: " + tmpWar);
            }

            // delete the .war from jboss deployment directory
            String jbossDeploymentDir = this.configuration.getEzConfiguration().getProperty("jboss.deployment.directory");
            File deployedWar = new File(jbossDeploymentDir + File.separator + tmpWar.getName());
            if (deployedWar.delete()) {
                logger.info("Deleted deployedWar: " + deployedWar);
            }
        }
    }

}
