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

import com.google.common.base.Objects;
import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class LocalWebAppPublisher extends BaseLocalPublisher {
    private static final Logger logger = LoggerFactory.getLogger(LocalWebAppPublisher.class);
    private static int nextPortNumber = 8000;

    @Inject
    public LocalWebAppPublisher(EzDeployerConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        //java -jar simple-web-server.jar war {port} {war file}
        String appName = ArtifactHelpers.getAppId(artifact);
        String serviceName = ArtifactHelpers.getServiceId(artifact);

        try {
            String warFile = getArtifactPath(artifact);
            String serverJar = ezDeployBinDirectory + File.separator + "jetty-runner.jar";
            String ezConfigDir = DirectoryConfigurationLoader.EZCONFIGURATION_DEFAULT_DIR;
            try {
                ezConfigDir = Objects.firstNonNull(System.getenv(DirectoryConfigurationLoader.EZCONFIGURATION_ENV_VAR),
                        System.getProperty(DirectoryConfigurationLoader.EZCONFIGURATION_PROPERTY));
            } catch (NullPointerException ex) {
                //don't care, we fall back to the ezconfiguration default dir
            }
            String command = String.format("java -DEZCONFIGURATION_DIR=%s -jar %s --port %d --path %s %s",
                    ezConfigDir, serverJar, nextPortNumber++, "/" +
                            ArtifactHelpers.getExternalWebUrl(artifact), warFile);
            logger.info("Executing command {}", command);
            Process process = Runtime.getRuntime().exec(command);
            dumpIO(process);
            deployments.put(appName + "_" + serviceName, new LocalDeployment(process, warFile, null));
        } catch (Exception ex) {
            throw new DeploymentException("IOException " + ex.getMessage());
        }
    }
}
