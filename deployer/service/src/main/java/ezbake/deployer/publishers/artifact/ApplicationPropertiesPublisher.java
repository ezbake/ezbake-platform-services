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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static ezbake.deployer.utilities.ArtifactHelpers.getAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getSecurityId;
import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;
import static ezbake.deployer.utilities.Utilities.CONFIG_DIRECTORY;
import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;


public class ApplicationPropertiesPublisher extends ArtifactContentsPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationPropertiesPublisher.class);
    private static final String ERROR_SECURITY_ID_REQUIRED = "securityId is required to be set";
    public static final String CONFIG_APPLICATION_PROPERTIES = CONFIG_DIRECTORY + "/application.properties";

    @Override
    public Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException {
        return createApplicationProperties(artifact);
    }

    private Collection<ArtifactDataEntry> createApplicationProperties(DeploymentArtifact artifact) throws DeploymentException {
        Map<String, String> valuesMap = Maps.newHashMap();

        // Add properties for app name, service name, version
        String appId = getAppId(artifact);
        if (appId != null) {
            valuesMap.put(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, appId);
        }
        valuesMap.put(EzBakePropertyConstants.EZBAKE_SERVICE_NAME, getServiceId(artifact));
        valuesMap.put(EzBakePropertyConstants.EZBAKE_APPLICATION_VERSION, artifact.getMetadata().getVersion());

        // Add EzSecurity properties
        String securityId = getSecurityId(artifact);
        if (securityId == null) {
            DeploymentException e = new DeploymentException(ERROR_SECURITY_ID_REQUIRED);
            logger.error(ERROR_SECURITY_ID_REQUIRED, e);
            throw e;
        }
        valuesMap.put(EzBakePropertyConstants.EZBAKE_SECURITY_ID, securityId);
        valuesMap.put(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, String.format("%s/%s", SSL_CONFIG_DIRECTORY, securityId));

        // Generate
        String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);
        return Lists.newArrayList(new ArtifactDataEntry(new TarArchiveEntry(CONFIG_APPLICATION_PROPERTIES), properties.getBytes()));
    }
}
