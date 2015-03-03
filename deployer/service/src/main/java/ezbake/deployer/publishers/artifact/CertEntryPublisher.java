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

import com.google.inject.Inject;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;

import java.util.Collection;


public class CertEntryPublisher implements ArtifactContentsPublisher {

    private SSLCertsService certsService;

    @Inject
    public CertEntryPublisher(SSLCertsService certsService) {
        this.certsService = certsService;
    }

    @Override
    public Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException {
        return certsService.get(ArtifactHelpers.getServiceId(artifact), ArtifactHelpers.getSecurityId(artifact));
    }
}
