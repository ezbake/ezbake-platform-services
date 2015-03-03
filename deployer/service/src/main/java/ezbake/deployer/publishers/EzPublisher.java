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

package ezbake.deployer.publishers;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;

/**
 * This interface is the service layer which actually does the publishing of an artifact to the respective PaaS.
 */
public interface EzPublisher {
    /**
     * This will publish the artifact to the given implementation of PaaS
     * <p/>
     * The artifact at this point in time will already have included the SSL certs.
     * <p/>
     * Its up to the publisher to reorganize the tar file if needed for its PaaS
     *
     * @param artifact    - The artifact to deploy
     * @param callerToken - The token of the user or application that initiated this call
     * @throws DeploymentException - On any exceptions
     */
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException;

    /**
     * This will remove the artifact from the given implementation of PaaS
     * <p/>
     * The artifact at this point will be unavailable for use by consumers.
     *
     * @param artifact    - The artifact to remove from deployment
     * @param callerToken - The token of the user or application that initiated this call
     * @throws DeploymentException - On any exceptions
     */
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException;

    /**
     * Validate the publisher's environment including attempting to do a health check on the publisher's service if
     * available for this publisher.
     *
     * @throws DeploymentException   - On any health issues of the service
     * @throws IllegalStateException - On any illegal state for the current publisher
     */
    public void validate() throws DeploymentException, IllegalStateException;
}
