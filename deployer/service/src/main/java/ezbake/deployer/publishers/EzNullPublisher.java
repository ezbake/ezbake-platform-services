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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ezbake.deployer.utilities.ArtifactHelpers.getFqAppId;
import static ezbake.deployer.utilities.Utilities.prettyPrintThriftObject;

/**
 * A NoOp Publisher for use while developing publishers.  And for unit tests that aren't concerned with publishers.
 * This will not do any sort of Publishing to anything.  It will send out a warning though to the logs saying that
 * it did not actually publish.
 */
public class EzNullPublisher implements EzPublisher {
    private static final Logger log = LoggerFactory.getLogger(EzNullPublisher.class);

    EzNullPublisher() {
        log.warn("USING NULL PUBLISHER, NOTHING WILL ACTUALLY PUBLISH!!");
    }


    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        log.warn("Did not publish artifact: {} object: {}", getFqAppId(artifact), prettyPrintThriftObject(artifact));
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        log.warn("Did not unpublish artifact: {} object: {}", getFqAppId(artifact), prettyPrintThriftObject(artifact));
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        log.warn("Validated EzNullPublisher");
    }
}
