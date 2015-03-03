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

package deployer.publishers.openshift;

import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzReverseProxyRegister;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.services.deploy.thrift.DeploymentException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

class EzOpenShiftPublisherMock extends EzOpenShiftPublisher {
    private final Rhc rhc;

    public EzOpenShiftPublisherMock(Rhc rhc, EzReverseProxyRegister reverseProxyRegister) {
        super();
        this.rhc = rhc;
        setReverseProxyRegister(reverseProxyRegister);
    }

    @Override
    protected InputStream getThriftRunnerBinary() throws DeploymentException {
        return new ByteArrayInputStream("I'm a binary".getBytes());
    }

    @Override
    protected Rhc createRhc() {
        return rhc;
    }
}
