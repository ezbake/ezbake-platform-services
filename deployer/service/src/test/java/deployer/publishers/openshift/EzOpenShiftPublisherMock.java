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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import deployer.publishers.openshift.inject.ThriftRunnerInjectorMock;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzReverseProxyRegister;
import ezbake.deployer.publishers.artifact.ArtifactContentsPublisher;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector;
import ezbake.deployer.publishers.openShift.inject.CronFileInjector;
import ezbake.deployer.publishers.openShift.inject.SecurityActionHookInjector;
import ezbake.deployer.utilities.ArtifactTypeKey;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.Language;

import java.util.Map;
import java.util.Set;

class EzOpenShiftPublisherMock extends EzOpenShiftPublisher {
    private final Rhc rhc;

    public EzOpenShiftPublisherMock(Rhc rhc, EzReverseProxyRegister reverseProxyRegister) {
        super();
        this.rhc = rhc;
        setReverseProxyRegister(reverseProxyRegister);

        Map<ArtifactTypeKey, Set<ArtifactResourceInjector>> injectors = Maps.newHashMap();
        for (ArtifactTypeKey key : ArtifactTypeKey.getPermutations()) {
            Set<ArtifactResourceInjector> toAdd = Sets.newHashSet();
            if (key.getLanguage() == Language.Java && key.getType() == ArtifactType.Thrift) {
                toAdd.add(new ThriftRunnerInjectorMock());
            }
            toAdd.add(new SecurityActionHookInjector());
            toAdd.add(new CronFileInjector());
            injectors.put(key, toAdd);
        }
        setInjectors(injectors);

        Map<ArtifactTypeKey, Set<ArtifactContentsPublisher>> publishers = Maps.newHashMap();
        for (ArtifactTypeKey key : ArtifactTypeKey.getPermutations()) {
            publishers.put(key, Sets.<ArtifactContentsPublisher>newHashSet());
        }
        setProcessors(publishers);

    }

    @Override
    protected Rhc createRhc() {
        return rhc;
    }
}
