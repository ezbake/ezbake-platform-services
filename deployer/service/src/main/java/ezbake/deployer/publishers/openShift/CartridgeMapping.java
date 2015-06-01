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

package ezbake.deployer.publishers.openShift;

import com.beust.jcommander.internal.Maps;
import com.openshift.client.cartridge.IStandaloneCartridge;
import ezbake.deployer.utilities.ArtifactTypeKey;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.Language;

import java.util.Iterator;
import java.util.Map;

/**
 * This provides a mapping between application type + language and Cartridge
 */
public class CartridgeMapping implements Iterable<Map.Entry<ArtifactTypeKey, IStandaloneCartridge>> {

    private Map<ArtifactTypeKey, IStandaloneCartridge> mapping;

    public CartridgeMapping(DeployerOpenShiftConfigurationHelper configuration) {
        mapping = Maps.newHashMap();

        // Populate the mapping for each type in the permutations
        for (ArtifactTypeKey key : ArtifactTypeKey.getPermutations()) {
            IStandaloneCartridge cartridge;

            if (key.getLanguage() == Language.Java) {
                if (key.getType() == ArtifactType.WebApp) {
                    cartridge = configuration.getJbossasCartridge();
                } else {
                    cartridge = configuration.getThriftRunnerCartridge();
                }
            } else if (key.getLanguage() == Language.NodeJs) {
                cartridge = configuration.getNodeJsCartridge();
            } else if (key.getLanguage() == Language.Python) {
                cartridge = configuration.getPythonCartridge();
            } else {
                // Default to JBoss if nothing else matched
                cartridge = configuration.getJbossasCartridge();
            }

            mapping.put(key, cartridge);
        }
    }

    public IStandaloneCartridge get(ArtifactTypeKey key) throws DeploymentException {
        IStandaloneCartridge cartridge = mapping.get(key);
        if (cartridge == null) {
            throw new DeploymentException("No cartridge mapping registered for artifact type: " + key);
        }
        return cartridge;
    }

    @Override
    public Iterator<Map.Entry<ArtifactTypeKey, IStandaloneCartridge>> iterator() {
        return mapping.entrySet().iterator();
    }

}
