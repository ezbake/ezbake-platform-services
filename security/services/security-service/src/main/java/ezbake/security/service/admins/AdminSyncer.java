/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.security.service.admins;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.security.thrift.EzSecurity;
import ezbake.security.thrift.ezsecurityConstants;
import ezbake.thrift.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * User: jhastings
 * Date: 7/24/14
 * Time: 9:46 PM
 */
public class AdminSyncer {
    private static final Logger logger = LoggerFactory.getLogger(AdminSyncer.class);

    private Properties properties;
    private ServiceDiscoveryClient discovery;

    @Inject
    public AdminSyncer(Properties ezProperties, ServiceDiscoveryClient discovery) {
        this.properties = ezProperties;
        this.discovery = discovery;
    }

    public void sendUpdates(Set<String> admins) throws Exception {
        logger.info("{} preparing to send updated list of EzAdmins to other instances",
                AdminSyncer.class.getSimpleName());

        List<String> instances = discovery.getEndpoints(ezsecurityConstants.SERVICE_NAME);
        if (instances.isEmpty()) {
            logger.info("Not sending EzAdmin updates. No other instances found");
        }

        for (String instance : instances) {
            HostAndPort hp = HostAndPort.fromString(instance);

            EzSecurity.Client client = null;
            try {

                client = ThriftUtils.getClient(EzSecurity.Client.class, hp, properties);
                logger.info("Sending update to {}", hp);
                client.updateEzAdmins(admins);

            } catch (Exception e) {
                logger.warn("Unable to send admins update to peer: {}", hp);
            } finally {
                if (client != null) {
                    client.getInputProtocol().getTransport().close();
                    client.getOutputProtocol().getTransport().close();
                }
            }
        }
    }
}
