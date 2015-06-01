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

package ezbake.discovery.stethoscope.server;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Multimap;

import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.discovery.stethoscope.thrift.stethoscopeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StethoscopeCacheRemovalListener implements RemovalListener<String, StethoscopeCacheEntry> {

    private ServiceDiscoveryClient serviceDiscoveryClient;
    private boolean shouldRemoveEntriesFromZookeeper;

    private final static Logger logger  = LoggerFactory.getLogger(StethoscopeCacheRemovalListener.class);
    private Multimap<String, String> servicesToIgnore;

    public StethoscopeCacheRemovalListener(ServiceDiscoveryClient client, boolean shouldRemoveEntriesFromZookeeper,
                                           Multimap<String, String> servicesToIgnore) {
        this.serviceDiscoveryClient = client;
        this.shouldRemoveEntriesFromZookeeper = shouldRemoveEntriesFromZookeeper;
        this.servicesToIgnore = servicesToIgnore;
        logger.debug("Setup removal handler");
    }

    @Override
    public void onRemoval(RemovalNotification<String, StethoscopeCacheEntry> entry) {
        if(entry.getCause() == RemovalCause.REPLACED) {
            // We don't want to do anything here since our key gets a new value
            return;
        }

        String appName = entry.getValue().getApplicationName();
        String serviceName = entry.getValue().getServiceName();
        if(servicesToIgnore.containsEntry(appName, serviceName)) {
            logger.info("Ignoring: {},{} and NOT removing that from zookeeper", appName, serviceName);
            return;
        }

        String endpoint = entry.getKey();

        // We want to ignore removing ourself for right now
        if(appName.equals(ServiceDiscoveryClient.COMMON_SERVICE_APP_NAME) &&
           serviceName.equals(stethoscopeConstants.SERVICE_NAME)) {
            return;
        }

        if(!shouldRemoveEntriesFromZookeeper) {
            logger.info("Would have removed: {} for {} {} from zookeeper!", endpoint, appName, serviceName);
            return;
        }

        try {
            serviceDiscoveryClient.unregisterEndpoint(appName, serviceName, endpoint);
            logger.info("Removed: {} for {} {} from zookeeper!", endpoint, appName, serviceName);
        } catch(Exception e) {
            StringBuilder builder = new StringBuilder("We had an error removing /ezdiscovery/")
                                        .append(appName)
                                        .append("/")
                                        .append("/endpoints/")
                                        .append(endpoint)
                                        .append("from zookeeper!");

            logger.error(builder.toString(), e);
        }
    }
}
