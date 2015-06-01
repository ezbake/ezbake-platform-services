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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.common.properties.EzProperties;
import ezbake.discovery.stethoscope.thrift.StethoscopeService;
import ezbake.discovery.stethoscope.thrift.Endpoint;
import ezbake.ezdiscovery.ServiceDiscoveryClient;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import org.apache.commons.lang3.math.NumberUtils;
import org.apache.thrift.TProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StethoscopeServiceHandler extends EzBakeBaseThriftService implements StethoscopeService.Iface {
    public final static String STETHOSCOPE_SERVICE_WRITE_EXPIRE_TIME = "stethoscope.service.write.expire.time.minutes";
    public final static String STETHOSCOPE_SERVICE_CLEANUP_TIME = "stethoscope.service.cleanup.time.minutes";
    public final static String STETHOSCOPE_ACTUALLY_REMOVE_FROM_ZOOKEEPER = "stethoscope.actually.remove.from.zookeeper";
    public final static String STETHOSCOPE_SERVICES_TO_IGNORE = "stethoscope.services.to.ignore";

    private final static Logger logger = LoggerFactory.getLogger(StethoscopeServiceHandler.class);

    private Cache<String, StethoscopeCacheEntry> serviceCache;

    private ServiceDiscoveryClient serviceDiscoveryClient;

    private EzProperties configuration;

    private ScheduledExecutorService scheduler;

    @Override
    public boolean checkin(String applicationName, String serviceName, Endpoint endpoint) {
        this.serviceCache.put(endpointToString(endpoint),
            new StethoscopeCacheEntry(applicationName, serviceName, System.nanoTime()));
        // Just make sure we access to make Guava happy
        this.serviceCache.getIfPresent(endpointToString(endpoint));
        logger.info("{} for {}{} just checked in!", endpointToString(endpoint), applicationName, serviceName);
        return true;
    }

    @Override
    public TProcessor getThriftProcessor() {
        this.configuration = new EzProperties(getConfigurationProperties(), true);
        this.serviceDiscoveryClient = new ServiceDiscoveryClient(configuration);

        int expireMinutes  = configuration.getInteger(STETHOSCOPE_SERVICE_WRITE_EXPIRE_TIME, 15);
        boolean shouldRemoveEntriesFromZookeeper = configuration.getBoolean(STETHOSCOPE_ACTUALLY_REMOVE_FROM_ZOOKEEPER,
                                                        false);
        logger.info("Stethoscope will wait {} minutes before timing something out after write", expireMinutes);

        Multimap<String, String> servicesToIgnore = getServicesToIgnore(configuration.getProperty(STETHOSCOPE_SERVICES_TO_IGNORE, ""));

        for(Map.Entry<String, String> entry : servicesToIgnore.entries()) {
            logger.info("Application: {}, Service: {} will NOT be removed from zookeeper",
                            entry.getKey(), entry.getValue());
        }

        if(shouldRemoveEntriesFromZookeeper) {
            logger.info("Stethoscope will remove entries from zookeeper");
        } else {
            logger.info("Stethoscope will NOT remove entries from zookeeper");
        }

        this.serviceCache = CacheBuilder.newBuilder()
                                        .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                                        .removalListener(new StethoscopeCacheRemovalListener(serviceDiscoveryClient,
                                                            shouldRemoveEntriesFromZookeeper, servicesToIgnore))
                                        .build();

        this.scheduler = Executors.newScheduledThreadPool(1);
        int cleanupMinutes = configuration.getInteger(STETHOSCOPE_SERVICE_CLEANUP_TIME, 10);
        logger.info("Stethoscope will wait {} minutes before running the clean up thread!", cleanupMinutes);
        scheduler.scheduleAtFixedRate(new CacheMaintenanceRunnable(), 0, cleanupMinutes, TimeUnit.MINUTES);
        populateCacheFromZookeeper();
        return new StethoscopeService.Processor(this);
    }

    @VisibleForTesting
    static String endpointToString(Endpoint endpoint) {
        return HostAndPort.fromParts(endpoint.getHostname(), endpoint.getPort()).toString();
    }

    /**
     * We use this to intially populate the cache
     */
    @VisibleForTesting
    void populateCacheFromZookeeper() {
        int count = 0;
        try {
            // So many loops :P
            for(String appName : serviceDiscoveryClient.getApplications()) {
                for(String serviceName : serviceDiscoveryClient.getServices(appName)) {
                    for(String endpoint : serviceDiscoveryClient.getEndpoints(appName, serviceName)) {
                        logger.info("Loaded application {}, service: {}, endpoint {}", appName, serviceName, endpoint);
                        ++count;
                        serviceCache.put(endpoint, new StethoscopeCacheEntry(appName, serviceName, System.nanoTime()));
                    }
                }
            }
        } catch(Exception e) {
            logger.error("Unable to load the cache from zookeeper", e);
            throw new RuntimeException(e);
        }
        logger.info("Loaded {} entries from zookeeper", count);
    }

    /**
     * A private interal class which will force clean up at a given interval
     */
    private class CacheMaintenanceRunnable implements Runnable {
        @Override
        public void run() {
            logger.info("Cleaning up cache!");
            serviceCache.cleanUp();
            logger.info("The size of the case is {} ", serviceCache.size());
        }
    }

    private Multimap<String, String> getServicesToIgnore(final String serviceString) {
        Multimap<String, String> retVal = ArrayListMultimap.create();
        for(String entry : Splitter.on(',').split(serviceString)) {
            String [] tokens = entry.split("/");
            if(tokens.length < 2) {
                continue;
            }
            retVal.put(tokens[0], tokens[1]);
        }
        return retVal;
    }
}

