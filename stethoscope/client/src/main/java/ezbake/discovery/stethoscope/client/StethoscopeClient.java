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

package ezbake.discovery.stethoscope.client;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import ezbake.base.thrift.EzBakeBaseService;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.common.openshift.OpenShiftUtil;
import ezbake.configuration.EzConfigurationLoader;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.discovery.stethoscope.thrift.StethoscopeService;
import ezbake.discovery.stethoscope.thrift.stethoscopeConstants;
import ezbake.discovery.stethoscope.thrift.Endpoint;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.util.Random;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StethoscopeClient implements Runnable {

    @Option(name="--checkin-interval", usage="The maximum number of minutes to wait before checking in")
    private int checkinInterval = -1;

    @Option(name="--private-service-hostname", usage="The hostname where the service being checked is running")
    String privateServiceHostname = null;

    @Option(name="--private-service-port", usage="The port where the service being checked is running")
    int privateServicePort = -1;

    @Option(name="--public-service-hostname", usage="The hostname where the service being checked is running")
    String publicserviceHostname = null;

    @Option(name="--public-service-port", usage="The port where the service being checked is running")
    int publicservicePort = -1;

    List<Path> additionalConfigurationDirs = Lists.newArrayList();
    @Option(name = "-P", aliases = "--additional-config-dirs", metaVar = "dir1 dir2 dir2")
    void setAdditionalConfigurationDirectory(final String directory) throws CmdLineException {
        Path path = Paths.get(directory);
        if(!Files.isDirectory(path)) {
            throw new CmdLineException(path.toString() + " is not a directory!");
        }

        additionalConfigurationDirs.add(path);
    }

    Properties additionalProperties = new Properties();
    @Option(name = "-D", metaVar = "<property>=<value>", usage = "use value for given property")
    void setProperty(final String property) throws CmdLineException {
        final String[] arr = property.split("=");
        if (arr.length != 2) {
            throw new CmdLineException("Properties must be specified in the form -D<property>=<value> instead of " + property);
        }

        additionalProperties.setProperty(arr[0], arr[1]);
    }

    private final static String STETHOSCOPE_CHECKIN_INTERVAL_MINUTES = "stethoscope.checkin.interval.minutes";
    private final static String NUM_RETRIES_FOR_CREATING_CLIENT = "stethoscope.num.retries";

    private final static Logger logger = LoggerFactory.getLogger(StethoscopeClient.class);

    private static StethoscopeService.Client stethoscopeClient = null;
    private EzProperties configuration;
    private HostAndPort privateHostAndPort;
    private HostAndPort publicHostAndPort;
    private ServiceDiscoveryClient serviceDiscovery;
    private String appName;
    private String serviceName;

    private int numRetries;

    /**
     * We use this constructor for things like the CLI with this constructor it the the responsiblity of the caller to
     * call the init method.
     *
     * @param props the properties which we use for configuration
     */
    public StethoscopeClient(Properties props) {
        this.configuration = new EzProperties(props, true);
    }

    public StethoscopeClient(Properties props, String privateServiceHostname, int privateServicePort, int checkinInterval,
        int numRetries, String publicserviceHostname, int publicservicePort) {
        this.configuration = new EzProperties(props, true);
        this.privateServiceHostname = privateServiceHostname;
        this.privateServicePort = privateServicePort;
        this.checkinInterval = checkinInterval;
        this.numRetries = numRetries;
        this.publicserviceHostname = publicserviceHostname;
        this.publicservicePort = publicservicePort;
        init();
    }

    @Override
    public void run() {
        try {

            ThriftClientPool clientPool = new ThriftClientPool(configuration);
            EzBakeBaseService.Client baseClient = null;

            // The thrift runner takes a bit to start up so lets give it some time
            int attempt = 0;
            while(attempt < numRetries) {
                try {
                    if (configuration.getBoolean(EzBakePropertyConstants.THRIFT_FRAMED_TRANSPORT, false)) {
                        baseClient = ThriftUtils.getClient(EzBakeBaseService.Client.class, privateHostAndPort, null,
                                configuration, new TFramedTransport.Factory());
                    } else {
                        baseClient = ThriftUtils.getClient(EzBakeBaseService.Client.class, privateHostAndPort, configuration);
                    }
                } catch(TTransportException e) {
                    if(attempt < numRetries) {
                        baseClient = null;
                        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                    } else {
                        throw e;
                    }
                }
                ++attempt;
            }

            logger.debug(privateHostAndPort.toString());
            Endpoint endpoint = new Endpoint(publicHostAndPort.getHostText(), publicHostAndPort.getPort());
            Random r = new Random();
            boolean registered = false;
            String hostInfo = publicHostAndPort.toString();

            unregisterOnShutdown();

            while(true) {
                boolean successfulPing = baseClient.ping();
                if (successfulPing) {
                    if(!registered) {
                        serviceDiscovery.registerEndpoint(configuration, publicHostAndPort.toString());
                        logger.info("Registering {} {} at {} with service discovery", appName, serviceName, hostInfo);
                        registered = true;
                    }
                    logger.debug("Got a successful ping");
                    checkInWithStethoscopeServer(clientPool, appName, serviceName, endpoint);
                } else {
                    // we failed our pinged and we are are registered so lets un register
                   if(registered) {
                        serviceDiscovery.unregisterEndpoint(configuration, publicHostAndPort.toString());
                        logger.info("DeRegistering {} {} at {} with service discovery", appName, serviceName, hostInfo);
                        registered = false;
                   }
                }

                int timeToSleep = checkinInterval - 1;
                if(timeToSleep < 1) {
                    timeToSleep = 1;
                }

                int minutesToSleep = r.nextInt(timeToSleep) + 1;
                logger.debug("Sleeping for {} minutes", minutesToSleep);
                Thread.sleep(TimeUnit.MINUTES.toMillis((long)minutesToSleep));
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            if(serviceDiscovery != null) {
                serviceDiscovery.close();
            }
        }
    }

    private void checkInWithStethoscopeServer(ThriftClientPool pool, String appName, String serviceName,
        Endpoint endpoint) {
        try {
            if(stethoscopeClient == null) {
                stethoscopeClient = pool.getClient(stethoscopeConstants.SERVICE_NAME, StethoscopeService.Client.class);
            }
            boolean success = stethoscopeClient.checkin(appName, serviceName, endpoint);
            if(success) {
                logger.info("Successfully checked in with Stethoscope Service!");
            } else {
                logger.info("Failed to check in with Stethoscope Service!");
            }
        } catch(Exception e) {
            stethoscopeClient = null;
            logger.warn("Could not check in to stethoscope server!");
        }
    }

    public void init() {
        Preconditions.checkNotNull(this.configuration, "No properties have been set!");

        // Merge configuration parameters from the command line
        if (!additionalConfigurationDirs.isEmpty()) {
            List<EzConfigurationLoader> loaders = Lists.newArrayList();
            for (Path configDir : additionalConfigurationDirs) {
                loaders.add(new DirectoryConfigurationLoader(configDir));
            }
            try {
                Properties loadedProps = new EzConfiguration(
                        loaders.toArray(new EzConfigurationLoader[loaders.size()])).getProperties();
                configuration.putAll(loadedProps);
            } catch (EzConfigurationLoaderException e) {
                logger.warn("Failed to load additional configuration directories", e);
            }
        }
        if (!additionalProperties.isEmpty()) {
            configuration.putAll(additionalProperties);
        }

        // Check to see if our host and port were set properly, if not then use what we can get from environment
        if(Strings.isNullOrEmpty(privateServiceHostname) || privateServicePort < 1) {
            HostAndPort openshiftHostAndPort = OpenShiftUtil.getThriftPrivateInfo();
            this.privateServiceHostname = openshiftHostAndPort.getHostText();
            this.privateServicePort = openshiftHostAndPort.getPort();
        }
        this.privateHostAndPort = HostAndPort.fromParts(privateServiceHostname, privateServicePort);
        this.publicHostAndPort = HostAndPort.fromParts(publicserviceHostname, publicservicePort);


        // Check to see if our checkinInterval was set, if not then lets get it from EzConfiguration
        if(checkinInterval == -1) {
            String checkinProp = configuration.getProperty(STETHOSCOPE_CHECKIN_INTERVAL_MINUTES);
            String errorMsg = String.format("Checkin Interval was NOT specified please set %s in ezconfiguration",
                STETHOSCOPE_CHECKIN_INTERVAL_MINUTES);
            Preconditions.checkState(!Strings.isNullOrEmpty(checkinProp), errorMsg);
            this.checkinInterval = NumberUtils.toInt(checkinProp);
        }


        EzBakeApplicationConfigurationHelper appHelper = new EzBakeApplicationConfigurationHelper(configuration);
        this.appName = appHelper.getApplicationName();
        this.serviceName = appHelper.getServiceName();
        this.numRetries = configuration.getInteger(NUM_RETRIES_FOR_CREATING_CLIENT, 4);

        this.serviceDiscovery = new ServiceDiscoveryClient(configuration);
    }

    private void unregisterOnShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if(serviceDiscovery != null) {
                        String hostInfo = publicHostAndPort.toString();
                        serviceDiscovery.unregisterEndpoint(configuration, hostInfo);
                        logger.info("DeRegistering {} {} at {} with service discovery", appName, serviceName, hostInfo);
                    }
                } catch (final Exception e) {
                    // Just log the error and close. Endpoint will hang around with no service running
                    logger.error("Error unregistering endpoint. Proceeding to close the service discovery client", e);
                } finally {
                    if(serviceDiscovery != null) {
                        serviceDiscovery.close();
                    }
                }
            }
        }));
    }

    public void doMain(String [] args) throws Exception {
        final CmdLineParser cmdLineParser = new CmdLineParser(this);

        if (args.length == 0) {
            cmdLineParser.printUsage(System.err);
            System.exit(-1);
        }
        cmdLineParser.parseArgument(args);

        init();
        run();
    }

    public static void main( String[] args ) throws Exception {
        EzConfiguration ezConfiguration = new EzConfiguration();
        Properties props = ezConfiguration.getProperties();
        new StethoscopeClient(props).doMain(args);
    }
}


