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

package ezbake.security.service;

import com.google.common.net.HostAndPort;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.security.service.processor.EzSecurityHandler;
import ezbake.security.thrift.ezsecurityConstants;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import ezbake.thrift.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/27/13
 * Time: 8:25 AM
 */
//need to run "zkServer start", before running
public class EzSecurityLauncher {
    private static final Logger log = LoggerFactory.getLogger(EzSecurityLauncher.class);
    private static final String ezconfigDir = "conf";
    private static final int defaultPort = 6030;
    private ServiceDiscoveryClient client;
    TServer server;
    
    @Option(name="-h", aliases="--help", usage="print this help message")
    public boolean help;

    @Option(name="-p", aliases="--port", usage="port for security service")
    private int port;

    @Option(name="-pk", aliases="--private-key", usage="the private key file")
    private String privateKey;

    @Option(name="-fs", aliases="--force-ssl", usage="force ssl from properties file")
    private boolean forceSSLFlag;

    @Option(name="-c", aliases="--config", usage="configuration directory")
    private String config = "conf";

    @Option(name="-id", aliases="--security-id", usage="SecurityId of this application (linked to cert folder)")
    private String id = "ssl";

    @Option(name="-m", aliases="--mock", usage="set support for mock clients")
    private boolean mock = false;

    /**
     * Parse command line arguments and start the server
     *
     * @throws Exception
     */
    public void run() throws Exception {
        checkEzConfig();
        // get the port
        if (this.port == 0) {
            this.port = findFreePort();
        }

        // Setup the EzConfiguration
        Properties config = new EzConfiguration().getProperties();
        if (!config.containsKey(EzBakePropertyConstants.EZBAKE_SECURITY_ID)) {
            config.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, this.id);
        }
        if (!config.containsKey(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY)) {
            config.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, new File(this.config, "ssl").getPath());
        }

        if (!config.containsKey(EzBakePropertyConstants.EZBAKE_SECURITY_SERVICE_MOCK_SERVER)) {
            config.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_SERVICE_MOCK_SERVER, String.valueOf(this.mock));
        }

        EzBakeBaseThriftService runner = new EzSecurityHandler();
        runner.setConfigurationProperties(config);

        TProcessor processor = runner.getThriftProcessor();
        TServerSocket socket = ThriftUtils.getSslServerSocket(this.port, config);
        server = new TThreadPoolServer(new TThreadPoolServer.Args(socket).processor(processor).minWorkerThreads(1000));

        // Make the service discoverable
        if(new ZookeeperConfigurationHelper(config).getZookeeperConnectionString() == null) {
            throw new RuntimeException("No zookeeper is available for service discovery!");
        }        

        // register with zookeeper
        client = new ServiceDiscoveryClient(new ZookeeperConfigurationHelper(config).getZookeeperConnectionString());
        client.registerEndpoint(ezsecurityConstants.SERVICE_NAME, getHostName());

        // Add shutdown hook to unregister
        unregisterOnShutdown();
        
        log.info("Starting EzBakeSecurity server: port({})", socket.getServerSocket().getLocalPort());       
        server.serve();
    }

    private void checkEzConfig() {
        String val = System.getProperty("EZCONFIGURATION_DIR");

        if (val == null) {
            System.setProperty("EZCONFIGURATION_DIR", this.config);
        }
    }

    public static int findFreePort() {
        int port;
        try {
            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } catch (IOException e) {
            port = defaultPort;
        }
        return port;
    }
    
    void shutdownServer() throws Exception
    {
        if(server != null && server.isServing()) {
            server.stop();
        }
        
        if(client != null) {
            client.unregisterEndpoint(ezsecurityConstants.SERVICE_NAME, getHostName());
        }
    }
    
    String getHostName() throws IOException {
        String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return HostAndPort.fromParts(hostName, this.port).toString();
    }
    
    private void unregisterOnShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    try {
                        shutdownServer();
                    } catch (Exception e) {
                        // Just log the error and close, endpoint will hang around with no service running
                        System.err.println("Error unregistering endpoint.  Proceeding to close the service discovery client");
                    }
                }
        });
    }

    /**
     * Ezbake Security service main entry point
     *
     * @param args command line arguments
     * @throws IOException on errors accessing key files
     */
    public static void main(String[] args) {
        EzSecurityLauncher server = new EzSecurityLauncher();
        CmdLineParser cmd = new CmdLineParser(server);
        try {
            cmd.parseArgument(args);
            if (server.help == true) {
                cmd.printUsage(System.out);
                System.exit(1);
            }
            server.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmd.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}

