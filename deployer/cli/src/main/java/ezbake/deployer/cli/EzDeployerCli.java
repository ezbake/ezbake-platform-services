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

package ezbake.deployer.cli;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.deployer.cli.commands.DeployCommand;
import ezbake.deployer.cli.commands.DeployTarCommand;
import ezbake.deployer.cli.commands.GetAllApplicationVersionsCommand;
import ezbake.deployer.cli.commands.LatestVersionCommand;
import ezbake.deployer.cli.commands.ListDeployedCommand;
import ezbake.deployer.cli.commands.PingCommand;
import ezbake.deployer.cli.commands.PublishArtifactCommand;
import ezbake.deployer.cli.commands.PublishLatestArtifactCommand;
import ezbake.deployer.cli.commands.PurgeCommand;
import ezbake.deployer.cli.commands.ReregisterCommand;
import ezbake.deployer.cli.commands.SSLCertsCommand;
import ezbake.deployer.cli.commands.UnDeployCommand;
import ezbake.deployer.utilities.InvalidOperation;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;
import ezbake.thrift.ThriftClientPool;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


public class EzDeployerCli implements Closeable {

    private EzBakeServiceDeployer.Client client;
    private Optional<TTransport> transport = Optional.absent();

    private Optional<ThriftClientPool> pool = Optional.absent();

    private GlobalParameters globalParameters;
    private ImmutableMap<String, Command> operations;
    private EzConfiguration configuration;
    List<Command> allCommands;

    public EzDeployerCli(String[] args) throws TTransportException {
        //Using Supplier because the pool hasn't been initialized yet
        Supplier<ThriftClientPool> clientPoolSupplier = Suppliers.memoize(
                new Supplier<ThriftClientPool>() {
                    @Override
                    public ThriftClientPool get() {
                        return pool.get();
                    }
                });
        allCommands = Lists.newArrayList((Command) new DeployCommand(), new DeployTarCommand(),
                new UnDeployCommand(), new PurgeCommand(), new GetAllApplicationVersionsCommand(),
                new LatestVersionCommand(), new ListDeployedCommand(), new PublishLatestArtifactCommand(),
                new PublishArtifactCommand(), new PingCommand(), new SSLCertsCommand(clientPoolSupplier),
                new ReregisterCommand());
        operations = Maps.uniqueIndex(allCommands, new Function<Command, String>() {
            @Override
            public String apply(Command input) {
                return input.getName();
            }
        });

        globalParameters = new GlobalParameters(args, allCommands);
    }

    public int run() throws TException, IOException, EzConfigurationLoaderException {
        if (globalParameters.isVersion()) {
            System.out.println("EzDeployCli version " + getCliVersion());
            return 0;
        }
        if (globalParameters.isHelp()) {
            displayHelp();
            return 0;
        }

        String configurationDir = Preconditions.checkNotNull(globalParameters.getConfigurationDir(), "Must specify the cli configuration dir");
        File confDir = new File(configurationDir);
        if (!confDir.exists() || !confDir.isDirectory()) {
            throw new RuntimeException(configurationDir + " does not exist or is not a directory!");
        }
        configuration = new EzConfiguration(new DirectoryConfigurationLoader(confDir.toPath()),
                new DirectoryConfigurationLoader());

        Supplier<EzBakeServiceDeployer.Client> clientSupplier = Suppliers.memoize(
                new Supplier<EzBakeServiceDeployer.Client>() {
                    @Override
                    public EzBakeServiceDeployer.Client get() {
                        try {
                            return connectClient();
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(configuration.getProperties());
        for (Command cmd : allCommands) {
            cmd.setGlobalParameters(globalParameters).setClientSupplier(clientSupplier).
                    setSecurityClient(securityClient).setConfiguration(configuration);
        }

        this.client = connectClient();
        try {
            getOperation(globalParameters.operation).call();
            return 0;
        } catch (InvalidOperation e) {
            System.out.println(Strings.repeat("\n", 3));
            System.out.println(e.getMessage());
            globalParameters.operation = "";
            displayHelp();
            System.out.println(Strings.repeat("\n", 3));
            return 1;
        } catch (UsageException usage) {
            System.out.println(Strings.repeat("\n", 3));
            usage.cmd.displayHelp();
            System.err.println(usage.errorMessage);
            System.out.println(Strings.repeat("\n", 3));
            return 1;
        }
    }

    private EzBakeServiceDeployer.Client connectClient() throws TException {
        if (globalParameters.getHostName() != null && globalParameters.getPortNumber() > 0) {
            System.out.println("Connecting to " + globalParameters.getHostName() + ":" + globalParameters.getPortNumber());
            this.transport = Optional.fromNullable((TTransport) new TSocket(globalParameters.getHostName(), globalParameters.getPortNumber()));
            this.transport.get().open();
            TProtocol protocol = new TBinaryProtocol(this.transport.get());
            return new EzBakeServiceDeployer.Client(protocol);
        } else {
            // Try ezDiscovery
            pool = Optional.fromNullable(new ThriftClientPool(this.configuration.getProperties()));
            return pool.get().getClient(globalParameters.getServiceName(), EzBakeServiceDeployer.Client.class);
        }
    }

    private void displayHelp() {
        System.out.println("Usage: deployer-cli [--help] [--version] [--debug] [-h|--host] [-p|--port] <operation> [<operation arguments>]");
        System.out.println();
        System.out.println("--version - Displays application version and quits.");
        System.out.println("--help - Displays help information.  Will display operation help if operation is provided.");
        System.out.println("--host,-h - sets host of ezDeployer service to connect to.");
        System.out.println("--port,-p - sets port of ezDeployer service to connect with.");
        System.out.println();
        if (globalParameters.getOperation().isEmpty()) {
            System.out.println("Supported Operations:");
            for (Command operation : operations.values()) {
                System.out.println("\t" + operation.quickUsage());
            }
            System.out.println();
        } else {
            System.out.println("Operation Help:");
            try {
                getOperation(globalParameters.getOperation()).displayHelp();
            } catch (InvalidOperation e) {
                System.out.println(e.getMessage());
            }
        }

    }

    @Override
    public void close() throws IOException {
        if (this.transport.isPresent() && this.transport.get().isOpen())
            this.transport.get().close();
        if (this.pool.isPresent()) {
            this.pool.get().returnToPool(this.client);
            this.pool.get().close();
        }

    }


    public Command getOperation(String operation) {
        if (!operations.containsKey(operation))
            throw new InvalidOperation(operation);
        return operations.get(operation);
    }

    private String getCliVersion() {
        try {
            return CharStreams.toString(new InputStreamReader(EzDeployerCli.class.getResourceAsStream("/ezdeploy/ezdeployer.version")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws TException, IOException {
        int returnVal = 43;
        try {
            EzDeployerCli cli = new EzDeployerCli(args);
            try {
                returnVal = cli.run();
            } finally {
                IOUtils.closeQuietly(cli);
            }
        } catch (DeploymentException e) {
            // A deployment exception won't be useful to print out the stack trace.
            System.out.println(Strings.repeat("\n", 3));
            System.err.println("Error: " + e.getMessage());
            System.out.println(Strings.repeat("\n", 3));
            returnVal = 15;
        } catch (Exception e) {
            e.printStackTrace();
            returnVal = 13;
        } finally {
            System.out.println("Finished");
        }
        System.exit(returnVal);
    }

}