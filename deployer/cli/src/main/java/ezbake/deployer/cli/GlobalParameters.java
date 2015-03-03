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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import ezbake.services.deploy.thrift.EzDeployServiceConstants;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class GlobalParameters {
    @Option(name = "-p", aliases = "--port", usage = "the port to connect to")
    public int portNumber = -1;

    @Option(name = "-h", aliases = "--host", usage = "the host to connect to")
    public String hostName = "localhost";

    @Option(name = "-s", aliases = "--service", usage = "the service to connect to")
    public String serviceName = EzDeployServiceConstants.SERVICE_NAME;

    @Option(name = "--help", usage = "help")
    public boolean help = false;

    @Option(name = "--debug", usage = "debug")
    public boolean debug = false;

    @Option(name = "--version", usage = "version")
    public boolean version = false;

    @Option(name = "-c", aliases = "--cli_configuration", usage = "the directory with the application configuration for the cli")
    public String cliConfDir = System.getenv("EZDEPLOYER_CONF_DIR");

    @Argument(index = 0, required = false)
    public String operation = "";

    @Argument(index = 1)
    public String[] unparsedArgs;

    private List<Command> allCommands;

    private CmdLineParser parser;

    public GlobalParameters(String[] args, List<Command> allCommands) {
        this.allCommands = allCommands;
        parser = new CmdLineParser(this);
        parser.setUsageWidth(80);

        int index = findOperation(args);
        if (index < args.length) {
            unparsedArgs = Arrays.copyOfRange(args, index, args.length);
            args = Arrays.copyOfRange(args, 0, index);
        } else {
            unparsedArgs = new String[0];
        }


        try {
            parser.parseArgument(args);
            if (operation.equals("help") || operation.isEmpty()) {
                help = true;
                if (unparsedArgs.length > 0) {
                    operation = popUnparsedArg();
                } else {
                    operation = "";
                }
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
        }
    }

    private int findOperation(String[] args) {
        Set<String> operationsNames = ImmutableSet.copyOf(Iterables.transform(allCommands, new Function<Command, String>() {
            @Override
            public String apply(Command input) {
                return input.getName();
            }
        }));
        for (int i = 0; i < args.length; i++) {
            if (operationsNames.contains(args[i])) {
                return i + 1;
            }
        }
        return args.length;
    }

    private String popUnparsedArg() {
        String tmp = unparsedArgs[0];
        unparsedArgs = Arrays.copyOfRange(unparsedArgs, 1, unparsedArgs.length);
        return tmp;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public String getHostName() {
        return hostName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isHelp() {
        return help;
    }

    public boolean isVersion() {
        return version;
    }

    public boolean isDebug() {
        return debug;
    }

    public String getOperation() {
        return operation;
    }

    public String[] getUnparsedArgs() {
        return unparsedArgs;
    }

    public String getConfigurationDir() {
        return cliConfDir;
    }

    public CmdLineParser getParser() {
        return parser;
    }
}
