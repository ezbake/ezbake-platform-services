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

package ezbake.security.test.suite.app;

import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.*;
import ezbake.protect.test.security.SecurityCommand;
import ezbake.security.test.suite.common.Command;
import ezbake.security.testsuite.registration.RegistrationCommand;
import org.kohsuke.args4j.*;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/8/14
 * Time: 12:02 PM
 */
public class App implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    @Option(name="-h", aliases="--help", help=true)
    boolean help = false;

    @Option(name="-c", aliases="--config", usage="Optional directory to load additional configuration from")
    protected String configDirectory;

    @Argument(metaVar="command", handler=SubCommandHandler.class, required=true, usage="group command to execute")
    @SubCommands({
            @SubCommand(name="security", impl=SecurityCommand.class),
            @SubCommand(name="registration", impl=RegistrationCommand.class),
            @SubCommand(name="performance", impl=PerformanceTestCommand.class)
    })
    Command command;


    private String[] arguments;
    public App(String[] arguments) {
        this.arguments = arguments;
    }

    protected Properties loadEzConfiguration() throws EzConfigurationLoaderException {
        List<EzConfigurationLoader> loaders = new ArrayList<>();
        loaders.add(new DirectoryConfigurationLoader());
        loaders.add(new OpenShiftConfigurationLoader());
        if (configDirectory != null) {
            loaders.add(new DirectoryConfigurationLoader(Paths.get(configDirectory)));
        }

        Properties loaded = new EzConfiguration(loaders.toArray(new EzConfigurationLoader[loaders.size()]))
                .getProperties();
        logger.info("Loaded EzConfiguration properties: {}", loaded);
        return loaded;
    }

    @Override
    public void run() {
        CmdLineParser parser = new CmdLineParser(this, ParserProperties.defaults().withUsageWidth(120));
        try {
            parser.parseArgument(arguments);
            if (help) {
                System.out.println("usage: java -jar test-suite.jar command [options]");
                parser.printUsage(System.out);
            } else {
                command.setConfigurationProperties(loadEzConfiguration());
                command.runCommand();
            }
        } catch(CmdLineException |EzConfigurationLoaderException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws EzConfigurationLoaderException, EzSecurityTokenException, IOException {
        App app = new App(args);
        app.run();
    }
}

