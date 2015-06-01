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

package ezbake.services.search;

import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import org.apache.commons.cli.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.Properties;

public class SSRServer {

    public static void main(String[] args) throws Exception  {
        final String applicationName = "search-app";//cmd.getOptionValue('a');
        final String listenPort = "13004";//cmd.getOptionValue('l');
        final String zookeeperString = "localhost:2181";//cmd.getOptionValue('z');

        Properties props = new EzConfiguration().getProperties();
        props.setProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, applicationName);
        props.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, zookeeperString);
        SSRServiceHandler handler = new SSRServiceHandler();
        handler.setConfigurationProperties(props);

        TServerTransport serverTransport = new TServerSocket(Integer.parseInt(listenPort));
        TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(handler.getThriftProcessor()));

        System.out.println("Starting the SSR service...");
        server.serve();
    }

    private static CommandLine parseCommandLine(String[] args) {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption(buildOption("Application Name", 'a'));
        options.addOption(buildOption("Thrift server listen port",'l'));
        options.addOption(buildOption("Zookeeper string (host:port)",'z'));
        try {
            return parser.parse(options,args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp("ezbake.services.search.SSRServer", options, true);
            System.exit(0);
            return null;
        }
    }

    private static Option buildOption(String desc, char opt) {
        return OptionBuilder.withDescription(desc).hasArg().isRequired().create(opt);
    }
    private static Option buildOptional(String desc, char opt) {
        return OptionBuilder.withDescription(desc).hasArg().isRequired(false).create(opt);
    }
}
