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

package ezbake.warehaus.tools;

import java.io.IOException;
import java.util.Properties;

import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.thrift.ThriftClientPool;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.BinaryReplay;
import ezbake.warehaus.WarehausService;

public class WarehausGet {

    @Option(name="-f", aliases="--file", required=true, usage="The export tar file to import")
    private String file;

    @Option(name="-u", aliases="--uri", required=true, usage="The uri")
    private String uri;

    @Option(name="-t", aliases="--type", required=true, usage="The data type: [raw] or [parsed]")
    private String type;

    @Option(name="-v", aliases="--version", usage="The version number")
    private Long version;

    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {
        
        WarehausGet getter = new WarehausGet();        
        CmdLineParser parser = new CmdLineParser(getter);

        try {
            parser.parseArgument(args);
            getter.process();
            System.out.println("WarehausGet started");
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }
    
    public void process() throws TException, IOException, EzConfigurationLoaderException {
        Properties config;
        try {
            config = new EzConfiguration().getProperties();
        } catch (EzConfigurationLoaderException e) {
            throw new RuntimeException(e);
        }
        ThriftClientPool pool = new ThriftClientPool(config);
        WarehausService.Client client = ToolHelper.createClient(pool);
        EzSecurityToken token = ToolHelper.importToken(pool);
 
        try {
            if (version != null) {
                if (type.toLowerCase().equals("raw")) {
                    BinaryReplay replay = client.getRaw(uri, version, token);
                    ToolHelper.exportFile(file, replay.getPacket());
                } else if (type.toLowerCase().equals("parsed")) {
                    BinaryReplay replay = client.getParsed(uri, version, token);
                    ToolHelper.exportFile(file, replay.getPacket());
                }
            } else {
                if (type.toLowerCase().equals("raw")) {
                    BinaryReplay replay = client.getLatestRaw(uri, token);
                    ToolHelper.exportFile(file, replay.getPacket());
                } else if (type.toLowerCase().equals("parsed")) {
                    BinaryReplay replay = client.getLatestParsed(uri, token);
                    ToolHelper.exportFile(file, replay.getPacket());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            pool.returnToPool(client);
            pool.close();
        }
    }
}
