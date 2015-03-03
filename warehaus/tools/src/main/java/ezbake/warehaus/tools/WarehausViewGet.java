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
import ezbake.warehaus.ViewId;
import ezbake.warehaus.WarehausService;

public class WarehausViewGet {
    
    @Option(name="-f", aliases="--file", required=true, usage="The file")
    private String file;
    
    @Option(name="-u", aliases="--uri", required=true, usage="The uri")
    private String uri;
    
    @Option(name="-s", aliases="--namespace", required=true, usage="The namespace")
    private String namespace;
    
    @Option(name="-n", aliases="--viewname", required=true, usage="The view name")
    private String viewname;
    
    @Option(name="-v", aliases="--version", usage="The version")
    private String version;
    
    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {

        WarehausViewGet viewGet = new WarehausViewGet();
        CmdLineParser parser = new CmdLineParser(viewGet);

        try {
            parser.parseArgument(args);
            viewGet.process();
            System.out.println("WarehausViewGet started");
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
        EzSecurityToken token = ToolHelper.importToken();
        BinaryReplay binary;
        try {
            if (version != null) {
                binary = client.getLatestView(new ViewId(uri, namespace, viewname), token);
            } else {
                try {
                    binary = client.getView(new ViewId(uri, namespace, viewname), Long.parseLong(version), token);
                } catch (NumberFormatException e) {
                    binary = client.getLatestView(new ViewId(uri, namespace, viewname), token);
                }
            }
            ToolHelper.exportFile(file, binary.getPacket());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            pool.returnToPool(client);
            pool.close();
        }
    }
}
