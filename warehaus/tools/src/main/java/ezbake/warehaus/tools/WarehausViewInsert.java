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
import java.nio.ByteBuffer;
import java.util.Properties;

import ezbake.base.thrift.Visibility;

import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.thrift.ThriftClientPool;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.ViewId;
import ezbake.warehaus.WarehausService;

public class WarehausViewInsert {
    
    @Option(name="-f", aliases="--file", required=true, usage="The view file")
    private String file;
    
    @Option(name="-u", aliases="--uri", required=true, usage="The uri")
    private String uri;
    
    @Option(name="-s", aliases="--namespace", usage="The namespace")
    private String namespace;
    
    @Option(name="-n", aliases="--viewname", usage="The view name")
    private String viewname;
    
    @Option(name="-c", aliases="--classification", required=true, usage="The classification")
    private String classification;
    
    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {
        
        WarehausViewInsert viewInsert = new WarehausViewInsert();
        CmdLineParser parser = new CmdLineParser(viewInsert);

        try {
            parser.parseArgument(args);
            viewInsert.process();
            System.out.println("ViewInsert started");
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
        byte[] binary = ToolHelper.importFile(file);
        System.out.println(client.insertView(ByteBuffer.wrap(binary), new ViewId(uri, namespace, viewname), new Visibility().setFormalVisibility(classification), token));
        pool.returnToPool(client);
        pool.close();
    }
}
