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

import ezbake.base.thrift.Visibility;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.thrift.ThriftClientPool;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.Repository;
import ezbake.warehaus.WarehausService;

public class WarehausInsert {

    @Option(name="-f", aliases="--file", required=true, usage="The whp file to import")
    private String file;

    @Option(name="-c", aliases="--classification", required=true, usage="The classification")
    private String classification;

    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {
        
        WarehausInsert inserter = new WarehausInsert();
        CmdLineParser parser = new CmdLineParser(inserter);

        try {
            parser.parseArgument(args);
            inserter.process();
            System.out.println("WarehausInsert started");
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
        Repository whp = new Repository();
        new TDeserializer().deserialize(whp,ToolHelper.importFile(file));
        System.out.println(client.insert(whp, new Visibility().setFormalVisibility(classification), token).getTimestamp());
        pool.returnToPool(client);
        pool.close();

    }
}
