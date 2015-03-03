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
import java.util.List;
import java.util.Properties;

import ezbake.data.common.TimeUtil;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.thrift.ThriftClientPool;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.DatedURI;
import ezbake.warehaus.WarehausService;

public class WarehausReplay {

    @Option(name="-u", aliases="--urn", required=true, usage="The URI prefix in the form <CATEGORY>://<FEED NAME>")
    private String urn;

    @Option(name="-s", aliases="--start", usage="The unix start time")
    private Long start;

    @Option(name="-e", aliases="--end", usage="The unix end time")
    private Long end;

    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {
        
        WarehausReplay replay = new WarehausReplay();
        CmdLineParser parser = new CmdLineParser(replay);

        try {
            parser.parseArgument(args);
            replay.process();
            System.out.println("WarehausReplay started");
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
        DateTime starttime = null;
        DateTime endtime = null;

        if (start != null) {
            starttime = TimeUtil.convertToThriftDateTime(start);
        }
        if (end != null) {
            endtime = TimeUtil.convertToThriftDateTime(end);
        }
        List<DatedURI> list = client.replay(urn, false, starttime, endtime, null, token);
        for (DatedURI uri : list) {
            System.out.println(TimeUtil.convertFromThriftDateTime(uri.getTimestamp()) + " : " + uri.getUri());
        }
        pool.returnToPool(client);
        pool.close();
    }
}
