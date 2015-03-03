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

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.warehaus.Repository;

public class CreateWHPFile {
    
    @Option(name="-f", aliases="--file", required=true, usage="The whp file")
    private String file;

    @Option(name="-u", aliases="--uri", required=true, usage="The uri")
    private String uri;

    @Option(name="-r", aliases="--raw", required=true, usage="The raw data file")
    private String raw;

    @Option(name="-p", aliases="--parsed", required=true, usage="The parsed data file")
    private String parsed;

	public static void main(String[] args) throws IOException, TException {
		
		CreateWHPFile whpFile = new CreateWHPFile();	
		CmdLineParser parser = new CmdLineParser(whpFile);

        try {
            parser.parseArgument(args);
            whpFile.process();
            System.out.println("CreateWHPFile started");
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }	
	}
	
	public void process() throws IOException, TException {
	    Repository repository = new Repository();
        repository.setUri(uri);
        repository.setRawData(ToolHelper.importFile(raw));
        repository.setParsedData(ToolHelper.importFile(parsed));
        
        ToolHelper.exportFile(file, new TSerializer().serialize(repository));
	}
	
}
