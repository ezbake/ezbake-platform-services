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

package ezbake.protect.ezca;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 7:32 AM
 */
public class Parameters {
    @Option(name="-h", aliases="--help", usage="print this help message")
    public boolean help = false;

    @Option(name="-d", aliases="--directory", required=true, usage="root directory of the persisted CA and certificates")
    public String directory;

    @Option(name="-c", aliases="--ez-config", usage="optional directory from which to load EZConfiguration overrides")
    public String configDir = "config";

    @Option(name="-o", aliases="--output-directory", required=false, usage="directory in which to output certificate tarballs")
    public String outDir;

    @Option(name="-n", aliases="--names", usage="clients to bootstrap")
    public String clientNames;

    @Option(name="--dry-run", usage="flag indicating whether or not to push updates to accumulo")
    public boolean dryRun;

    private CmdLineParser parser;

    public Parameters(String[] args) {
        parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    public void printUsage() {
        System.err.println("Usage: ezca-bootstrap -d DIRECTORY [<optional arguments>]");
        parser.printUsage(System.err);
    }
}
