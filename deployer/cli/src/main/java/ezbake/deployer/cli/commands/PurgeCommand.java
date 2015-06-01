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

package ezbake.deployer.cli.commands;

import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.thrift.TException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;

public class PurgeCommand extends BaseCommand {

    // Added this class, cause I figured we might want at some point an option to say, keep latest, and just delete
    // the old artifacts
    public static class PurgeOptions {
        @Argument(index = 0, required = true)
        public String applicationId = "";

        @Argument(index = 1, required = true)
        public String serviceId;
    }

    @Override
    public void call() throws IOException, TException, DeploymentException {
        String[] args = globalParameters.unparsedArgs;
        PurgeOptions options = new PurgeOptions();
        CmdLineParser parser = makeCliParser(options);
        parseArgs(args, parser);
        getClient().deleteArtifact(options.applicationId, options.serviceId, getSecurityToken());
    }

    @Override
    public String getName() {
        return "purge";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <applicationId> <serviceId>");
        System.out.println("\tPurges the given application to the platform. After this operation the application will " +
                "no longer be stored, and thus, won't be able to republish without re-uploading the artifact.");
    }

    @Override
    public String quickUsage() {
        return getName() + " - <applicationId> <serviceId>";
    }

    private CmdLineParser makeCliParser(PurgeOptions options) {
        CmdLineParser parser = new CmdLineParser(options);
        parser.setUsageWidth(80);
        return parser;
    }

    private void parseArgs(String[] args, CmdLineParser parser) throws DeploymentException {
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            throw new DeploymentException(e.getMessage());
        }
    }
}
