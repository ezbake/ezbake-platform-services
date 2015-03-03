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

import java.io.IOException;

public class PublishLatestArtifactCommand extends BaseCommand {
    @Override
    public void call() throws IOException, TException, DeploymentException {
        String[] args = globalParameters.unparsedArgs;
        minExpectedArgs(2, args, this);
        String applicationId = args[0];
        String serviceId = args[1];
        getClient().publishArtifactLatestVersion(applicationId, serviceId, getSecurityToken());
    }

    @Override
    public String getName() {
        return "publishLatestArtifact";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <applicationId> <serviceId>");
        System.out.println("\t(re)publish the latest version of the application that was deployed");
        System.out.println("\tThis mainly allows redeployment back to newest version if an older version was " +
                "published after initial deployment of newest version");
    }

    @Override
    public String quickUsage() {
        return getName() + " - (re)publish the latest version of the application that was deployed";
    }
}
