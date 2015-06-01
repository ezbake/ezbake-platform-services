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

public class PublishArtifactCommand extends BaseCommand {

    @Override
    public void call() throws IOException, TException, DeploymentException {
        String[] args = globalParameters.unparsedArgs;
        minExpectedArgs(3, args, this);
        String applicationId = args[0];
        String serviceId = args[1];
        String version = args[2];

        getClient().publishArtifact(applicationId, serviceId, version, getSecurityToken());
    }

    @Override
    public String getName() {
        return "publishArtifact";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <applicationId> <serviceId> <version>");
        System.out.println("\t(re)publish a certain version that is specified of the application that was deployed");
    }

    @Override
    public String quickUsage() {
        return getName() + " - (re)publish a certain version that is specified of the application that was deployed";
    }
}
