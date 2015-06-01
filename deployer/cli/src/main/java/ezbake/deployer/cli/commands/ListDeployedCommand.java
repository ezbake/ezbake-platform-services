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

import com.google.common.base.Objects;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

public class ListDeployedCommand extends BaseCommand {
    @Override
    public void call() throws IOException, TException, DeploymentException {
        String[] args = globalParameters.unparsedArgs;

        final String term = args.length > 0 ? args[0] : "";
        final String termValue = args.length > 1 ? args[1] : "";
        List<DeploymentMetadata> result = getClient().listDeployed(term, termValue, getSecurityToken());
        if (!result.isEmpty()) {
            System.out.println("Applications Installed");
            System.out.println("---");
            for (DeploymentMetadata app : result) {
                System.out.println("- " + Objects.firstNonNull(ArtifactHelpers.getAppId(app), "common_services") + ":" +
                        ArtifactHelpers.getServiceId(app) + ":" + app.getVersion());
            }
        } else {
            throw new DeploymentException(String.format("No matching application found for %s=%s", term, termValue));
        }
    }

    @Override
    public String getName() {
        return "list-deployed";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + "[security-id|application-id] [search value]");
        System.out.println("\tList the services that are currently deployed.  Optionally giving a fieldName and fieldValue to search on");
    }

    @Override
    public String quickUsage() {
        return getName() + " - List the services that are currently deployed.";
    }
}
