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

import com.google.common.collect.Lists;
import ezbake.deployer.utilities.PackageDeployer;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DeployCommand extends BaseCommand {

    @Override
    public void call() throws IOException, TException, DeploymentException {
        String[] args = globalParameters.unparsedArgs;
        minExpectedArgs(2, args, this);

        File jarFile = new File(args[0]);
        File configDataDirectory = new File(args[1]);
        File manifest = new File(args[2]);

        File[] configFiles = configDataDirectory.listFiles();
        List<File> children = configFiles == null ? new ArrayList<File>() : Lists.newArrayList(configFiles);
        PackageDeployer.deployPackage(getClient(), jarFile, manifest, children, null, getSecurityToken());
    }

    @Override
    public String getName() {
        return "deploy";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <jar> <config> <manifest file>");
        System.out.println("\tDeploys the given application to the platform.");
        System.out.println("\tjar - The jar file to be ran by thrift runner.");
        System.out.println("\tconfig - Configuration directory to pick up config files from");
        System.out.println("\tmanifest file - The manifest file to read from for deployment information");
        System.out.println("\t");

    }


    @Override
    public String quickUsage() {
        return getName() + " - deploy an application artifact to the platform";
    }
}
