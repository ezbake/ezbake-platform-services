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

import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeployTarCommand extends BaseCommand {

    @Override
    public void call() throws IOException, TException, DeploymentException {

        String[] args = globalParameters.unparsedArgs;
        minExpectedArgs(1, args, this);
        String prefix = trimNamePrefix(args[0]);
        final File manifest = new File(prefix + "-manifest.yml");
        final File artifact = new File(prefix + ".tar.gz");

        ArtifactManifest artifactManifest = readManifestFile(manifest).get(0);
        byte[] artifactBuffer = FileUtils.readFileToByteArray(artifact);

        getClient().deployService(artifactManifest, ByteBuffer.wrap(artifactBuffer), getSecurityToken());
    }

    private String trimNamePrefix(String fileName) {
        int dotIndex = fileName.lastIndexOf("-manifest.yml\"");
        if (dotIndex != -1) {
            return fileName.substring(0, dotIndex);
        }
        dotIndex = fileName.lastIndexOf(".tar.gz");
        if (dotIndex != -1) {
            return fileName.substring(0, dotIndex);
        }
        return fileName;
    }

    @Override
    public String getName() {
        return "deploy-tar";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <applicationPack>");
        System.out.println("\tDeploys the given application to the platform.");
        System.out.println("\tapplication pack is expected to be a file prefix that matches the following files:");
        System.out.println("\t\t<applicationPack>-manifest.yml - The application manifest file for application metadata");
        System.out.println("\t\t<applicationPack>.tar.gz - The application binary to be deployed in tar.gz format");
        System.out.println("\t\t\tThe tar.gz is expected to be in this format for a Thrift artifact:");
        System.out.println("\t\t\t+-bin\\");
        System.out.println("\t\t\t|   +- applicationName.jar");
        System.out.println("\t\t\t+-config\\");
        System.out.println("\t\t\t|   +- applicationConfig.properties");
        System.out.println("\t\t\t|   +- anyOtherEzConfig.properties");
        System.out.println("\t\t\t\\------");
        System.out.println("\t\t\tThe tar.gz is expected to be in this format for a webapp artifact:");
        System.out.println("\t\t\t+-deployments\\");
        System.out.println("\t\t\t|   +- ROOT.war");
        System.out.println("\t\t\t|   +- attachedWars.war");
        System.out.println("\t\t\t+-config\\");
        System.out.println("\t\t\t|   +- applicationConfig.properties");
        System.out.println("\t\t\t|   +- anyOtherEzConfig.properties");
        System.out.println("\t\t\t\\------");
    }

    @Override
    public String quickUsage() {
        return getName() + " - deploy an application artifact to the platform";
    }
}
