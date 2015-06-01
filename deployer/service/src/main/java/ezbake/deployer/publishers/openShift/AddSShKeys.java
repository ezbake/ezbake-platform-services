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

package ezbake.deployer.publishers.openShift;

import com.openshift.client.IOpenShiftConnection;
import com.openshift.client.OpenShiftConnectionFactory;
import com.openshift.client.SSHPublicKey;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.services.deploy.thrift.DeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AddSShKeys {
    private static final Logger log = LoggerFactory.getLogger(Rhc.class);
    private static final String CLIENT_ID = "EzBakeOpenShiftClient";

    private DeployerOpenShiftConfigurationHelper configuration;

    public static void main(String[] args) throws IOException, DeploymentException, EzConfigurationLoaderException {
        DeployerOpenShiftConfigurationHelper deployerConfiguration = new DeployerOpenShiftConfigurationHelper(
                new EzConfiguration().getProperties());
        AddSShKeys keys = new AddSShKeys(deployerConfiguration);
        if (args.length != 2) {
            System.out.println("Usage: " + AddSShKeys.class.getName() + " [String KeyName] [File public_key_file]");
            System.exit(1);
        }
        File keyFile = new File(args[1]);
        if (!keyFile.canRead()) {
            System.err.println("Can not read key file to import into OpenShift.");
            System.exit(3);
        }
        keys.addKey(args[0], keyFile);
    }

    public AddSShKeys(DeployerOpenShiftConfigurationHelper configuration) {
        this.configuration = configuration;
    }

    public void addKey(String keyname, File publicKey) throws DeploymentException, IOException {
        IOpenShiftConnection connection = getOrCreateConnection();
        connection.getUser().putSSHKey(keyname, new SSHPublicKey(publicKey));
    }

    private IOpenShiftConnection getOrCreateConnection() throws DeploymentException {
        try {
            String openshiftUsername = configuration.getUsername();
            String openshiftPassword = configuration.getPassword();
            String openshiftHost = configuration.getHost();

            return new OpenShiftConnectionFactory().getConnection(CLIENT_ID, openshiftUsername, openshiftPassword,
                    openshiftHost);
        } catch (com.openshift.client.OpenShiftEndpointException e) {
            log.error("Error connecting to openshift application", e);
            throw new DeploymentException(e.getMessage());
        }
    }

}
