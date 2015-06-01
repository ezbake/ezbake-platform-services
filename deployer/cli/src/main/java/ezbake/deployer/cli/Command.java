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

package ezbake.deployer.cli;

import com.google.common.base.Supplier;
import ezbake.configuration.EzConfiguration;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;
import org.apache.thrift.TException;

import java.io.IOException;

public interface Command {
    public void call() throws IOException, TException, DeploymentException;

    public String getName();

    public void displayHelp();

    public String quickUsage();

    public Command setClientSupplier(Supplier<EzBakeServiceDeployer.Client> client);

    public Command setGlobalParameters(GlobalParameters globalParameters);

    public Command setConfiguration(EzConfiguration configuration);

    public Command setSecurityClient(EzbakeSecurityClient securityClient);
}
