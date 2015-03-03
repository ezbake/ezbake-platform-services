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

import com.google.common.base.Supplier;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.deployer.cli.Command;
import ezbake.deployer.cli.GlobalParameters;
import ezbake.deployer.cli.UsageException;
import ezbake.deployer.utilities.SystemUserProvider;
import ezbake.deployer.utilities.YamlManifestFileReader;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.EzBakeServiceDeployer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public abstract class BaseCommand implements Command {
    protected GlobalParameters globalParameters;
    protected Supplier<EzBakeServiceDeployer.Client> client;
    protected EzConfiguration configuration;
    private EzbakeSecurityClient securityClient;
    private static final Logger log = LoggerFactory.getLogger(BaseCommand.class);

    @Override
    public Command setClientSupplier(Supplier<EzBakeServiceDeployer.Client> client) {
        this.client = client;
        return this;
    }

    @Override
    public Command setConfiguration(EzConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public Command setSecurityClient(EzbakeSecurityClient securityClient) {
        this.securityClient = securityClient;
        return this;
    }

    protected EzBakeServiceDeployer.Client getClient() throws TException {
        try {
            return this.client.get();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TException) {
                throw (TException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    protected EzSecurityToken getSecurityToken() throws DeploymentException {
        return getSecurityToken(null);
    }

    protected EzSecurityToken getSecurityToken(String appId) throws DeploymentException {
        try {
            if (appId == null) {
                return securityClient.fetchAppToken();
            } else {
                return securityClient.fetchAppToken(appId);
            }
        } catch (Exception e) {
            log.error("Failed App Info call", e);
            throw new DeploymentException("Failed to get security token with App Info call");
        }
    }

    @Override
    public Command setGlobalParameters(GlobalParameters globalParameters) {
        this.globalParameters = globalParameters;
        return this;
    }

    public static void minExpectedArgs(int expected, String[] args, Command cmd) {
        if (args.length < expected) {
            throw new UsageException(cmd, cmd.getName(), "Expected at least " + expected + " number of arguments.");
        }
    }

    public static List<ArtifactManifest> readManifestFile(File manifest) throws IOException, TException {
        return new YamlManifestFileReader(new SystemUserProvider()).readFile(manifest);
    }

}
