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

package ezbake.deployer.publishers;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.AppCerts;
import ezbake.security.thrift.EzSecurityRegistration;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftClientPool;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;

/**
 * User: jhastings
 * Date: 6/2/14
 * Time: 4:18 PM
 */
public class EzSecurityRegistrationClient implements SSLCertsService {
    private static Logger logger = LoggerFactory.getLogger(EzSecurityRegistrationClient.class);


    private EzDeployerConfiguration config;
    private final EzbakeSecurityClient securityClient;
    private String registrationSecurityId;

    @Inject
    public EzSecurityRegistrationClient(EzDeployerConfiguration config, EzbakeSecurityClient securityClient) {
        this.config = config;
        this.securityClient = securityClient;
        this.registrationSecurityId = pool.get().getSecurityId(EzSecurityRegistrationConstants.SERVICE_NAME);
    }

    private Supplier<ThriftClientPool> thriftClientPoolSupplier = new Supplier<ThriftClientPool>() {
        @Override
        public ThriftClientPool get() {
            return new ThriftClientPool(config.getEzConfiguration());
        }
    };
    private volatile Supplier<ThriftClientPool> pool = Suppliers.memoize(thriftClientPoolSupplier);

    @Override
    public List<ArtifactDataEntry> get(String applicationId, String securityId) throws DeploymentException {
        EzSecurityToken token;
        try {
            token = securityClient.fetchAppToken(registrationSecurityId);
        } catch (TException e) {
            throw new DeploymentException("Unable to retrieve SSL certs because of failure to get EzSecutiyToken - "
                    + e.getMessage());
        }
        return get(token, applicationId, securityId);
    }

    public List<ArtifactDataEntry> get(EzSecurityToken token, String applicationId, String securityId) throws DeploymentException {
        List<ArtifactDataEntry> certs = new ArrayList<ArtifactDataEntry>();
        EzSecurityRegistration.Client client = null;
        try {
            client = pool.get().getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);

            AppCerts s = client.getAppCerts(token, securityId);
            for (AppCerts._Fields fields : AppCerts._Fields.values()) {
                Object o = s.getFieldValue(fields);
                if (o instanceof byte[]) {
                    String fieldName = fields.getFieldName().replace("_", ".");
                    TarArchiveEntry tae = new TarArchiveEntry(
                            Files.get(SSL_CONFIG_DIRECTORY, securityId, fieldName));
                    certs.add(new ArtifactDataEntry(tae, (byte[]) o));
                }
            }

        } catch (RegistrationException e) {
            logger.error("Unable to download certificates from security service.", e);
            throw new DeploymentException("Unable to download certificates from security service. " + e.getMessage());
        } catch (SecurityIDNotFoundException e) {
            logger.error("Unable to download certificates from security service.", e);
            throw new DeploymentException("Unable to download certificates from security service. " + e.getMessage());
        } catch (TException e) {
            logger.error("Unable to download certificates from security service.", e);
            throw new DeploymentException("Unable to download certificates from security service. " + e.getMessage());
        } finally {
            if (client != null) {
                pool.get().returnToPool(client);
            }
        }
        return certs;

    }
}
