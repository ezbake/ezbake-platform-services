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

package ezbake.deployer.publishers.database;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.ApplicationRegistration;
import ezbake.security.thrift.EzSecurityRegistration;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.NamespaceOperationsImpl;
import org.apache.accumulo.core.client.impl.SecurityOperationsImpl;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class that setups an Accumulo User and Namespace for the application
 */
public class AccumuloDatabaseSetup implements DatabaseSetup {
    private static final Logger logger = LoggerFactory.getLogger(AccumuloDatabaseSetup.class);
    private final SecureRandom random = new SecureRandom();
    private static final String ACCUMULO_PROPERTIES_FILE_NAME = "accumulo.properties";
    private static final String ENCRYPTED_ACCUMULO_PROPERTIES_FILE_NAME = "encrypted_accumulo.properties";
    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;

    private static final NamespacePermission[] USER_DEFAULT_NAMESPACE_PERMISSIONS = new NamespacePermission[]{
            NamespacePermission.CREATE_TABLE,
            NamespacePermission.DROP_TABLE,
            NamespacePermission.ALTER_TABLE,
            NamespacePermission.READ,
            NamespacePermission.WRITE,
            NamespacePermission.BULK_IMPORT
    };

    @Inject
    public AccumuloDatabaseSetup(ThriftClientPool pool, EzbakeSecurityClient securityClient) {
        this.pool = pool;
        this.securityClient = securityClient;
    }

    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact, Properties configuration, EzSecurityToken callerToken) throws DeploymentException {
        EzSecurityRegistration.Client client = null;
        AccumuloHelper accumuloConfiguration = new AccumuloHelper(configuration);

        String instanceName = accumuloConfiguration.getAccumuloInstance();
        String zooServers = accumuloConfiguration.getAccumuloZookeepers();

        Boolean useMock = accumuloConfiguration.useMock();
        String namespaceName = ArtifactHelpers.getNamespace(artifact);
        String userName = String.format("%s_user", namespaceName);
        String password = new BigInteger(130, random).toString(32);

        Instance instance = new ZooKeeperInstance(instanceName, zooServers);
        Credentials credentials = new Credentials(accumuloConfiguration.getAccumuloUsername(),
                new PasswordToken(accumuloConfiguration.getAccumuloPassword()));
        NamespaceOperationsImpl namespaceOpsImpl = new NamespaceOperationsImpl(instance, credentials, new TableOperationsImpl(instance, credentials));
        SecurityOperationsImpl secOpsImpl = new SecurityOperationsImpl(instance, credentials);

        try {
            if (!secOpsImpl.listLocalUsers().contains(userName)) { // user does not exist, create one
                secOpsImpl.createLocalUser(userName, new PasswordToken(password));
            } else { // user exists, re-generate and reset password
                secOpsImpl.changeLocalUserPassword(userName, new PasswordToken(password));
            }

            // Add the application's authorizations to the user. We are doing this every time in case there is a change
            // in the user's auths.
            EzSecurityToken token = securityClient.fetchDerivedTokenForApp(callerToken, pool.getSecurityId(EzSecurityRegistrationConstants.SERVICE_NAME));
            client = pool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            ApplicationRegistration registration = client.getRegistration(token, ArtifactHelpers.getSecurityId(artifact));
            List<String> authorizations = registration.getAuthorizations();
            if (registration.getCommunityAuthorizations() != null && registration.getCommunityAuthorizations().size() > 0) {
                authorizations.addAll(registration.getCommunityAuthorizations());
            }
            Authorizations accumuloAuths = new Authorizations(authorizations.toArray(new String[authorizations.size()]));
            secOpsImpl.changeUserAuthorizations(userName, accumuloAuths);

            if (!namespaceOpsImpl.exists(namespaceName)) {
                namespaceOpsImpl.create(namespaceName);
            }

            // grant default permissions to user
            for (NamespacePermission permission : USER_DEFAULT_NAMESPACE_PERMISSIONS) {
                secOpsImpl.grantNamespacePermission(userName, namespaceName, permission);
            }

            // Create accumulo.properties file
            Map<String, String> valuesMap = Maps.newHashMap();
            valuesMap.put(EzBakePropertyConstants.ACCUMULO_INSTANCE_NAME, instanceName);
            valuesMap.put(EzBakePropertyConstants.ACCUMULO_ZOOKEEPERS, zooServers);
            valuesMap.put(EzBakePropertyConstants.ACCUMULO_NAMESPACE, namespaceName);
            valuesMap.put(EzBakePropertyConstants.ACCUMULO_USE_MOCK, useMock.toString());

            Map<String, String> encryptedValuesMap = Maps.newHashMap();
            encryptedValuesMap.put(EzBakePropertyConstants.ACCUMULO_USERNAME, userName);
            encryptedValuesMap.put(EzBakePropertyConstants.ACCUMULO_PASSWORD, password);

            String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);
            String encryptedProperties = Joiner.on('\n').withKeyValueSeparator("=").join(encryptedValuesMap);

            List<ArtifactDataEntry> entries = Lists.newArrayList();
            entries.add(Utilities.createConfCertDataEntry(ACCUMULO_PROPERTIES_FILE_NAME, properties.getBytes()));
            entries.add(Utilities.createConfCertDataEntry(ENCRYPTED_ACCUMULO_PROPERTIES_FILE_NAME, encryptedProperties.getBytes()));
            return entries;
        } catch (AccumuloException | AccumuloSecurityException e) {
            logger.error("Failed to create or update accumulo user", e);
            throw new DeploymentException("Failed to create or update accumulo user: " + e.getMessage());
        } catch (NamespaceExistsException e) {
            logger.error("Failed to create namespace", e);
            throw new DeploymentException("Failed to create namespace: " + e.getMessage());
        } catch (TException e) {
            logger.error("Failed to set Accumulo authorizations for user. Could not retrieve Registration client", e);
            throw new DeploymentException("Failed to set Accumulo authorizations for user: " + e.getMessage());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase("Accumulo");
    }
}
