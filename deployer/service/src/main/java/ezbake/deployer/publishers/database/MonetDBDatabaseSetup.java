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
import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Sets up the properties file for MonetDB.  Doesn't actually create the database or anything.
 */
public class MonetDBDatabaseSetup implements DatabaseSetup {

    private static final String MONET_PROPERTIES_FILE_NAME = "monetdb.properties";
    private static final String ENCRYPTED_MONET_PROPERTIES_FILE_NAME = "encrypted_monetdb.properties";

    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact,
                                             Properties configuration, EzSecurityToken callerToken) throws DeploymentException {

        Map<String, String> valuesMap = new HashMap<>();
        Map<String, String> encryptedValuesMap = new HashMap<>();

        valuesMap.put(EzBakePropertyConstants.MONETDB_HOSTNAME, configuration.getProperty(EzBakePropertyConstants.MONETDB_HOSTNAME));
        valuesMap.put(EzBakePropertyConstants.MONETDB_PORT, configuration.getProperty(EzBakePropertyConstants.MONETDB_PORT));

        //Handle the encrypted properties
        EzProperties ezProperties = new EzProperties(configuration, true);
        ezProperties.setTextCryptoProvider(new SystemConfigurationHelper(configuration).getTextCryptoProvider());
        encryptedValuesMap.put(EzBakePropertyConstants.MONETDB_USERNAME, ezProperties.getProperty(EzBakePropertyConstants.MONETDB_USERNAME));
        encryptedValuesMap.put(EzBakePropertyConstants.MONETDB_PASSWORD, ezProperties.getProperty(EzBakePropertyConstants.MONETDB_PASSWORD));

        //This should be removed as soon as we know swivl can support 2.0 encrypted properties
        if (ezProperties.getBoolean("swivl.not.ready.for.2.0", false)) {
            valuesMap.putAll(encryptedValuesMap);
            String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);
            return Lists.newArrayList(Utilities.createConfCertDataEntry(MONET_PROPERTIES_FILE_NAME, properties.getBytes()));
        } else {
            String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);
            String encryptedProperties = Joiner.on('\n').withKeyValueSeparator("=").join(encryptedValuesMap);

            return Lists.newArrayList(Utilities.createConfCertDataEntry(MONET_PROPERTIES_FILE_NAME, properties.getBytes()),
                    Utilities.createConfCertDataEntry(ENCRYPTED_MONET_PROPERTIES_FILE_NAME, encryptedProperties.getBytes()));
        }
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase("MonetDB");
    }

}
