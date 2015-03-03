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
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class that setups an Elasticsearch properties file
 */
public class ElasticsearchDatabaseSetup implements DatabaseSetup {

    private static final String ELASTICSEARCH_PROPERTIES_FILE_NAME = "elasticsearch.properties";

    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact,
                                             Properties configuration, EzSecurityToken callerToken) throws DeploymentException {
        ElasticsearchConfigurationHelper esConfig = new ElasticsearchConfigurationHelper(configuration);

        Map<String, String> valuesMap = Maps.newHashMap();
        valuesMap.put(EzBakePropertyConstants.ELASTICSEARCH_HOST, esConfig.getElasticsearchHost());
        valuesMap.put(EzBakePropertyConstants.ELASTICSEARCH_PORT, Integer.toString(esConfig.getElasticsearchPort()));
        valuesMap.put(EzBakePropertyConstants.ELASTICSEARCH_CLUSTER_NAME, esConfig.getElasticsearchClusterName());
        valuesMap.put(EzBakePropertyConstants.ELASTICSEARCH_FORCE_REFRESH_ON_PUT, Boolean.toString(esConfig.getForceRefresh()));

        String properties = Joiner.on('\n').withKeyValueSeparator("=").join(valuesMap);

        return Lists.newArrayList(Utilities.createConfCertDataEntry(ELASTICSEARCH_PROPERTIES_FILE_NAME, properties.getBytes()));
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase("Elasticsearch");
    }

}
