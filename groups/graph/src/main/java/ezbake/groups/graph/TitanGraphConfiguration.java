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

package ezbake.groups.graph;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import ezbakehelpers.accumulo.AccumuloHelper;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 6/23/14
 * Time: 12:29 PM
 */
public class TitanGraphConfiguration extends BaseConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanGraphConfiguration.class);
    private static final String SEARCH_INDEX = "search";

    private final Properties ezConfiguration;
    public TitanGraphConfiguration(Properties ezConfiguration) {
        this.ezConfiguration = ezConfiguration;

        // Copy all the titan configurations out of the ezconfiguration
        for (String key : ezConfiguration.stringPropertyNames()) {
            String propValue = ezConfiguration.getProperty(key);
            if (propValue != null && key.startsWith(GraphDatabaseConfiguration.STORAGE_NAMESPACE)) {
                setProperty(key, propValue);
            }
        }

        // Update the instance number
        EzBakeApplicationConfigurationHelper appHelper = new EzBakeApplicationConfigurationHelper(ezConfiguration);
        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY
        ), appHelper.getApplicationInstanceNumber());

        // Gather all the accumulo configurations
        AccumuloHelper ah = new AccumuloHelper(ezConfiguration);
        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                AccumuloStoreManager.ACCUMULO_NAMESPACE,
                AccumuloStoreManager.ACCUMULO_INSTANCE_KEY
        ), ah.getAccumuloInstance());

        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.HOSTNAME_KEY
        ), ah.getAccumuloZookeepers());

        String tableNameKey = joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                AccumuloStoreManager.TABLE_NAME_KEY);
        setProperty(tableNameKey, joinProperties(ah.getAccumuloNamespace(), getString(tableNameKey)));

        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.AUTH_USERNAME_KEY
        ), ah.getAccumuloUsername());

        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.AUTH_PASSWORD_KEY
        ), ah.getAccumuloPassword());

        // Gather elasticsearch properties
        ElasticsearchConfigurationHelper eh = new ElasticsearchConfigurationHelper(ezConfiguration);
        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.INDEX_NAMESPACE,
                SEARCH_INDEX,
                ElasticSearchIndex.CLUSTER_NAME_KEY
        ), eh.getElasticsearchClusterName());

        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.INDEX_NAMESPACE,
                SEARCH_INDEX,
                GraphDatabaseConfiguration.HOSTNAME_KEY
        ), eh.getElasticsearchHostWithPort());

    }

    public Properties getEzConfiguration() {
        return ezConfiguration;
    }

    public String getIndex() {
        Configuration indexSubset = subset(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE,
                GraphDatabaseConfiguration.INDEX_NAMESPACE,
                SEARCH_INDEX));

        String index = Titan.Token.STANDARD_INDEX;
        if (!indexSubset.isEmpty()) {
            index = SEARCH_INDEX;
        }
        return index;
    }

    private String joinProperties(String ...properties) {
        return Joiner.on(".").skipNulls().join(properties);
    }
}
