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


package ezbake.data.common.graph;


import com.google.common.base.Joiner;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;


public final class TitanGraphConfiguration extends BaseConfiguration {
    private final Properties properties;

    public TitanGraphConfiguration(Properties props) {
        properties = (Properties) props.clone();

        for (final String key : properties.stringPropertyNames()) {
            // add only properties started with "storage"
            if (key.startsWith(GraphDatabaseConfiguration.STORAGE_NAMESPACE)) {
                setProperty(key, properties.getProperty(key));
            }

        }

    }

    private static String joinProperties(String... props) {
        return Joiner.on(".").skipNulls().join(props);
    }

    public void setTitanAccumuloProperties() {
        final AccumuloHelper ah = new AccumuloHelper(this.properties);

        // storage.accumulo.instance
        setProperty(joinProperties(
                GraphDatabaseConfiguration.STORAGE_NAMESPACE, AccumuloStoreManager.ACCUMULO_NAMESPACE,
                AccumuloStoreManager.ACCUMULO_INSTANCE_KEY), ah.getAccumuloInstance());

        // storage.hostname
        setProperty(
                joinProperties(
                        GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.HOSTNAME_KEY),
                ah.getAccumuloZookeepers());

        // storage.tablename
        final String tableNameKey =
                joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE, AccumuloStoreManager.TABLE_NAME_KEY);
        final String tableName = getString(tableNameKey);

        if (StringUtils.isNotEmpty(ah.getAccumuloNamespace()) && StringUtils.isNotEmpty(tableName)) {
            setProperty(tableNameKey, joinProperties(ah.getAccumuloNamespace(), tableName));
        }

        // storage.username
        setProperty(joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.AUTH_USERNAME_KEY),
                ah.getAccumuloUsername());
        // storage.password
        setProperty(joinProperties(
                        GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.AUTH_PASSWORD_KEY),
                ah.getAccumuloPassword());
    }


    public Properties getProperties() {

        return properties;

    }

}