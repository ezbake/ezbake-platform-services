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
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.Utilities;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.postgres.PostgresConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class to setup the schema and role for an application using PostgreSQL
 */
public class PostgresDatabaseSetup implements DatabaseSetup {

    public static final String DB_PROPERTIES = "postgresdb.properties";
    public static final String ENCRYPTED_DB_PROPERTIES = "encrypted_postgresdb.properties";
    private static final String DBA_ROLE = "deployerdba";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatabaseSetup.class);

    private final SecureRandom random = new SecureRandom();
    private PostgresConfigurationHelper postgresConfiguration;

    @Override
    public List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact,
                                             Properties configuration, EzSecurityToken callerToken) throws DeploymentException {
        postgresConfiguration = new PostgresConfigurationHelper(configuration,
                new SystemConfigurationHelper(configuration).getTextCryptoProvider());
        //Setup new sql properties
        String host = postgresConfiguration.getHost();
        String port = postgresConfiguration.getPort();
        String adminDatabase = postgresConfiguration.getDatabase();
        String databaseName = ArtifactHelpers.getNamespace(artifact).toLowerCase();
        String userName = databaseName + "_user";
        String password = new BigInteger(130, random).toString(32);
        String useSSL = postgresConfiguration.useSSL() ? "?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory" : "";
        String baseConnectionString = "jdbc:postgresql://" + host + ":" + port + "/";

        setupDatabase(baseConnectionString + adminDatabase + useSSL, userName, password, databaseName);
        loadExtensions(baseConnectionString + databaseName + useSSL, userName, databaseName);
        revokeSuperUser(baseConnectionString + databaseName + useSSL, userName, databaseName);

        Map<String, String> map = Maps.newHashMap();
        map.put(EzBakePropertyConstants.POSTGRES_DB, databaseName);
        map.put(EzBakePropertyConstants.POSTGRES_HOST, host);
        map.put(EzBakePropertyConstants.POSTGRES_PORT, port);
        map.put(EzBakePropertyConstants.POSTGRES_USE_SSL, Boolean.toString(postgresConfiguration.useSSL()));

        Map<String, String> encryptedMap = Maps.newHashMap();
        encryptedMap.put(EzBakePropertyConstants.POSTGRES_USERNAME, userName);
        encryptedMap.put(EzBakePropertyConstants.POSTGRES_PASSWORD, password);

        String properties = Joiner.on('\n').withKeyValueSeparator("=").join(map);
        String encryptedProps = Joiner.on('\n').withKeyValueSeparator("=").join(encryptedMap);
        List<ArtifactDataEntry> entries = Lists.newArrayList();
        entries.add(Utilities.createConfCertDataEntry(ENCRYPTED_DB_PROPERTIES, encryptedProps.getBytes()));
        entries.add(Utilities.createConfCertDataEntry(DB_PROPERTIES, properties.getBytes()));
        return entries;
    }

    @Override
    public boolean canSetupDatabase(DeploymentArtifact artifact) {
        return artifact.getMetadata().getManifest().getDatabaseInfo().getDatabaseType().equalsIgnoreCase("PostgreSQL");
    }

    /**
     * Creates a role with the provided username and password.
     * Creates a database with the provided database name and sets the provided
     * user as its owner
     *
     * @param url          JDBC connection url to use to connect
     * @param username     username to use for creating the role
     * @param password     for accessing this data base
     * @param databaseName name of the database provided in configurations.
     * @throws DeploymentException thrown if connection to database server fails
     */
    private void setupDatabase(String url, String username, String password, String databaseName)
            throws DeploymentException {
        LOGGER.info("Try to connect to {} @ {}", databaseName, url);
        Connection connection = null;
        try {
            // Here we connect with the admin's username and password to create the user/db
            connection = connect(
                    url,
                    postgresConfiguration.getUsername(),
                    postgresConfiguration.getPassword());
            // Check if each of these exist before creating them
            boolean userExists = checkExists(connection, "SELECT usename FROM pg_catalog.pg_user WHERE usename = \'" +
                    username + "\';");
            boolean databaseExists = checkExists(connection, "SELECT datname FROM pg_database WHERE " +
                    "datname=\'" + databaseName + "\';");
            boolean dbaRoleExists = checkExists(connection, "SELECT rolname FROM pg_roles WHERE rolname = \'" +
                    DBA_ROLE + "\';");

            List<String> commands = new ArrayList<>();
            if (!userExists) {
                LOGGER.info("User {} doesn't exist. Creating..", username);
                commands.add("CREATE USER " + username + " WITH PASSWORD \'" + password + "\'");
            } else {
                // Just change the password if the user already exists
                LOGGER.info("User {} exists, updating password", username);
                commands.add("ALTER ROLE " + username + " WITH PASSWORD \'" + password + "\'");
            }

            if (!databaseExists) {
                LOGGER.info("Database {} doesn't exist, creating...", databaseName);
                commands.add("CREATE DATABASE " + databaseName + " OWNER " + username);
            } else {
                LOGGER.info("Database {} exists, nothing to do.", databaseName);
            }

            if (!dbaRoleExists) {
                LOGGER.info("Creating {} role", DBA_ROLE);
                commands.add("CREATE ROLE " + DBA_ROLE + " WITH SUPERUSER NOINHERIT");
            }
            runStatements(connection, commands.toArray(new String[commands.size()]));

        } catch (SQLException e) {
            LOGGER.error("Error executing db configuration", e);
            throw new DeploymentException("Error executing db configuration sql: " + e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("error closing connection", e);
            }
        }
    }

    /**
     * Loads all the extensions used by postgres into the new database.
     *
     * @param url    The JDBC connection url to connect to postgres
     * @param schema The default schema for this database
     * @throws DeploymentException thrown if connection to database server fails
     */
    private void loadExtensions(String url, String username, String schema) throws DeploymentException {
        Connection connection = null;
        try {
            // Connect with the admin's username and password
            connection = connect(url, postgresConfiguration.getUsername(),
                    postgresConfiguration.getPassword());
            runStatements(connection,
                    "CREATE SCHEMA IF NOT EXISTS " + schema + " AUTHORIZATION " + username,
                    "ALTER ROLE " + username + " RESET search_path",
                    "ALTER ROLE " + username + " SET search_path = " + schema + ",public",
                    "GRANT " + DBA_ROLE + " TO " + username,
                    "ALTER DATABASE " + schema + " SET search_path TO '" + schema + ",public'",
                    "REVOKE ALL PRIVILEGES ON DATABASE " + schema + " FROM PUBLIC CASCADE",
                    "REVOKE ALL PRIVILEGES ON SCHEMA " + schema + " FROM PUBLIC CASCADE",
                    "GRANT ALL PRIVILEGES ON DATABASE " + schema + " TO " + username,
                    "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + username,
                    "GRANT ALL PRIVILEGES ON SCHEMA public TO " + username,
                    "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public",
                    "CREATE EXTENSION IF NOT EXISTS postgis_topology",
                    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA public",
                    "CREATE EXTENSION IF NOT EXISTS ezbake_visibility SCHEMA public"
            );
        } catch (SQLException e) {
            LOGGER.error("Error creating extensions/functions.", e);
            throw new DeploymentException("Unable to create database extensions/functions " + e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("error closing connection", e);
            }
        }
    }

    /**
     * Loads all the extensions used by postgres into the new database.
     *
     * @param url    The JDBC connection url to connect to postgres
     * @param schema The default schema for this database
     * @throws DeploymentException thrown if connection to database server fails
     */
    private void revokeSuperUser(String url, String username, String schema) throws DeploymentException {
        Connection connection = null;
        try {
            // Connect with the admin's username and password
            connection = connect(url, postgresConfiguration.getUsername(),
                    postgresConfiguration.getPassword());
            runStatements(connection, "REVOKE " + DBA_ROLE + " FROM " + username,
                    "REVOKE ALL PRIVILEGES ON SCHEMA public FROM " + username,
                    "GRANT USAGE ON SCHEMA public TO " + username);
        } catch (SQLException e) {
            LOGGER.error("Error revoking superuser from new user.", e);
            throw new DeploymentException("Unable to create database extensions/functions " + e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("error closing connection.", e);
            }
        }
    }

    /**
     * Attempts to connect with the postgres server
     * - Tests the driver
     * - Tests and initializes the connection
     *
     * @throws DeploymentException
     */
    public Connection connect(String url, String username, String password) throws DeploymentException {
        LOGGER.info("PostgreSQL JDBC Connecting to {} as user {}", url, username);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            throw new DeploymentException("Driver not found : " + e.getMessage());
        }
        LOGGER.info("PostgreSQL JDBC Driver Registered!");

        Connection connection;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            LOGGER.error("Connection Failed! ", e);
            throw new DeploymentException("Connection Failed! " + e.getMessage());
        }
        return connection;
    }

    private void runStatements(Connection connection, String... statements) throws SQLException {
        String sql = Joiner.on(";").join(statements) + ";";
        LOGGER.debug("Running {}", sql);
        Statement st = connection.createStatement();
        st.execute(sql);
    }

    private boolean checkExists(Connection connection, String sqlStatement) {
        Statement st = null;
        try {
            st = connection.createStatement();
            ResultSet rs = st.executeQuery(sqlStatement);
            return rs.isBeforeFirst();
        } catch (SQLException e) {
            LOGGER.error("Error checking if user or database existed: " + e);
            return false;
        }
    }
}
