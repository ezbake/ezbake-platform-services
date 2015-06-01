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

package deployer.publishers.database;

import deployer.TestUtils;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.publishers.database.PostgresDatabaseSetup;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Note: This test will fail if there is no postgres server
 * set up OR if "user1" already exists.
 * This test requires a postgres server running.
 * Modify the host and port accordingly.
 */
public class PostgresDatabaseSetupTest {


    private static final String APP_NAME = "app";
    private static final String DB_TYPE = "Postgres";
    private static final String HOST = "localhost";
    private static final String PORT = "5433";
    private static final String CON_URL = "jdbc:postgresql://" + HOST + ":" + PORT + "/";

    private static Logger log = LoggerFactory.getLogger(PostgresDatabaseSetupTest.class);
    private static final String ADMIN = "postgres";
    private static final String ADMIN_PASSWORD = "postgres";
    private static String username = "user1";
    private static String dbName;
    private static String dbPassword;


    //@Test
    public void testPostgresDBSetup() throws Exception {

        try {
            PostgresDatabaseSetup setup = new PostgresDatabaseSetup();
            DeploymentArtifact artifact = TestUtils.createSampleDeploymentArtifact(ArtifactType.DataSet);
            artifact.getMetadata().getManifest().getApplicationInfo().setApplicationId(APP_NAME);
            artifact.getMetadata().getManifest().getDatabaseInfo().setDatabaseType(DB_TYPE);

            //Configuration files
            Properties configuration = new Properties();
            configuration.setProperty(EzBakePropertyConstants.POSTGRES_USERNAME, ADMIN);
            configuration.setProperty(EzBakePropertyConstants.POSTGRES_PASSWORD, ADMIN_PASSWORD);
            configuration.setProperty(EzBakePropertyConstants.POSTGRES_HOST, HOST);
            configuration.setProperty(EzBakePropertyConstants.POSTGRES_PORT, PORT);

            List<ArtifactDataEntry> entries = setup.setupDatabase(artifact, configuration, TestUtils.getTestEzSecurityToken());
            testProperties(entries, artifact);
            testUserSpace();
        } finally {
            cleanup();
        }


    }

    /**
     * Attempts to connect as the created
     * user and tests the database
     */
    private void testUserSpace() throws SQLException {
        Connection con = getConnection(CON_URL, username, dbPassword, dbName);
        //Connection was successful (user space was created)
        assertNotNull(con);
        testDB(con);
        con.close();
    }

    //Verify that user has access to the database
    private void testDB(Connection con) throws SQLException {
        String query = "CREATE TABLE testTable "
                + "(id integer, name varchar(40))";
        String insert = "INSERT INTO testTable VALUES (24, 'hello world')";
        String select = "SELECT * from testTable";
        String drop = "DROP TABLE IF EXISTS testTable";
        PreparedStatement st = null;
        try {

            //Create a table
            st = con.prepareStatement(query);
            st.execute();

            //Insert some values
            st = con.prepareStatement(insert);
            st.execute();

            st = con.prepareStatement(select);
            ResultSet rs = st.executeQuery();

            //Check that values were successfully inserted
            while (rs.next()) {
                assertTrue(rs.getInt(1) == 24);
                assertEquals(rs.getString(2), "hello world");
            }

        } finally {
            //Clean up
            st = con.prepareStatement(drop);
            st.execute();
            con.close();
        }
    }

    //Verify that the properties were correctly mapped
    private void testProperties(List<ArtifactDataEntry> entries, DeploymentArtifact artifact) throws Exception {
        Properties properties = new Properties();
        for (ArtifactDataEntry entry : entries) {
            properties.load(new ByteArrayInputStream(entry.getData()));
        }

        dbName = ArtifactHelpers.getAppId(artifact);
        dbPassword = properties.getProperty(EzBakePropertyConstants.POSTGRES_PASSWORD);
        username = dbName + "_user";

        assertEquals(properties.getProperty(EzBakePropertyConstants.POSTGRES_USERNAME), username);
        assertNotNull(properties.getProperty(EzBakePropertyConstants.POSTGRES_PASSWORD));
        assertEquals(properties.getProperty(EzBakePropertyConstants.POSTGRES_HOST), HOST);
        assertEquals(properties.getProperty(EzBakePropertyConstants.POSTGRES_PORT), PORT);
        assertEquals(properties.getProperty(EzBakePropertyConstants.POSTGRES_DB), dbName);

    }

    private Connection getConnection(String conurl, String username, String password, String dbname) {
        Connection connection = null;
        String url = conurl + dbname;
        System.out.println(url);
        try {
            connection = DriverManager.getConnection(
                    url, username, password);
        } catch (SQLException e) {
            log.error(" Connection failed ", e);
        }
        return connection;
    }

    private void cleanup() throws SQLException {
        String dropDB = "DROP DATABASE " + dbName;
        String dropRole = "DROP USER " + username;
        Connection con = getConnection(CON_URL, ADMIN, ADMIN_PASSWORD, "");

        //Successfully connected as admin
        assertNotNull(con);
        Statement st = con.createStatement();
        st.execute(dropDB);
        st.execute(dropRole);
        con.close();
    }
}
