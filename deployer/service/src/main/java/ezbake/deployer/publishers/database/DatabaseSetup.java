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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;

import java.util.List;
import java.util.Properties;

/**
 * Interface that all classes that are responsible for configuring a database should implement
 */
public interface DatabaseSetup {
    /**
     * Setup anything in the database that is needed for this deployment.  Things like database users, tables, etc.
     *
     * @param artifact      The deployment artifact
     * @param configuration Properties object to get database connection information
     * @param callerToken   EzSecurityToken from the caller that initiated this publish event
     * @return A CertDataEntry for the database properties file for connection information specific for this application
     */
    List<ArtifactDataEntry> setupDatabase(DeploymentArtifact artifact, Properties configuration, EzSecurityToken callerToken) throws DeploymentException;

    /**
     * Determines whether this object can configure this database
     *
     * @param artifact The deployment artifact
     * @return True if this instance can setup the database, otherwise False
     */
    boolean canSetupDatabase(DeploymentArtifact artifact);
}
