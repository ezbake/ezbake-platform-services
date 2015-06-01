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

package ezbake.deployer.publishers.artifact;

import com.google.common.collect.Lists;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import static ezbake.deployer.utilities.Utilities.getResourcesFromClassPath;

public class PythonAppsArtifactContentsPublisher extends ArtifactContentsPublisher {

    // resource path
    public final static String WSGI_FILES_CLASSPATH = "ezdeploy.openshift.django.wsgi";

    // final artifact path
    public final static Path DEPLOY_OPENSHIFT_WSGI_BASEPATH = Paths.get(".");

    /**
     * Process generated Archive entries for the deployment artifact
     *
     * @param artifact artifact to be processed
     * @return a list of tar archive entries for the artifact
     */
    @Override
    public Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException {
        /* find list of files from from git repo
           for each dep, do the most appropriate thing for that type
           return null; */
        List<ArtifactDataEntry> entries = Lists.newArrayList();

        /* add the wsgi file
           note: we don't want to add it if it already exists */
        entries.addAll(collectConfigurationResources(artifact,
                WSGI_FILES_CLASSPATH, DEPLOY_OPENSHIFT_WSGI_BASEPATH,
                new ArtifactDataEntryResourceCreator()));

        return entries;
    }
}
