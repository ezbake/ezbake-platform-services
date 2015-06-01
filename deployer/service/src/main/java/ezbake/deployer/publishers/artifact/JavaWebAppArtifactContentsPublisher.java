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

public class JavaWebAppArtifactContentsPublisher extends ArtifactContentsPublisher {

    public final static String WEBAPP_FILES_CLASSPATH = "ezdeploy.openshift.webapp";
    public final static String WEBAPP_CONFIG_FILES_CLASSPATH = WEBAPP_FILES_CLASSPATH + ".config";
    public final static String WEBAPP_ACTION_HOOKS_FILES_CLASSPATH = WEBAPP_FILES_CLASSPATH + ".action_hooks";
    public final static String WEBAPP_LOGBACK_FILE_CLASSPATH = WEBAPP_FILES_CLASSPATH + ".logback";

    public final static Path DEPLOY_OPENSHIFT_CONFIG_FILES_BASEPATH = Paths.get(".openshift", "config");
    public final static Path DEPLOY_OPENSHIFT_ACTION_HOOK_BASEPATH = Paths.get(".openshift", "action_hooks");
    public final static Path DEPLOY_CONFIG_FILES_BASEPATH = Paths.get("config");

    /**
     * Process generated Archive entries for the deployment artifact
     *
     * @param artifact artifact to be processed
     * @return a list of tar archive entries for the artifact
     */
    @Override
    public Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException {
        List<ArtifactDataEntry> entries = Lists.newArrayList();

        //add openshift config files (standalone.xml, etc) if not already present in artifact
        entries.addAll(collectConfigurationResources(artifact,
                WEBAPP_CONFIG_FILES_CLASSPATH, DEPLOY_OPENSHIFT_CONFIG_FILES_BASEPATH,
                new ArtifactDataEntryResourceCreator()));

        //add (force overwrite) openshift action_hooks - would overwrite if present in artifact
        entries.addAll(collectConfigurationResources(artifact,
                WEBAPP_ACTION_HOOKS_FILES_CLASSPATH, DEPLOY_OPENSHIFT_ACTION_HOOK_BASEPATH,
                new ArtifactDataEntryResourceCreator(
                        ArtifactDataEntryResourceCreator.DEFAULT_EXECUTABLE_FILE_PERMISSIONS), true));

        //Unless specifically directed not to in the manifest, we'll add our own logback xml
        if (!artifact.getMetadata().getManifest().getArtifactInfo().isSystemLogfileDisabled()) {
            entries.addAll(collectConfigurationResources(artifact,
                    WEBAPP_LOGBACK_FILE_CLASSPATH, DEPLOY_CONFIG_FILES_BASEPATH,
                    new ArtifactDataEntryResourceCreator(), true));
        }

        //Force the JBOSS cartridge to use JAVA 7
        entries.add(new ArtifactDataEntryResourceCreator().createEmptyArtifact(".openshift/markers/java7"));

        return entries;
    }
}
