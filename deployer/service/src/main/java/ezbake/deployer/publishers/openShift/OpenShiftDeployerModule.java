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

package ezbake.deployer.publishers.openShift;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import ezbake.deployer.ArtifactWriter;
import ezbake.deployer.impl.HdfsArtifactWriter;
import ezbake.deployer.publishers.EzAzkabanPublisher;
import ezbake.deployer.publishers.EzDataSetPublisher;
import ezbake.deployer.publishers.EzFrackPublisher;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.deployer.publishers.EzSecurityRegistrationClient;
import ezbake.deployer.publishers.artifact.*;
import ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector;
import ezbake.deployer.publishers.openShift.inject.CronFileInjector;
import ezbake.deployer.publishers.openShift.inject.DjangoActionHookInjector;
import ezbake.deployer.publishers.openShift.inject.ExtraFilesInjector;
import ezbake.deployer.publishers.openShift.inject.SecurityActionHookInjector;
import ezbake.deployer.publishers.openShift.inject.ThriftRunnerInjector;
import ezbake.deployer.utilities.ArtifactTypeKey;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.Language;
import ezbakehelpers.hdfs.HDFSHelper;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class OpenShiftDeployerModule extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(OpenShiftDeployerModule.class);
    public static final String OPENSHIFT_CONTENTS_PUBLISHER_MAP = "OPENSHIFT_CONTENTS_PUBLISHER";

    protected void configure() {
        bind(SSLCertsService.class).to(EzSecurityRegistrationClient.class);
        bind(EzOpenShiftPublisher.class).in(Singleton.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Thrift.class).to(EzOpenShiftPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.WebApp.class).to(EzOpenShiftPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.DataSet.class).to(EzDataSetPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Custom.class).to(EzOpenShiftPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Frack.class).to(EzFrackPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Batch.class).to(EzAzkabanPublisher.class);
        bind(ArtifactWriter.class).to(HdfsArtifactWriter.class);

        // Bindings for artifact publishing
        MapBinder<ArtifactTypeKey, ArtifactResourceInjector> artifactPublisher = MapBinder
                .newMapBinder(binder(), ArtifactTypeKey.class, ArtifactResourceInjector.class)
                .permitDuplicates();

        for (ArtifactTypeKey artifactType : ArtifactTypeKey.getPermutations()) {
            // Add common bindings for all languages
            artifactPublisher.addBinding(artifactType).to(SecurityActionHookInjector.class);
            artifactPublisher.addBinding(artifactType).to(CronFileInjector.class);
            artifactPublisher.addBinding(artifactType).to(ExtraFilesInjector.class);

            // Add bindings for Java WebApps
            if ((artifactType.getLanguage() == Language.Java) &&
                    (artifactType.getType() == ArtifactType.Thrift || artifactType.getType() == ArtifactType.DataSet)) {
                artifactPublisher.addBinding(artifactType).to(ThriftRunnerInjector.class);
            } else if (artifactType.getLanguage() == Language.Python) {
                // Python webapps get the django action hooks
                if (artifactType.getType() == ArtifactType.WebApp) {
                    artifactPublisher.addBinding(artifactType).to(DjangoActionHookInjector.class);
                }
            }
        }

        // Bindings for artifact publishing
        MapBinder<ArtifactTypeKey, ArtifactContentsPublisher> contentsPublisher = MapBinder
                .newMapBinder(
                        binder(), ArtifactTypeKey.class, ArtifactContentsPublisher.class,
                        Names.named(OPENSHIFT_CONTENTS_PUBLISHER_MAP))
                .permitDuplicates();
        for (ArtifactTypeKey artifactType : ArtifactTypeKey.getPermutations()) {
            // Add bindings for Java WebApps
            if ((artifactType.getLanguage() == Language.Java) &&
                    (artifactType.getType() == ArtifactType.WebApp)) {
                contentsPublisher.addBinding(artifactType).to(JavaWebAppArtifactContentsPublisher.class);
            }

            // Add bindings for all Python Apps
            if (artifactType.getLanguage() == Language.Python) {
                contentsPublisher.addBinding(artifactType).to(PythonRequirementsPublisher.class);

                // Add bindings for Python Web Apps
                if ((artifactType.getLanguage() == Language.Python) &&
                        (artifactType.getType() == ArtifactType.WebApp)) {
                    contentsPublisher.addBinding(artifactType).to(PythonAppsArtifactContentsPublisher.class);
                }
            }
        }
    }

    @Provides
    @Singleton
    FileSystem provideFileSystem(Properties configuration) {
        try {
            return HDFSHelper.getFileSystemFromProperties(configuration);
        } catch (IOException ex) {
            logger.error("Failed to get HDFS File System", ex);
            // Providers shouldn't throw exceptions
            return null;
        }
    }
}
