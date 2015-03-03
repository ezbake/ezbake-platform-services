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
import ezbake.deployer.ArtifactWriter;
import ezbake.deployer.impl.HdfsArtifactWriter;
import ezbake.deployer.publishers.EzAzkabanPublisher;
import ezbake.deployer.publishers.EzDataSetPublisher;
import ezbake.deployer.publishers.EzFrackPublisher;
import ezbake.deployer.publishers.EzOpenShiftPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.deployer.publishers.EzSecurityRegistrationClient;
import ezbake.deployer.utilities.SSLCertsService;
import ezbakehelpers.hdfs.HDFSHelper;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class OpenShiftDeployerModule extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(OpenShiftDeployerModule.class);

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
