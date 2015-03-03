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

package ezbake.deployer.publishers.ezcentos;

import com.google.inject.AbstractModule;
import ezbake.deployer.ArtifactWriter;
import ezbake.deployer.impl.LocalFileArtifactWriter;
import ezbake.deployer.publishers.EzAzkabanPublisher;
import ezbake.deployer.publishers.EzPublisher;
import ezbake.deployer.publishers.EzPublisherMapping;
import ezbake.deployer.publishers.local.LocalFrackSubmitterPublisher;
import ezbake.deployer.publishers.local.NullCertsService;
import ezbake.deployer.utilities.SSLCertsService;

public class EzCentosDeployerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SSLCertsService.class).to(NullCertsService.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Thrift.class).to(EzCentosThriftRunnerPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.WebApp.class).to(EzCentosWebAppPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.DataSet.class).to(EzCentosThriftRunnerPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Custom.class).to(EzCentosThriftRunnerPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Frack.class).to(LocalFrackSubmitterPublisher.class);
        bind(EzPublisher.class).annotatedWith(EzPublisherMapping.Batch.class).to(EzAzkabanPublisher.class);


        bind(ArtifactWriter.class).to(LocalFileArtifactWriter.class);
    }
}
