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

package ezbake.deployer.publishers;

import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Iterator;
import java.util.Map;

/**
 * This provides a mapping between the different PaaS publishers.  This is how EzPublisher dictates which artifact
 * goes to which PaaS.
 */
public class EzPublisherMapping implements Iterable<Map.Entry<ArtifactType, EzPublisher>> {

    private Map<ArtifactType, EzPublisher> mapping;
    private final EzDeployerConfiguration configuration;

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Thrift {
    }

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface DataSet {
    }

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Custom {
    }

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface WebApp {
    }

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Frack {
    }

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Batch {
    }

    @Inject
    public EzPublisherMapping(EzDeployerConfiguration configuration, @Thrift EzPublisher thriftPublisher,
                              @DataSet EzPublisher datasetPublisher, @Custom EzPublisher customPublisher,
                              @WebApp EzPublisher webAppPublisher, @Frack EzPublisher frackPublisher,
                              @Batch EzPublisher batchPublisher) {
        this.configuration = configuration;
        mapping = new ImmutableMap.Builder<ArtifactType, EzPublisher>()
                .put(ArtifactType.Thrift, thriftPublisher)
                .put(ArtifactType.DataSet, datasetPublisher)
                .put(ArtifactType.Custom, customPublisher)
                .put(ArtifactType.WebApp, webAppPublisher)
                .put(ArtifactType.Frack, frackPublisher)
                .put(ArtifactType.Batch, batchPublisher)
                .build();
    }

    public EzPublisher get(ArtifactType artifactType) throws DeploymentException {
        EzPublisher publisher = mapping.get(artifactType);
        if (publisher == null)
            throw new DeploymentException("Unknown artifact type " + artifactType + " to get publisher for.");
        return publisher;
    }

    @Override
    public Iterator<Map.Entry<ArtifactType, EzPublisher>> iterator() {
        return mapping.entrySet().iterator();
    }
}
