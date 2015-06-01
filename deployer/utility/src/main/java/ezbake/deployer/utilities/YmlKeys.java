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

package ezbake.deployer.utilities;

import com.google.common.collect.Lists;

import java.util.Collections;

public class YmlKeys {
    public interface ManifestKey {
        public String getName();

        public Iterable<ManifestKey> getSubKeys();
    }

    public enum BatchServiceKeys implements ManifestKey {
        JobName("job_name"),
        StartDate("start_date"),
        StartTime("start_time"),
        Repeat("repeat"),
        FlowName("flow_name");

        BatchServiceKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }

    }

    public enum FrackServiceKeys implements ManifestKey {
        PipelineName("pipeline_name");

        FrackServiceKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }

    }

    public enum CustomServiceKeys implements ManifestKey {
        ServiceName("service_name");

        CustomServiceKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }

    }

    public enum ThriftServiceKeys implements ManifestKey {
        ServiceName("service_name");

        ThriftServiceKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }

    }

    public enum WebAppKeys implements ManifestKey {
        ExternalWebUrl("external_web_url"),
        InternalWebUrl("internal_web_url"),
        Hostname("hostname"),
        Timeout("timeout"),
        TimeoutRetries("timeout_retries"),
        StickySession("sticky_session"),
        UploadFileSize("upload_file_size"),
        WebAppName("web_app_name"),
        PreferredContainer("preferred_container"),
        DisableChunkedTransferEncoding("disable_chunked_transfer_encoding"),
        DisableWebsocketSupport("disable_websocket_support");

        WebAppKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }

    }

    public enum DatabaseKeys implements ManifestKey {
        ServiceName("service_name"),
        Database("database");

        DatabaseKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }
    }

    public enum ResourcesKeys implements ManifestKey {
        cpu("cpu"), mem("mem"), disk("disk");

        ResourcesKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }
    }

    public enum ScalingKeys implements ManifestKey {

        numberOfInstances("number_of_instances");

        ScalingKeys(String name) {
            this.name = name;
        }

        String name;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }
    }

    public enum ArtifactKeys implements ManifestKey {
        bin("bin"),
        config("config"),
        purgeable("purgeable"),
        disableSystemLogfile("disable_system_logfile");

        String name;

        ArtifactKeys(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }
    }

    public enum ApplicationKeys implements ManifestKey {
        AppName("name"),;

        String name;

        ApplicationKeys(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return Collections.emptyList();
        }
    }

    public enum RootManifestKeys implements ManifestKey {
        language("language"),
        applicationName("application_name"),
        serviceName("service_name"),
        securityId("security_id"),
        resources("resources", ResourcesKeys.values()),
        scaling("scaling", ScalingKeys.values()),
        datasets("datasets"),
        type("type", ArtifactKeys.values()),
        auths("auths"),
        artifactInfo("artifact_info");

        RootManifestKeys(String name) {
            this(name, Collections.<ManifestKey>emptyList());
        }

        RootManifestKeys(String name, ManifestKey... values) {
            this(name, Lists.newArrayList(values));
        }

        RootManifestKeys(String name, Iterable<ManifestKey> subKeys) {
            this.name = name;
            this.subKeys = subKeys;
        }

        String name;
        Iterable<ManifestKey> subKeys;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Iterable<ManifestKey> getSubKeys() {
            return subKeys;
        }
    }

}
