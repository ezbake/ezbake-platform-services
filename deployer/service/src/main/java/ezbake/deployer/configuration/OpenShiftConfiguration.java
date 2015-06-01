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

package ezbake.deployer.configuration;

import java.util.Properties;

public class OpenShiftConfiguration {
    /**
     * These environmental variables are provided by the OpenShift PaaS.
     * The Thriftrunner ones, are only available for ThriftRunner applications
     */
    public enum EnvVariables {
        OPENSHIFT_APP_DNS,
        OPENSHIFT_APP_NAME,
        OPENSHIFT_APP_UUID,
        OPENSHIFT_DATA_DIR,
        OPENSHIFT_GEAR_DNS,
        OPENSHIFT_GEAR_NAME,
        OPENSHIFT_GEAR_UUID,
        OPENSHIFT_HOMEDIR,
        OPENSHIFT_REPO_DIR,
        OPENSHIFT_TMP_DIR,
        OPENSHIFT_JAVA_THRIFTRUNNER_IP,
        OPENSHIFT_JAVA_THRIFTRUNNER_TCP_PORT,
        OPENSHIFT_JAVA_THRIFTRUNNER_TCP_PROXY_PORT,;

        public String getEnvName() {
            return toString();
        }

        public String getEnvValue() {
            return System.getenv(getEnvName());
        }

        public String getEnvValue(String defaultValue) {
            String val = getEnvValue();
            if (val == null) return defaultValue;
            return val;
        }
    }


    public OpenShiftConfiguration(Properties properties) {

    }
}

