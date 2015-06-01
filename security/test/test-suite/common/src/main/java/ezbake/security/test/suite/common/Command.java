/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.security.test.suite.common;

import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.utils.EzSSL;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/8/14
 * Time: 12:05 PM
 */
public abstract class Command {

    @Option(name="-p", aliases="--efe-pki", required=true, usage="directory containing the EFE PKI certs. Needed to get an EzSecurityToken")
    public String efePkiDir;

    @Option(name="-m", aliases="--my-pki", usage="directory containing the application's PKI certs. Needed to get an EzSecurityToken")
    public String myPkiDir;

    @Option(name="-u", aliases="--user", required=true, usage="subject of the user to act as")
    public String user;

    protected Properties configuration;

    public Command() { }

    public Command(Properties properties) {
        this.configuration = properties;
    }

    public abstract void runCommand();

    public void setConfigurationProperties(Properties configurationProperties) {
        this.configuration = configurationProperties;
    }

    public PKeyCrypto getEfeCrypto() throws IOException {
        Properties efeConfig = new Properties();
        efeConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, efePkiDir);
        return EzSSL.getCrypto(efeConfig);
    }
}
