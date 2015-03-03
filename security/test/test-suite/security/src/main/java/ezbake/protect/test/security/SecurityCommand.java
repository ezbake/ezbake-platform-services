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

package ezbake.protect.test.security;

import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.utils.EzSSL;
import ezbake.protect.test.EzSecurityClientHelpers;
import ezbake.security.test.suite.common.Command;
import ezbake.security.thrift.EzSecurity;
import ezbake.security.thrift.ezsecurityConstants;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.kohsuke.args4j.Option;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/8/14
 * Time: 12:07 PM
 */
public class SecurityCommand extends Command{
    public enum Actions {
        JSON,
        PRINCIPAL,
        APP_TOKEN,
        USER_TOKEN
    }

    @Option(name="-a", aliases="--action")
    public Actions action = Actions.USER_TOKEN;

    @Option(name="-ps")
    public String ps;

    public SecurityCommand() {}

    public SecurityCommand(Properties configuration) {
        super(configuration);
    }

    @Override
    public void runCommand() {
        EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(configuration);

        // Initialize the client pool
        ThriftClientPool clientPool = new ThriftClientPool(configuration);

        EzSecurity.Client client = null;
        try {
            client = clientPool.getClient(ezsecurityConstants.SERVICE_NAME, EzSecurity.Client.class);

            // Initialize the efe crypto
            Properties efeConfig = new Properties();
            efeConfig.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, efePkiDir);
            PKeyCrypto efeCrypto = EzSSL.getCrypto(efeConfig);

            // Initialize my crypto
            PKeyCrypto myCrypto = EzSSL.getCrypto(configuration);

            // Make the request
            ProxyPrincipal principal = EzSecurityClientHelpers.getPrincipalToken(client, efeCrypto, user);

            switch(action) {
                case PRINCIPAL:
                    System.out.println(principal);
                    break;
                case JSON:
                    // Fetch and print the json
                    EzSecurityTokenJson json = EzSecurityClientHelpers.getJsonToken(client, efeCrypto, user);
                    System.out.println(json);
                    break;
                case USER_TOKEN:
                    // Make the request
                    EzSecurityToken userToken = EzSecurityClientHelpers.getUserToken(client, myCrypto, principal, appConfig.getSecurityID());
                    System.out.println(userToken);
                    break;
                case APP_TOKEN:
                    // Make the request
                    EzSecurityToken appToken = EzSecurityClientHelpers.getAppToken(client, myCrypto, appConfig.getSecurityID());
                    System.out.println(appToken);
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
            clientPool.close();
        }




    }
}
