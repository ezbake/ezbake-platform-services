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

package ezbake.security.testsuite.registration;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.ProxyPrincipal;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.protect.test.EzSecurityClientHelpers;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.test.suite.common.Command;
import ezbake.security.thrift.*;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/8/14
 * Time: 12:54 PM
 */
public class RegistrationCommand extends Command {
    private static final Logger logger = LoggerFactory.getLogger(RegistrationCommand.class);

    enum ACTION {
        REGISTER, UPDATE, PROMOTE
    }

    @Option(name="-a", aliases="--action")
    public ACTION action = ACTION.REGISTER;

    @Option(name="-i", aliases="--id", usage="Security ID of application to register/update")
    public String id;

    private ThriftClientPool clientPool;

    public RegistrationCommand() {}

    public RegistrationCommand(Properties configuration) {
        super(configuration);
    }

    @Override
    public void runCommand() {
        // Initialize the client pool
        if (clientPool == null) {
            clientPool = new ThriftClientPool(configuration);
        }

        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(configuration);
        try {
            // Initialize the efe crypto
            PKeyCrypto efeCrypto = getEfeCrypto();

            // Get Registration's security id
            String registrationSecurityId = clientPool.getSecurityId(EzSecurityRegistrationConstants.SERVICE_NAME);

            // First get the signed principal token from EzSecurity
            ProxyPrincipal ep;
            EzSecurity.Client principalClient = clientPool.getClient(ezsecurityConstants.SERVICE_NAME, EzSecurity.Client.class);
            try {
                ep = EzSecurityClientHelpers.getPrincipalToken(principalClient, efeCrypto, user);
            } finally {
                clientPool.returnToPool(principalClient);
            }

            // Now get an EzSecurityToken for calling out to the registration service
            EzSecurityToken regToken = securityClient.fetchTokenForProxiedUser(ep, registrationSecurityId);

            String appId = id;
            if (appId == null || appId.isEmpty()) {
                // Now register an application
                appId = registerApplication(regToken, "Initial test app", "U", Collections.<String>emptyList());
            }

            // Now promote it
            promoteApplication(regToken, appId);

            /* Best way to determine that the private key is correct, is to encrypt something with it */
            AppCerts ac = this.getAppCerts(regToken, appId);
            byte[] priv = ac.getApplication_priv();
            byte[] pub = ac.getApplication_pub();

            logger.debug("Data of Application Private Key From App Cert {}", new String(priv));

            RSAKeyCrypto crypto = new RSAKeyCrypto(new String(priv), new String(pub));
            byte[] cipherTxt = crypto.encrypt("Test Data".getBytes());
            byte[] uncipherTxt = crypto.decrypt(cipherTxt);

            if(!"Test Data".equals(new String(uncipherTxt))) {
                logger.error("There seems to be an invalid private key");
            }

        } catch (Exception e) {
            logger.error("Caught Exception with cause {} and message {} with exception {}", e.getCause(), e.getMessage(), e);
        } finally {
            try {
                securityClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    private void promoteApplication(EzSecurityToken token, String id) throws TException {
        EzSecurityRegistration.Client client = null;
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            client.promote(token, id);
        } finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
        }
    }
    private String registerApplication(EzSecurityToken token, String appName, String level,
                                       List<String> authorizations) throws TException {
        EzSecurityRegistration.Client client = null;
        String id = null;
        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            id = client.registerApp(token, appName, level, authorizations, null, null, null, null);
        } finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
        }
        return id;
    }

    private AppCerts getAppCerts(EzSecurityToken ezToken, String id){
        AppCerts cert = null;
        EzSecurityRegistration.Client client = null;

        try {
            client = clientPool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
            cert = client.getAppCerts(ezToken, id);
        } catch (TException e) {
            logger.error("Error {}", e);
        }
        finally {
            if(client != null) {
                clientPool.returnToPool(client);
            }
        }

        return cert;
    }


}
