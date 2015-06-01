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

package ezbake.security.test.suite.app;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.ProxyPrincipal;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.protect.test.EzSecurityClientHelpers;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityClient;
import ezbake.security.test.suite.common.Command;
import ezbake.security.thrift.EzSecurity;
import ezbake.security.thrift.ezsecurityConstants;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 10/8/14
 * Time: 12:26 PM
 */
public class PerformanceTestCommand extends Command {

    @Option(name="-n", usage="number of requests to make")
    public int number = 10;

    @Option(name="-c", aliases="--clear-cache", usage="request that the cache be cleared before making requests")
    public boolean clear = false;

    public PerformanceTestCommand() {}

    public PerformanceTestCommand(Properties properties) {
        super(properties);
    }

    @Override
    public void runCommand() {
        ThriftClientPool clientPool = new ThriftClientPool(configuration);
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(configuration, clientPool);


        EzSecurity.Client client = null;
        try {
            PKeyCrypto efeCrypto = getEfeCrypto();
            client = clientPool.getClient(ezsecurityConstants.SERVICE_NAME, EzSecurity.Client.class);

            // Get a token for the user performing this action
            ProxyPrincipal userPrincipal = EzSecurityClientHelpers.getPrincipalToken(client, efeCrypto, user);
            EzSecurityToken userToken = securityClient.fetchTokenForProxiedUser(userPrincipal, null);

            if (clear) {
                client.invalidateCache(userToken);
            }


            for (int i = 0; i < number; ++i) {
                long startAll = System.currentTimeMillis();

                ProxyPrincipal principal = EzSecurityClientHelpers.getPrincipalToken(client, efeCrypto, user);
                long endPrincipalToken = System.currentTimeMillis();

                long startSecurityToken = System.currentTimeMillis();
                EzSecurityToken token = securityClient.fetchTokenForProxiedUser(principal, null, true);
                long endSecurityToken = System.currentTimeMillis();

                System.out.println((i+1) + ". Total time: " + computeSeconds(startAll, endSecurityToken) +
                        ", Principal Token: " + computeSeconds(startAll, endPrincipalToken) +
                        ", Security Token: " + computeSeconds(startSecurityToken, endSecurityToken));
            }

        } catch (TException | IOException | PKeyCryptoException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                clientPool.returnToPool(client);
            }
            clientPool.close();
        }
    }

    private float computeSeconds(long startMillis, long endMillis) {
        return (endMillis - startMillis) / 1000f;
    }
}
