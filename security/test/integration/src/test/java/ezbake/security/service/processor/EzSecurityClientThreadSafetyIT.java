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

package ezbake.security.service.processor;

import ezbake.base.thrift.EzSecurityPrincipal;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.ProxyPrincipal;
import ezbake.security.EzSecurityITBase;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.RSAKeyCrypto;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: jhastings
 * Date: 12/27/13
 * Time: 4:58 PM
 */
public class EzSecurityClientThreadSafetyIT  extends EzSecurityITBase {

    @BeforeClass
    public static void setUpCrpto() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        // Set up crypto for signing requests
        serverCrypt = new RSAKeyCrypto(FileUtils.readFileToString(new File(privateKeyPath)), FileUtils.readFileToString(new File(publicKeyPath)));
    }

    @Before
    public void setUpClient() {
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @Test @Ignore
    public void threadedAppInfo() throws InterruptedException {
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 100; ++i) {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        ProxyPrincipal dn = getSignedPrincipal(DN);
                        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
                    } catch (PKeyCryptoException e) {
                        e.printStackTrace();
                    } catch (EzSecurityTokenException e) {
                        e.printStackTrace();
                    } catch (TException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }
            });
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        threads.clear();

        for (int i = 0; i < 100; ++i) {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        ProxyPrincipal dn = getSignedPrincipal(DN);
                        EzSecurityToken info = ezbakeSecurityClient.fetchTokenForProxiedUser(dn, null);
                    } catch (PKeyCryptoException e) {
                        e.printStackTrace();
                    } catch (EzSecurityTokenException e) {
                        e.printStackTrace();
                    } catch (TException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

    }


}
