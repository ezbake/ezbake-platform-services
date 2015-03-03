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

package ezbake.security;

import com.google.common.io.Closeables;
import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.utils.EzSSL;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.thrift.*;
import org.apache.thrift.TException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

/**
 * User: jhastings
 * Date: 12/13/13
 * Time: 3:13 PM
 */
public class EzSecurityMockOFEIT extends EzSecurityITBase {
    private static Logger log = LoggerFactory.getLogger(EzSecurityMockOFEIT.class);


    @BeforeClass
    public static void useOFE() {
    }

    @Before
    public void setUpClient() {
        properties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "EFE");
        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, "src/test/resources/pki/efe");
        ezbakeSecurityClient = new EzbakeSecurityClient(properties);
    }

    @After
    public void closeClient() throws IOException {
        Closeables.close(ezbakeSecurityClient, true);
    }

    @Before
    public void setUpTest() {
    }

    @Test
    public void testUserDn() throws TException, PKeyCryptoException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, IOException {
        PKeyCrypto crypto = EzSSL.getCrypto(properties);

        ProxyTokenRequest req = new ProxyTokenRequest(new X509Info(DN), new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis(), ""));
        req.getValidity().setSignature(EzSecurityTokenUtils.proxyTokenRequestSignature(req, crypto));

        EzSecurity.Client client = ezbakeSecurityClient.getClient();
        try {
            ProxyTokenResponse res = client.requestProxyToken(req);
            Assert.assertNotNull(res);
            ProxyUserToken put = EzSecurityTokenUtils.deserializeProxyUserToken(res.getToken());
            Assert.assertEquals(DN, put.getX509().getSubject());
            ezbakeSecurityClient.verifyProxyUserToken(res.getToken(), res.getSignature());
       } finally {
            ezbakeSecurityClient.returnClient(client);
        }
    }

    @Test
    public void testProxyUserToken() throws TException, PKeyCryptoException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, IOException {
        ProxyPrincipal dn = getSignedPrincipal(DN);

        PKeyCrypto crypto = EzSSL.getCrypto(properties);

        ProxyTokenRequest req = new ProxyTokenRequest(
                new X509Info(DN),
                new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis(), "")
        );
        req.getValidity().setSignature(EzSecurityTokenUtils.proxyTokenRequestSignature(req, crypto));

        EzSecurity.Client client = ezbakeSecurityClient.getClient();
        try {
            ProxyTokenResponse res = client.requestProxyToken(req);
            Assert.assertNotNull(res);
            ezbakeSecurityClient.verifyProxyUserToken(res.getToken(), res.getSignature());

            ProxyUserToken token = EzSecurityTokenUtils.deserializeProxyUserToken(res.getToken());
            Assert.assertEquals(DN, token.getX509().getSubject());

        } catch (AppNotRegisteredException e) {
            e.printStackTrace();
        } finally {
            ezbakeSecurityClient.returnClient(client);
        }
    }

    @Test
    public void testUserJson() throws IOException, PKeyCryptoException, TException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        testUserDn();

        PKeyCrypto crypto = EzSSL.getCrypto(properties);

        TokenRequest req = new TokenRequest("10000000", System.currentTimeMillis(), TokenType.USER);
        req.setPrincipal(getUnsignedEzSecurityPrincipal(DN));
        String signature = EzSecurityTokenUtils.tokenRequestSignature(req, crypto);
        req.setCaveats(new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis()+1000, signature));

        EzSecurity.Client client = ezbakeSecurityClient.getClient();
        try {
            EzSecurityTokenJson res = client.requestUserInfoAsJson(req, signature);
            Assert.assertNotNull(res);
            System.out.println(res.getJson());
        } finally {
            ezbakeSecurityClient.returnClient(client);
        }
    }
}
