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

package ezbake.protect.test;

import ezbake.base.thrift.*;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.security.common.core.SecurityID;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.thrift.EzSecurity;
import ezbake.security.thrift.ProxyTokenRequest;
import ezbake.security.thrift.ProxyTokenResponse;
import org.apache.thrift.TException;

import java.io.IOException;

/**
 * User: jhastings
 * Date: 7/8/14
 * Time: 9:37 AM
 */
public class EzSecurityClientHelpers {

    public static ProxyPrincipal getPrincipalToken(EzSecurity.Client client, PKeyCrypto efeCrypto, String principal) throws TException, PKeyCryptoException, IOException {
        ProxyTokenRequest req = new ProxyTokenRequest(
                new X509Info(principal),
                new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis(), ""));
        req.getValidity().setSignature(EzSecurityTokenUtils.proxyTokenRequestSignature(req, efeCrypto));

        ProxyTokenResponse response = client.requestProxyToken(req);
        return new ProxyPrincipal(response.getToken(), response.getSignature());
    }

    public static EzSecurityTokenJson getJsonToken(EzSecurity.Client client, PKeyCrypto crypto, String principal) throws IOException, PKeyCryptoException, TException {
        TokenRequest jsonRequest = new TokenRequest();
        jsonRequest.setSecurityId(SecurityID.ReservedSecurityId.EFE.getId());
        jsonRequest.setTargetSecurityId(SecurityID.ReservedSecurityId.EFE.getId());
        jsonRequest.setTimestamp(System.currentTimeMillis());
        jsonRequest.setPrincipal(new EzSecurityPrincipal(
                principal,
                new ValidityCaveats("EzSecurity", "",System.currentTimeMillis()+1000, "")));

        return client.requestUserInfoAsJson(jsonRequest, EzSecurityTokenUtils.tokenRequestSignature(jsonRequest, crypto));
    }

    public static EzSecurityToken getUserToken(EzSecurity.Client client, PKeyCrypto crypto, ProxyPrincipal principal, String securityID) throws IOException, PKeyCryptoException, TException {
        TokenRequest userRequest = new TokenRequest(
                securityID,
                System.currentTimeMillis(),
                TokenType.USER);
        userRequest.setProxyPrincipal(principal);

        return client.requestToken(userRequest, EzSecurityTokenUtils.tokenRequestSignature(userRequest, crypto));
    }

    public static EzSecurityToken getAppToken(EzSecurity.Client client, PKeyCrypto crypto, String securityID) throws IOException, PKeyCryptoException, TException {
        TokenRequest appRequest = new TokenRequest(
                securityID,
                System.currentTimeMillis(),
                TokenType.APP);

        return client.requestToken(appRequest, EzSecurityTokenUtils.tokenRequestSignature(appRequest, crypto));
    }

}
