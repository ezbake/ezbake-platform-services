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

package ezbake.security.service;

import com.google.common.collect.Sets;
import ezbake.base.thrift.*;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.impl.ua.FileUAService;
import ezbake.security.persistence.AppPersistenceModule;
import ezbake.security.persistence.impl.FileRegManager;
import ezbake.security.service.processor.EzSecurityTokenBaseTest;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.easymock.EasyMock;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * User: jhastings
 * Date: 4/25/14
 * Time: 10:52 AM
 */
public class EzSecurityBaseTest {
    public static final String serverPki = "/pki/server";
    public static final String clientPki = "/pki/client";

    public static final String serverPrivateKey = "src/test/resources/pki/server/application.priv";
    public static final String serverPublicKey = "src/test/resources/pki/server/application.pub";

    public static final String serverID = "EzSecurityServer";
    public static final String clientID = "SecurityClientTest";

    public static Properties setServerEzConfig(Properties ezConfiguration) {
        Properties copy = new EzProperties(ezConfiguration, true);
        copy.setProperty(FileUAService.USERS_FILENAME,
                EzSecurityBaseTest.class.getResource("/users.json").getFile());
        copy.setProperty(
                EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                EzSecurityTokenBaseTest.class.getResource(serverPki).getFile());
        copy.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, serverID);
        copy.setProperty(EzBakePropertyConstants.EZBAKE_APP_REGISTRATION_IMPL,
                FileRegManager.class.getCanonicalName());
        copy.setProperty(FileRegManager.REGISTRATION_FILE_PATH,
                EzSecurityBaseTest.class.getResource("/ezsecurity_apps.yaml").getFile());
        return copy;
    }

    public static Properties setClientEzConfig(Properties ezConfiguration) {
        Properties copy = new EzProperties(ezConfiguration, true);
        copy.setProperty(
                EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                EzSecurityBaseTest.class.getResource(clientPki).getFile());
        copy.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, clientID);
        return copy;
    }

    public ValidityCaveats getValidityCaveats(String issuedTo) {
        return getValidityCaveats(issuedTo, "");
    }
    public ValidityCaveats getValidityCaveats(String issuedTo, String issuedFor) {
        ValidityCaveats vc = new ValidityCaveats("EzSecurity", issuedTo, System.currentTimeMillis()+60000, "");
        vc.setIssuedFor(issuedFor);
        return vc;
    }

    public ThriftClientPool getMockClientPool() throws TException {
        // EzGroups mock
        EzGroups.Client ezGroups = EasyMock.createNiceMock(EzGroups.Client.class);
        EasyMock.expect(ezGroups.createUserAndGetAuthorizations(
                EasyMock.anyObject(EzSecurityToken.class),
                EasyMock.<List<String>>anyObject(),
                EasyMock.anyString(),
                EasyMock.anyString()
        )).andReturn(Sets.newHashSet(1l, 2l, 3l)).anyTimes();
        EasyMock.expect(ezGroups.createAppUserAndGetAuthorizations(
                EasyMock.anyObject(EzSecurityToken.class),
                EasyMock.<List<String>>anyObject(),
                EasyMock.anyString(),
                EasyMock.anyString()
        )).andReturn(Sets.newHashSet(1l, 2l, 3l)).anyTimes();

        // TODO: write a test that changes this to be invalid and actually check app access (have run tests with invalid app access and they do fail, just need to capture it for real
        EasyMock.expect(ezGroups.getGroupsMask(
                EasyMock.anyObject(EzSecurityToken.class),
                EasyMock.<Set<String>>anyObject()
        )).andReturn(Sets.<Long>newHashSet()).anyTimes();

        // Thrift Client pool mock
        ThriftClientPool clientPool = EasyMock.createMock(ThriftClientPool.class);
        EasyMock.expect(clientPool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class)).andReturn(ezGroups).anyTimes();
        EasyMock.expect(clientPool.getSecurityId(EasyMock.anyString())).andReturn("ezgroups").anyTimes();
        clientPool.returnToPool(EasyMock.anyObject(TServiceClient.class));
        EasyMock.expectLastCall().anyTimes();

        EasyMock.replay(clientPool, ezGroups);
        return clientPool;
    }

    public ProxyPrincipal getSignedProxyPrincipal(String dn, PKeyCrypto crypto) throws PKeyCryptoException, TException {
        ProxyUserToken proxyUserToken = new ProxyUserToken(
                new X509Info(dn),
                "EzSecurity",
                "*",
                System.currentTimeMillis()+1000);

        return new ProxyPrincipal(EzSecurityTokenUtils.serializeProxyUserTokenToJSON(proxyUserToken),
                EzSecurityTokenUtils.proxyUserTokenSignature(proxyUserToken, crypto));
    }

    public EzSecurityPrincipal getSignedDn(String dns, PKeyCrypto crypto) throws PKeyCryptoException, IOException {
        EzSecurityPrincipal dn = new EzSecurityPrincipal();
        dn.setPrincipal(dns);

        dn.setValidity(new ValidityCaveats("EzSecurity", "", System.currentTimeMillis()+1000, ""));
        dn.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(dn, crypto));

        return dn;
    }
}
