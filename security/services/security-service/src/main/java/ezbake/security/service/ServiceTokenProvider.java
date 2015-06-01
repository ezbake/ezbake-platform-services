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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ezbake.base.thrift.*;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.common.core.EzSecurityConstant;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.common.core.SecurityID;
import ezbake.security.service.processor.EzSecurityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: jhastings
 * Date: 8/1/14
 * Time: 12:36 PM
 */
public class ServiceTokenProvider implements EzSecurityTokenProvider {
    private static final Logger logger = LoggerFactory.getLogger(ServiceTokenProvider.class);
    private static final Long tokenExpiration = 30*1000l;

    private PKeyCrypto crypto;
    @Inject
    public ServiceTokenProvider(@Named("server crypto")PKeyCrypto crypto) {
        this.crypto = crypto;
    }

    @Override
    public EzSecurityToken get(String targetSecurityId) throws EzSecurityTokenException {
        logger.info("EzSecurity generating it's own token for external service: {}", targetSecurityId);
        // Get a token that proxies the given app, and is for the target app
        String securityId = SecurityID.ReservedSecurityId.EzSecurity.getId();

        EzSecurityToken token = EzSecurityTokenUtils.freshToken(securityId, TokenType.APP,
                System.currentTimeMillis()+tokenExpiration);
        token.getValidity().setIssuedFor(targetSecurityId);

        EzSecurityPrincipal principal = new EzSecurityPrincipal(
                securityId,
                EzSecurityTokenUtils.generateValidityCaveats("EzSecurity", securityId,
                    System.currentTimeMillis()+tokenExpiration));

        // Update the request chain - this may be used in the targetApp
        EzSecurityHandler.updateChain(securityId, targetSecurityId, principal);

        // EzSecurity always operates at admin level
        Map<String, List<String>> groups = Maps.newHashMap();
        groups.put(EzSecurityConstant.EZ_INTERNAL_PROJECT,
                Lists.newArrayList(EzSecurityConstant.EZ_INTERNAL_ADMIN_GROUP));
        token.setExternalProjectGroups(groups);

        try {
            principal.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(principal, crypto));
            token.setTokenPrincipal(principal);
            token.getValidity().setSignature(EzSecurityTokenUtils.tokenSignature(token, crypto));

        } catch (IOException |PKeyCryptoException e) {
            throw new EzSecurityTokenException("Failure to generate a valid token signature");
        }

        return token;
    }
}
