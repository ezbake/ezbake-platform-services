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

package ezbake.security.service.group;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.groups.thrift.*;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.service.EzSecurityContext;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class EzGroupsClient {
    private static final Logger logger = LoggerFactory.getLogger(EzGroupsClient.class);
    private static final String GROUP_SERVICE_NAME = EzGroupsConstants.SERVICE_NAME;

    private ThriftClientPool clientPool;
    private EzSecurityTokenProvider tokenProvider;
    private Supplier<String> ezGroupsSecurityId = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            return clientPool.getSecurityId(GROUP_SERVICE_NAME);
        }
    });

    @Inject
    public EzGroupsClient(EzSecurityTokenProvider tokenProvider, ThriftClientPool clientPool) {
        this.clientPool = clientPool;
        this.tokenProvider = tokenProvider;
    }

    /**
     * This will attempt to get a user's authorizations from the EzGroup service. In the case of any exception, this
     * will return an empty set.
     *
     * @param userType type of token the authorizations are being requested for, which is either USER or APP
     * @param id the user's unique platform ID
     * @return an ordered set of all the user group authorizations, if there was an error, it will return the empty set
     */
    public Set<Long> getAuthorizations(List<String> chain, TokenType userType, String id, String name) {
        Set<Long> authorizations = Collections.emptySet();

        EzGroups.Client client = null;
        try {
            logger.debug("trying to get an ezgroups client {} (requestUUID {})", id, EzSecurityContext.getRequestId(), clientPool);
            client = clientPool.getClient(GROUP_SERVICE_NAME, EzGroups.Client.class);

            EzSecurityToken token = tokenProvider.get(ezGroupsSecurityId.get());
            switch (userType) {
                case USER:
                    logger.info("User getAuthorizations/create for {}, chain: {} (requestUUID {})", id, chain, EzSecurityContext.getRequestId());
                    authorizations = client.createUserAndGetAuthorizations(token, chain, id, name);
                    break;
                case APP:
                    logger.info("App user getAuthorizations/create for {}, chain: {} (requestUUID {})", id, chain, EzSecurityContext.getRequestId());
                    authorizations = client.createAppUserAndGetAuthorizations(token, chain, id, name);
                    break;
            }
            logger.debug("User: {} has authorizations: {}", id, authorizations);
        } catch (EzSecurityTokenException tokenError) {
            logger.error("EzSecurityToken was rejected by {} - {}", GROUP_SERVICE_NAME, tokenError.getMessage());
        } catch (GroupQueryException queryError) {
            logger.error("User authorizations query was rejected: {}", queryError.getErrorType());
        } catch (TException e) {
            logger.error("Caught TException connecting to EzGroups to request authorizations: {}", e.getMessage());
        } finally {
            if (client != null) {
                logger.debug("Returning ezgroups client to the pool (requestUUID {})", EzSecurityContext.getRequestId());
                clientPool.returnToPool(client);
            }
        }
        logger.debug("returning group auths for {} (requestUUID {})", id, EzSecurityContext.getRequestId());
        return authorizations;
    }

    public Set<Long> getAppAccessGroup(String appName) {
        Set<Long> mask = Collections.emptySet();
        String groupName = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                .join(EzGroupsConstants.APP_ACCESS_GROUP, appName);

        EzGroups.Client client = null;
        try {
            client = clientPool.getClient(GROUP_SERVICE_NAME, EzGroups.Client.class);

            logger.debug("Getting groups mask {}", appName);
            mask = client.getGroupsMask(tokenProvider.get(ezGroupsSecurityId.get()), Sets.newHashSet(groupName));
        } catch (EzSecurityTokenException tokenError) {
            logger.error("EzSecurityToken was rejected by {}", GROUP_SERVICE_NAME);
        } catch (GroupQueryException queryError) {
            logger.error("User authorizations query was rejected: {}", queryError.getErrorType());
        } catch (TException e) {
            logger.error("Caught TException connecting to EzGroups to request authorizations: {}", e.getMessage());
        } finally {
            clientPool.returnToPool(client);
        }

        return mask;
    }
}
