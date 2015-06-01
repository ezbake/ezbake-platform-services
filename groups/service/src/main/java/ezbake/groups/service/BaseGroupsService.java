/*   Copyright (C) 2013-2015 Computer Sciences Corporation
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

package ezbake.groups.service;

import com.google.common.collect.Lists;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.common.properties.EzProperties;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.UserType;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.SecurityID;
import ezbake.thrift.authentication.EzX509;
import ezbake.thrift.authentication.ThriftPeerUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;

/**
 * Interface for groups services to be closeable and validate tokens
 */
public abstract class BaseGroupsService implements EzGroups.Iface, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BaseGroupsService.class);
    protected static final String X509_RESTRICT = "ezbake.groups.service.x509.restrict";
    protected static final GroupNameHelper nameHelper = new GroupNameHelper();

    protected EzProperties configuration;
    protected EzbakeSecurityClient ezbakeSecurityClient;

    public BaseGroupsService(Properties configuration, EzbakeSecurityClient ezbakeSecurityClient) {
        this.configuration = new EzProperties(configuration, true);
        this.ezbakeSecurityClient = ezbakeSecurityClient;
    }

    public static BaseVertex.VertexType userTypeFromUserType(UserType type) {
        if(type == UserType.USER) {
            return BaseVertex.VertexType.USER;
        } else {
            return BaseVertex.VertexType.APP_USER;
        }
    }

    public static BaseVertex.VertexType userTypeFromToken(EzSecurityToken token) {
        return vertexTypeFromTokenType(token.getType());
    }

    public static BaseVertex.VertexType vertexTypeFromTokenType(TokenType type) {
        if (type == TokenType.USER) {
            return BaseVertex.VertexType.USER;
        }  else {
            return BaseVertex.VertexType.APP_USER;
        }
    }

    public static List<String> requestChainFromToken(EzSecurityToken token) {
        List<String> appChain = token.getTokenPrincipal().getRequestChain();
        if (appChain == null) {
            appChain = Lists.newArrayList();
        }

        String issuedTo = token.getValidity().getIssuedTo();
        String issuedFor = token.getValidity().getIssuedFor();
        if (!appChain.contains(issuedTo)) {
            appChain.add(issuedTo);
        }
        if (!appChain.contains(issuedFor)) {
            appChain.add(issuedFor);
        }

        return appChain;
    }

    /**
     * Validate an EzSecurityToken, using the EzbakeSecurityClient to make sure it is valid, but performing no other
     * access checks
     *
     * @param token a received EzSecurityToken to be validated
     * @throws ezbake.base.thrift.EzSecurityTokenException
     */
    protected void validateToken(EzSecurityToken token) throws EzSecurityTokenException {
        validateToken(token, false);
    }

    /**
     * Validate an EzSecurityToken, using the EzbakeSecurityClient to make sure it is valid. If requireAdmin is true,
     * then the token must belong to an EzAdmin
     *
     * @param token a received EzSecurityToken to be validated
     * @param requireAdmin whether or not to require administrator privileges
     * @throws EzSecurityTokenException if the token fails to validate, or requireAdmin is true and it is not EzAdmin
     */
    protected void validateToken(EzSecurityToken token, boolean requireAdmin) throws EzSecurityTokenException {
        logger.debug("Validating security token from: {} require admin: {}", token.getValidity().getIssuedTo(), requireAdmin);
        ezbakeSecurityClient.validateReceivedToken(token);
        if (requireAdmin && !EzbakeSecurityClient.isEzAdmin(token)) {
            logger.info("Rejecing security token from: {}. EzAdmin is required!", token.getValidity().getIssuedTo());
            throw new EzSecurityTokenException("EzAdmin permissions are required");
        }
    }


    protected boolean isPrivilegedPeer(String peerSId, SecurityID.ReservedSecurityId ...requiredPeers) {
        boolean privileged = false;
        List<SecurityID.ReservedSecurityId> allowedPeers = Lists.newArrayList(requiredPeers);
        if (SecurityID.ReservedSecurityId.isReserved(peerSId)) {
            if (allowedPeers.contains(SecurityID.ReservedSecurityId.fromEither(peerSId))) {
                privileged = true;
            }
        }
        return privileged;
    }

    protected boolean isPrivilegedPeer(EzX509 peer, SecurityID.ReservedSecurityId ...requiredPeers) {
        boolean privileged;
        if (configuration.getBoolean(X509_RESTRICT, true)) {
            try {
                privileged = isPrivilegedPeer(peer.getPeerSecurityID(), requiredPeers);
            } catch (ThriftPeerUnavailableException e) {
                privileged = false;
            }
        } else {
            // Not restricting based on x509 certificates, let them through
            privileged = true;
        }
        return privileged;
    }

    protected void validatePrivilegedPeer(EzSecurityToken token, EzX509 peer) throws EzSecurityTokenException {
        validateToken(token);

        if (configuration.getBoolean(X509_RESTRICT, true)) {
            try {
                String peerSId = peer.getPeerSecurityID();
                if (!isPrivilegedPeer(peerSId, SecurityID.ReservedSecurityId.EzSecurity)) {
                    logger.error("createUserAndGetAuthorizations request from unauthorized peer; {}", peerSId);
                    throw new EzSecurityTokenException("createUserAndGetAuthorizations may only be called by EzSecurity, " +
                            "not: " + peerSId);
                }
            } catch (ThriftPeerUnavailableException e) {
                logger.error("createUserAndGetAuthorizations request unable to get peer CN from X509 cert");
                throw new EzSecurityTokenException("peer CN unavailable from X509 cert. Are you using SSL?");
            }
        }
    }
}
