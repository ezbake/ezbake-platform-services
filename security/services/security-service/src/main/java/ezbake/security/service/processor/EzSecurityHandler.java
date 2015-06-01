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

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import ezbake.base.thrift.*;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.api.ua.*;
import ezbake.security.common.core.*;
import ezbake.security.common.core.TokenExpiredException;
import ezbake.security.service.TokenSignatureVerificationException;
import ezbake.security.service.group.EzGroupsClient;
import ezbake.security.service.modules.AdminServiceModule;
import ezbake.security.service.modules.EzSecurityModule;
import ezbake.security.service.modules.TokenJSONModule;
import ezbake.security.service.policy.AuthorizationPolicy;
import ezbake.security.service.sync.EzSecurityRedisCache;
import ezbake.security.thrift.*;
import ezbake.security.thrift.UserNotFoundException;
import ezbake.security.ua.UAModule;
import ezbake.security.service.EzSecurityContext;
import ezbake.security.service.registration.AppInstance;
import ezbake.security.service.registration.EzbakeRegistrationService;
import ezbake.security.service.admins.AdministratorService;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;

import ezbake.thrift.authentication.EzX509;
import ezbake.thrift.authentication.ThriftPeerUnavailableException;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.util.*;

import ezbake.base.thrift.EzBakeBaseThriftService;

import javax.inject.Named;

/**
 * User: jhastings
 * Date: 9/27/13
 * Time: 8:28 AM
 */
public class EzSecurityHandler extends EzBakeBaseThriftService implements EzSecurity.Iface {
    private static Logger log = LoggerFactory.getLogger(EzSecurityHandler.class);
    private static AuditLogger auditLogger = AuditLogger.getAuditLogger(EzSecurityHandler.class);
    public static final Long maxTokenRefresh = ((11 * 60) + 23) * 60 * 1000l; // The running time of the Lord of the Rings trilogy extended edition 11 hours, 23 minutes
    private static long wiggleMillis = 5 /* seconds */ * 1000 /* milliseconds */;

    private Properties ezConfiguration;
    private PKeyCrypto crypto;
    private long requestExpirationMillis = 60 /* minutes */ * 60 /* seconds */ * 1000 /* milliseconds */;
    private boolean ignoreSignedDn = false;
    private long tokenTtlMillis;
    private long proxyTokenTtlMillis;
    private String mySecurityId;

    private UserAttributeService uaservice;
    private TokenJSONProvider jsonProvider;
    private AdministratorService adminService;
    private EzbakeRegistrationService clientLookup;
    private AuthorizationPolicy authorizationPolicy;
    private EzGroupsClient ezGroups;

    /**
     * This constructor should only be used when calling getThriftProcessor on the new instance
     */
    public EzSecurityHandler() {
        this.uaservice = null;
        this.jsonProvider = null;
        this.adminService = null;
        this.clientLookup = null;
        this.ezGroups = null;
    }

    @Inject
    public EzSecurityHandler(Properties properties, UserAttributeService uaservice,
                             TokenJSONProvider jsonProvider, AdministratorService adminService,
                             EzbakeRegistrationService clientLookup, EzGroupsClient ezGroups,
                             @Named("server crypto")PKeyCrypto crypto) throws IOException {
        // Configuration
        ezConfiguration = properties;

        SystemConfigurationHelper sysconfig = new SystemConfigurationHelper(properties);
        SecurityConfigurationHelper securityConfiguration = new SecurityConfigurationHelper(ezConfiguration, sysconfig.getTextCryptoProvider());
        EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(ezConfiguration);

        // Injected services
        this.uaservice = uaservice;
        this.ezGroups = ezGroups;
        this.jsonProvider = jsonProvider;
        this.adminService = adminService;
        this.clientLookup = clientLookup;

        this.authorizationPolicy = AuthorizationPolicy.getInstance(ezConfiguration);

        // Sign/Verify
        this.crypto = crypto;
        this.mySecurityId = appConfig.getSecurityID();

        // Set up mock
        ignoreSignedDn = securityConfiguration.useMockServer();
        if (ignoreSignedDn) {
            log.warn("Staring EzSecurity in 'mock' mode. Won't validate DN signature. This is not a production option");
        }

        EzProperties propHelper = new EzProperties(properties, true);

        tokenTtlMillis = propHelper.getLong(EzBakePropertyConstants.EZBAKE_TOKEN_EXPIRATION, 120) * 1000;
        proxyTokenTtlMillis = propHelper.getLong(EzBakePropertyConstants.EZBAKE_SECURITY_PROXYTOKEN_TTL, 720)* 1000;
        requestExpirationMillis = propHelper.getLong(EzBakePropertyConstants.EZBAKE_REQUEST_EXPIRATION, 60) * 1000;
    }

    private List<EzSecurityHandler> injectedHandlers = new ArrayList<>();
    @Override
    public TProcessor getThriftProcessor() {
        EzSecurityHandler handler = getHandler(getConfigurationProperties());
        injectedHandlers.add(handler);
        return new EzSecurity.Processor<>(handler);
    }

    public static EzSecurityHandler getHandler(Properties ezConfig) {
        Injector injector = Guice.createInjector(
                new EzSecurityModule(ezConfig),
                new AdminServiceModule(ezConfig),
                new UAModule(ezConfig),
                new TokenJSONModule(ezConfig));
        return injector.getInstance(EzSecurityHandler.class);
    }


    /**
     * Shutdown the server, this is called before the server process exits
     */
    @Override
    public void shutdown() {
        log.info("Shutting down all EzSecurithHandler instances. Instances to stop: {}", injectedHandlers.size());
        if (injectedHandlers != null) {
            Iterator<EzSecurityHandler> handlers = injectedHandlers.iterator();
            while(handlers.hasNext()) {
                handlers.next().stopEverything();
                handlers.remove();
            }
        }
    }

    private void stopEverything() {
        if (adminService != null) {
            // stop the admin service
            adminService.close();
        }
        if (uaservice != null) {
            try {
                uaservice.getCache().close();
            } catch (Exception e) {
                log.error("Error closing UAService cache: {}", e.getMessage());
            }
        }
    }


    /**
     * Ping the security service to determine whether or not it is healthy
     *
     * @return true if the service is healthy
     */
    @Override
    public boolean ping() {
        log.info("pong");
        return true;
    }


    /**
     * Not all instances of EzSecurity know who the admins are. Accept an SSL connection from another instance of
     * EzSecurity and take those as the admins for this instance
     *
     * @param admins the set of admins
     * @return true if the admins were updated
     */
    @Override
    public boolean updateEzAdmins(Set<String> admins) {
        EzX509 peer = new EzX509(); // get the peer certificate

        // Make sure only called by another instance of EzSecurity
        try {
            String peerId = peer.getPeerSecurityID();

            // Make sure peer ID matches EzSecurity's CN
            if (SecurityID.ReservedSecurityId.EzSecurity.getCn().equals(peerId)) {
                log.info("updateEzAdmins invoked by EzSecurity peer, updating our admins cache");
                log.info("updateEzAdmins using admins: {}", admins);
                adminService.setAdmins(admins);
            } else {
                // called by peer with the wrong CN! log the problem
                log.error("updateEzAdmins invoked by an invalid peer: {}", peerId);
            }
        } catch (ThriftPeerUnavailableException e) {
            log.info("updateEzAdmins invoked by a non-ssl client. Not processing admins");
        }

        return true;
    }


    /**
     * Invalidate the user attribute caches
     *
     * @param token peer security token
     * @throws EzSecurityTokenException
     */
    @Override
    public void invalidateCache(EzSecurityToken token) throws EzSecurityTokenException {
        if(!EzSecurityTokenUtils.isEzAdmin(token)) {
            log.error("Token of invocation was not of an Ez Admin, and therefore cache was not invalidated.");
            throw new EzSecurityTokenException("You are not an Admin, and therefore you do not have the privileges to invoke this method. " );
        }

        try {
            EzSecurityRedisCache cache = this.uaservice.getCache();
            if(cache != null) {
                log.info("Invalidating UA Service cache");
                cache.invalidate();
            }
        } catch (Exception e) {
            log.error("Error invalidating the cache {}", e);
            throw new EzSecurityTokenException("Error invalidating the cache" +  e);
        }
    }

    /**
     *
     * @param tokenRequest the request object
     * @param signature token request signed by the requesting application / token request issuer
     * @return a security token
     * @throws ezbake.security.thrift.AppNotRegisteredException
     * @throws ezbake.base.thrift.EzSecurityTokenException
     */
    @Override
    public EzSecurityToken requestToken(TokenRequest tokenRequest, String signature) throws AppNotRegisteredException, EzSecurityTokenException {
        if (EzSecurityContext.getRequestId() == null) {
            EzSecurityContext.setUp();
        }
        UUID uuid = EzSecurityContext.getRequestId();
        log.info("requestToken: requester: {}, targetApp: {} (requestUUID {})", tokenRequest.getSecurityId(),
                tokenRequest.getTargetSecurityId(), uuid);

        TokenType tokenType = tokenRequest.getType();

        // Get the requesting application registration
        String issueTo = tokenRequest.getSecurityId();
        String issueFor = (tokenRequest.isSetTargetSecurityId()) ? tokenRequest.getTargetSecurityId() : tokenRequest.getSecurityId();
        AppInstance requester = fetchApplication(issueTo);
        AppInstance targetApp = fetchApplication(issueFor);

        // Figure out who signed the request
        AppInstance issuer;
        // Get the issuer application registration
        if (tokenRequest.getCaveats() != null) {
            String issuerSecurityId = tokenRequest.getCaveats().getIssuer();
            if (issuerSecurityId != null && !issuerSecurityId.equals(issueTo)) {
                // fetch the issuer app registration
                issuer = fetchApplication(issuerSecurityId);
            } else {
                issuer = requester;
            }
        } else {
            issuer = requester;
        }

        // Now that the TokenRequest issuer has been determined, validate the token request
        try {
            validateTokenRequest(tokenRequest, signature, issuer);
        } catch (TokenExpiredException e) {
            log.info("TokenRequest was expired. from: {} (requestUUID {})", issuer.getId(), uuid);
            throw new EzSecurityTokenException("EzSecurity rejected requestToken, reason: TokenRequest was expired");
        } catch (TokenSignatureVerificationException e) {
            log.info("failed to verify signature from: {} (requestUUID {})", issuer.getId(), uuid);
            throw new EzSecurityTokenException("EzSecurity failed to verify signature provided by: " + issuer.getId());
        }

        // Enforce App access
        if (!hasAppAccess(requester.getId(), targetApp.getRegistration().getAppName())) {
            throw new EzSecurityTokenException("AppAccess rejected from " + requester.getId() + " to " + targetApp.getId());
        }

        // Verify any EzSecurity signed objects in the request - that's the user DN if present
        validateRequestSignatures(tokenRequest, issuer.getId());

        // Get the principal from the request
        EzSecurityPrincipal tokenPrincipal;
        try {
            tokenPrincipal = getPrincipalFromRequest(tokenRequest);
        } catch (PKeyCryptoException|IOException e) {
            log.error("Failed to generate a signature for the principal in token request: {}", e.getMessage());
            throw new EzSecurityTokenException("Unable to extract/generate an EzSecurityPrincipal for the request: "+
                    e.getMessage());
        }

        ValidityCaveats tokenCaveats = getValidityCaveatsFromRequest(tokenRequest);
        tokenCaveats.setIssuedTo(issueTo);
        tokenCaveats.setIssuedFor(issueFor);

        String subject = tokenPrincipal.getPrincipal();
        List<String> requestChain = tokenPrincipal.getRequestChain();
        Set<String> formalAuthPreFilter = getAuthFilterFromTokenRequest(tokenRequest);

        // These authorizations should be excluded from the final sets
        Set<String> authorizationPostFilter = tokenRequest.getExcludeAuthorizations();

        // Build the token
        EzSecurityToken token = EzSecurityTokenUtils.freshToken(issueTo, tokenType, getTokenNotAfter());
        try {
            log.info("requestToken: subject: {}, type: {} (requestUUID {})", subject, tokenType, uuid);

            // Get the target app if necessary
            Set<String> targetAppAuths = null;
            Set<String> targetAppCommunityAuths = null;
            if(tokenRequest.getTargetSecurityId() != null) {
                targetAppAuths = targetApp.getAuthorizations();
                targetAppCommunityAuths = Sets.newHashSet(targetApp.getRegistration().getCommunityAuthorizations());
            }

            String name;
            String externalId;
            String tokenLevel;
            Set<String> tokenAuths;
            Set<String> tokenCommunityAuths;
            switch (tokenType) {
                case APP:
                    // only allow to request own auths
                    if (isFirstRequest(tokenRequest) && !tokenRequest.getSecurityId().equals(subject)) {
                        log.info("appInfo request rejected from {}. Illegal request for other app {} (requestUUID {})",
                                tokenRequest.getSecurityId(), subject, uuid);
                        throw new EzSecurityTokenException("appInfo request rejected. App should only request own auths");
                    }
                    name = requester.getRegistration().getAppName();
                    externalId = requester.getRegistration().getAppDn();
                    tokenLevel = requester.getRegistration().getAuthorizationLevel();
                    tokenAuths = requester.getAuthorizations();
                    tokenCommunityAuths = Sets.newHashSet(requester.getRegistration().getCommunityAuthorizations());
                    break;
                case USER:
                default:
                    /* Add the current request to the request chain */
                    User info = fetchUser(subject);
                    name = info.getName();
                    externalId = info.getUid();
                    tokenLevel = info.getAuthorizations().getLevel();
                    tokenAuths = Sets.newTreeSet(info.getAuthorizations().getAuths());
                    tokenCommunityAuths = authorizationPolicy.externalCommunityAuthorizationsForUser(info);

                    /* Get and filter External Auths */
                    authorizationPolicy.populateTokenForUser(token, info);
                    break;
            }

            // Set the common things on the token principal
            tokenPrincipal.setName(name);
            tokenPrincipal.setExternalID(externalId);
            updateChain(issueTo, issueFor, tokenPrincipal);
            updatePrincipalValidity(tokenPrincipal, issueTo);

            // Set authorizations on the token
            token.setAuthorizationLevel(tokenLevel);

            // Filter the authorizations
            tokenAuths = filterAuthorizations(tokenAuths, requester.getAuthorizations(), targetAppAuths, formalAuthPreFilter, requestChain);
            tokenCommunityAuths = filterCommunityAuthorizations(tokenCommunityAuths, Sets.newHashSet(requester.getRegistration().getCommunityAuthorizations()), targetAppCommunityAuths, requestChain);

            // Do the post filter
            if (authorizationPostFilter != null) {
                tokenAuths = Sets.difference(tokenAuths, authorizationPostFilter);
                tokenCommunityAuths = Sets.difference(tokenCommunityAuths, authorizationPostFilter);
            }

            token.getAuthorizations().setFormalAuthorizations(tokenAuths);
            token.getAuthorizations().setExternalCommunityAuthorizations(tokenCommunityAuths);

            // Populate EzGroups
            Set<Long> groups = ezGroups.getAuthorizations(tokenPrincipal.getRequestChain(), tokenType, subject, name);
            token.getAuthorizations().setPlatformObjectAuthorizations(groups);


            // Finally, update the token with all the pertinent information
            updateToken(token, tokenCaveats, tokenPrincipal);

        } catch (UserNotFoundException e) {
            log.info("User: {} not found in database, request from: {} (requestUUID {})", subject, requester.getId(), uuid);
            throw new EzSecurityTokenException("User not found in database");
        } catch (PKeyCryptoException e) {
            log.info("EzSecurity unable to issue tokens. Error signing the token - from: {} (requestUUID {})", requester.getId(), uuid, e);
            throw new EzSecurityTokenException("EzSecurity unable to issue tokens at this time");
        } catch (IOException e) {
            log.info("Caught an IOException trying to serialize the token for signing", e);
            throw new EzSecurityTokenException("EzSecurity unable to issue tokens at this time");
		} catch (TException e) {
            log.error("Unexpected texception", e);
            throw e;
        } catch (Exception e) {
            log.error("catching and rethrowing an exception", e);
            throw e;
        } finally {
            EzSecurityContext.cleanUp();
        }

        log.info("requestToken: returning token for requestUUID {}", uuid);
        return token;
    }

    @Override
    public EzSecurityToken refreshToken(TokenRequest request, String signature) throws EzSecurityTokenException, AppNotRegisteredException {
        if (EzSecurityContext.getRequestId() == null) {
            EzSecurityContext.setUp();
        }
        UUID uuid = EzSecurityContext.getRequestId();
        log.info("refreshToken: requester: {} (requestUUID {})", request.getSecurityId(), uuid);

        // Get the issuer registration
        String issuerId = request.getSecurityId();
        if (request.getCaveats() != null) {
            String issuerSecurityId = request.getCaveats().getIssuer();
            if (issuerSecurityId != null) {
                issuerId = issuerSecurityId;
            }
        }
        AppInstance issuer = fetchApplication(issuerId);

        // Now that the TokenRequest issuer has been determined, validate the token request
        try {
            validateTokenRequest(request, signature, issuer);
        } catch (TokenExpiredException e) {
            log.info("TokenRequest was expired. from: {} (requestUUID {})", issuer.getId(), uuid);
            throw new EzSecurityTokenException("EzSecurity rejected requestToken, reason: TokenRequest was expired");
        } catch (TokenSignatureVerificationException e) {
            log.info("failed to verify signature from: {} (requestUUID {})", issuer.getId(), uuid);
            throw new EzSecurityTokenException("EzSecurity failed to verify signature provided by: " + issuer.getId());
        }

        // Make sure it is actually a security token token request
        EzSecurityToken requestToken = request.getTokenPrincipal();
        if (requestToken == null) {
            throw new EzSecurityTokenException("EzSecurityToken could not be refreshed because no EzSecurityToken " +
                    "was passed in request");
        } else {
            // Verify the token
            if (!EzSecurityTokenUtils.verifyTokenSignature(requestToken, crypto)) {
                throw new EzSecurityTokenException("Unable to verify passed EzSecurityToken");
            }
        }

        // Make sure the token isn't too old
        if (requestToken.getValidity().getIssuedTime()+maxTokenRefresh <= System.currentTimeMillis()) {
            throw new EzSecurityTokenException("Token could not be refreshed because it is too old");
        }

        // Now fetch all users credentials again
        String subject = requestToken.getTokenPrincipal().getPrincipal();
        String tokenLevel;
        Set<String> tokenAuths;
        Set<String> tokenCommunityAuths;
        switch (requestToken.getType()) {
            case APP:
                // Fetch the principal app
                AppInstance app = fetchApplication(subject);
                tokenAuths = app.getAuthorizations();
                tokenLevel = app.getRegistration().getAuthorizationLevel();
                tokenCommunityAuths = Sets.newHashSet(app.getRegistration().getCommunityAuthorizations());
                break;
            case USER:
            default:
                try {
                    User info = fetchUser(subject);
                    tokenAuths = Sets.newTreeSet(info.getAuthorizations().getAuths());
                    tokenLevel = info.getAuthorizations().getLevel();
                    tokenCommunityAuths = authorizationPolicy.externalCommunityAuthorizationsForUser(info);
                } catch (UserNotFoundException e) {
                    log.info("User: {} not found in database (requestUUID {})", subject, uuid);
                    throw new EzSecurityTokenException("User "+subject+" not found in database");
                }
        }
        // Populate authorizations
        requestToken.setAuthorizationLevel(tokenLevel);
        requestToken.getAuthorizations().setFormalAuthorizations(
                Sets.intersection(requestToken.getAuthorizations().getFormalAuthorizations(), tokenAuths));
        requestToken.getAuthorizations().setExternalCommunityAuthorizations(
                Sets.intersection(requestToken.getAuthorizations().getExternalCommunityAuthorizations(), tokenCommunityAuths));

        // Populate EzGroups
        Set<Long> groups = ezGroups.getAuthorizations(
                requestToken.getTokenPrincipal().getRequestChain(),
                requestToken.getType(),
                subject,
                requestToken.getTokenPrincipal().getName());
        requestToken.getAuthorizations().setPlatformObjectAuthorizations(groups);

        try {
            updateRefreshToken(requestToken);
        } catch (PKeyCryptoException e) {
            log.info("EzSecurity unable to issue tokens. Error signing the token (requestUUID {})", uuid, e);
            throw new EzSecurityTokenException("EzSecurity unable to issue tokens at this time");
        } catch (IOException e) {
            log.info("Caught an IOException trying to serialize the token for signing", e);
            throw new EzSecurityTokenException("EzSecurity unable to issue tokens at this time");
        }

        return requestToken;
    }


    @Override
    public boolean isUserInvalid(EzSecurityToken ezSecurityToken, String userId) throws EzSecurityTokenException {
        EzSecurityContext.setUp();
        log.info("checkIsValidUser: from App: {}, DN: {} (requestUUID {})", ezSecurityToken.getValidity().getIssuedTo(),
                userId, EzSecurityContext.getRequestId());

        try {
            EzSecurityTokenUtils.verifyReceivedToken(crypto, ezSecurityToken, mySecurityId);
        } catch (TokenExpiredException e) {
            throw new EzSecurityTokenException("Received token was expired: " + e.getMessage());
        }

        boolean isValidUser =  uaservice.assertUserStrictFailure(userId);

        log.info("checkIsValidUser: user is valid {} (requestUUID {})", isValidUser, EzSecurityContext.getRequestId());
        return isValidUser;
    }

    @Override
    public Set<String> getAuthorizations(EzSecurityToken token, TokenType userType, String userId) throws EzSecurityTokenException, UserNotFoundException {
        if (new ThriftConfigurationHelper(ezConfiguration).useSSL()) {
            EzX509 peer = new EzX509();
            try {

                if (!peer.getPeerSecurityID().startsWith("_Ez_")) {
                    throw new EzSecurityTokenException("getAuthorizations is restricted to _Ez_ infrastructure apps");
                }
            } catch (ThriftPeerUnavailableException e) {
                throw new EzSecurityTokenException("thrift.use.ssl is true, but peer DN unavailable");
            }
        }

        AuditEvent event = AuditEvent.event("getAuthorizations", token);
        event.arg("TokenType", userType.toString()).arg("UserID", userId);

        Set<String> auths;
        Collection<String> externalAuths;
        try {
            if (userType == TokenType.APP) {
                AppInstance app = fetchApplication(userId);
                auths = authorizationPolicy.authorizationsForApp(app.getRegistration());
                externalAuths = app.getRegistration().getCommunityAuthorizations();
            } else {
                User user = fetchUser(userId);
                auths = authorizationPolicy.authorizationsForUser(user);
                externalAuths = authorizationPolicy.externalCommunityAuthorizationsForUser(user);
            }
            if (externalAuths != null && externalAuths.size() > 0) {
                auths.addAll(externalAuths);
            }
        } catch (AppNotRegisteredException | UserNotFoundException e) {
            log.error("Failed to get authorizations for {}:{}. {}", userType, userId, e.getMessage());
            throw new UserNotFoundException("Failed to find user type: "+ userType + " id: " + userId);
        } finally {
            auditLogger.logEvent(event);
        }
        return auths;
    }

    /**
     * Method only open to the EzFrontend, requests made by any other client will fail with an exception
     *
     * @param request
     * @return
     * @throws UserNotFoundException
     * @throws EzSecurityTokenException
     * @throws TException
     */
    @Override
    public ProxyTokenResponse requestProxyToken(ProxyTokenRequest request) throws TException {
        EzSecurityContext.setUp();

        String dn = request.getX509().getSubject();
        String requester = request.getValidity().getIssuer();
        log.info("requestUserDN : from App: {}, DN: {} (requestUUID {})", requester, dn,
                EzSecurityContext.getRequestId());

        ProxyTokenResponse response = new ProxyTokenResponse();
        try {
            AppInstance registration = fetchApplication(SecurityID.ReservedSecurityId.EFE.getId());

            // Validate token request - must be called. it throws if the token is not valid
            validateProxyRequest(request, registration.getCrypto());

            // assert the user is valid
            boolean valid = uaservice.assertUser(dn);
            log.info("DN request received user info for: {} from the user service. valid: {} (requestUUID {})", dn,
                    valid, EzSecurityContext.getRequestId());
            if (!valid) {
                throw new UserNotFoundException("The user: " + dn + " is not valid");
            }

            // issue the principal token
            ProxyUserToken token = new ProxyUserToken(
                    request.getX509(),
                    "EzSecurity",
                    SecurityID.ReservedSecurityId.EFE.getCn(),
                    getProxyTokenNotAfter());

            response.setToken(EzSecurityTokenUtils.serializeProxyUserTokenToJSON(token));
            response.setSignature(EzSecurityTokenUtils.proxyUserTokenSignature(token, crypto));

        } catch (PKeyCryptoException e) {
            log.info("EzSecurity unable to sign DN response, sorry (requestUUID {})",
                    EzSecurityContext.getRequestId(), e);
            throw new EzSecurityTokenException("Unable to sign response object, sorry");
        } catch (TokenSignatureVerificationException e) {
            log.info("Unable to verify signature provided by app: {} (requestUUID {})", requester,
                    EzSecurityContext.getRequestId());
            throw new EzSecurityTokenException("Unable to verify signature provided by app: " + requester);
        } catch (TokenExpiredException e) {
            log.info("Request for DN: {} failed because TokenRequest was expired (requestUUID {})",
                    dn, EzSecurityContext.getRequestId());
            throw new EzSecurityTokenException("Request for DN: '" + dn +
                    "' failed because TokenRequest was expired");
        } catch (UserNotFoundException e) {
            log.info("Request for DN: {} failed because User was not found (requestUUID {})",
                    dn, EzSecurityContext.getRequestId());
            throw new EzSecurityTokenException("Request for DN: '" + dn +
                    "' failed because User was not found");
        } catch (AppNotRegisteredException e) {
            log.info("Application {} ({}) not registered with Security service. Unable to verify requestUserDN " +
                    "request (requestUUID {})", SecurityID.ReservedSecurityId.EFE.getId(),
                    SecurityID.ReservedSecurityId.EFE.getCn(), EzSecurityContext.getRequestId());
            throw new EzSecurityTokenException("Application " + SecurityID.ReservedSecurityId.EFE.getCn() +
                    " not registered!");
        }

        return response;
    }

    /**
     * Instead of issuing a thrift security token, return the token info as JSON. This should be used as little as
     * possible
     *
     * @param request
     * @param signature
     * @return
     * @throws TException
     */
    @Override
    public EzSecurityTokenJson requestUserInfoAsJson(TokenRequest request, String signature) throws TException {
        EzSecurityContext.setUp();
        UUID uuid = EzSecurityContext.getRequestId();

        if (!request.isSetPrincipal()) {
            throw new EzSecurityTokenException("Unable to get user principal from token request");
        }
        EzSecurityPrincipal principal = request.getPrincipal();
        String id = principal.getPrincipal();

        log.info("requestUserInfoAsJson : from App: {}, DN: {}, targetApp: {} (requestUUID {})",
                request.getSecurityId(), id, request.getTargetSecurityId(), uuid);

        // Restrict this function to the ezfrontend
        EzSecurityTokenJson tokenJson;
        try {
            EzSecurityToken token = requestToken(request, signature);
            tokenJson = jsonProvider.getTokenJSON(token, crypto);
        } catch (AppNotRegisteredException e) {
            String requester = (request.getCaveats() != null) ? request.getCaveats().getIssuer() : request.getSecurityId();
            log.error("Can not issue token JSON. App {} does not exist (requestUUID {})", requester, uuid);
            throw new EzSecurityTokenException(e.getMessage());
        } catch (Exception e) {
            log.error("Error converting EzSecurityToken into a JSON string. JSON provider: {}", jsonProvider, e);
            throw new EzSecurityTokenException("Error getting JSON representation. Cause: " + e.getMessage());
        } finally {
            EzSecurityContext.cleanUp();
        }

        log.info("Returning token json (requestUUID {})", uuid);
        return tokenJson;
    }


    /**
     * Retrieve a user from the user attribute service
     *
     * @param dn the user's id from the external service
     * @return
     * @throws UserNotFoundException
     */
    private User fetchUser(String dn) throws UserNotFoundException {
        User info;

        try {
            log.info("requesting user info for DN({}) from backend service (requestUUID {})", dn,
                    EzSecurityContext.getRequestId());

            // Get the user from the backing service
            info = uaservice.getUser(dn);
            if (info != null) {
                // Get the user's authorizations
                Set<String> auths = authorizationPolicy.authorizationsForUser(info);
                info.getAuthorizations().setAuths(auths);

                // Sort some things
                for (Community c : info.getCommunities()) {
                    Collections.sort(c.getGroups());
                    Collections.sort(c.getRegions());
                    Collections.sort(c.getTopics());
                }

            } else {
                throw new IOException("User not found in database: " + dn);
            }
        } catch (IOException i) {
            log.info("user info for DN({}) not returned from backend service (requestUUID {})", dn,
                    EzSecurityContext.getRequestId());
            throw new UserNotFoundException("Error finding user with DN("+dn+")");
        } catch (ezbake.security.api.ua.UserNotFoundException e1) {
            throw new UserNotFoundException("User not found: " + e1.getMessage());
        }

        // Check if the user is an admin (do this here in case the admins file changes while it's cached
        if (adminService.isAdmin(dn)) {
            // Only add the project if it didn't exist in the user service
            if (!info.getProjects().containsKey(EzSecurityConstant.EZ_INTERNAL_PROJECT)) {
                info.getProjects().put(
                        EzSecurityConstant.EZ_INTERNAL_PROJECT,
                        Arrays.asList(EzSecurityConstant.EZ_INTERNAL_ADMIN_GROUP)
                );
                auditLogger.log("[{}] SUCCESS [app=EzSecurity] [user={}] [message=\"event\": \"assignEzAdmin\"]",
                        AuditEventType.PrivilegeRoleEscalation, dn);
            }
        }

        return info;
    }


    /**
     * Retrieve an application's registration from the registration service
     *
     * @param id the application's id
     * @return the application registration instance
     * @throws AppNotRegisteredException
     */
    private AppInstance fetchApplication(String id) throws AppNotRegisteredException {
        if (SecurityID.ReservedSecurityId.isReserved(id)) {
            id = SecurityID.ReservedSecurityId.fromEither(id).getCn();
        }
        log.info("Looking up app registration for {}", id);
        AppInstance registration = clientLookup.getClient(id);

        // Try looking up using the legacy id number
        if (registration == null && SecurityID.ReservedSecurityId.isReserved(id)) {
            String reservedId = id;
            id = SecurityID.ReservedSecurityId.fromEither(id).getId();
            log.info("Reserved app {} not found. Looking up app registration for {}", reservedId, id);
            registration = clientLookup.getClient(id);
        }

        if (registration == null) {
            throw new AppNotRegisteredException(MessageFormatter.format(
                    "EzSecurity doesn't have record of application {}. You must register if you would like to " +
                    "use EzSecurity", id).getMessage()
            );
        }

        auditLogger.log("[{}] SUCCESS [app=EzSecurity] [message=\"event\": \"readAppRegistration\", securityId=\"{}\"]",
                AuditEventType.FileObjectAccess, id);

        // Compute the apps auths
        registration.setAuthorizations(authorizationPolicy.authorizationsForApp(registration.getRegistration()));

        return registration;
    }

    /**
     * Check all the signatures of things that are expected to be signed by EzSecurity
     * @param request
     * @param issuer
     * @throws EzSecurityTokenException
     */
    private void validateRequestSignatures(TokenRequest request, String issuer) throws EzSecurityTokenException {
        if (ignoreSignedDn) {
            log.info("Not verifying DN signature for because server is in mock mode (requestUUID {})",
                    EzSecurityContext.getRequestId());
            return;
        }
        ProxyPrincipal proxyPrincipal = request.getProxyPrincipal();
        EzSecurityToken principalToken = request.getTokenPrincipal();
        EzSecurityPrincipal principal = request.getPrincipal();

        if (proxyPrincipal != null) {
            if (!SecurityID.ReservedSecurityId.isReserved(issuer) ||
                    SecurityID.ReservedSecurityId.EFE != SecurityID.ReservedSecurityId.fromEither(issuer)) {
                if (!EzSecurityTokenUtils.verifyProxyUserToken(proxyPrincipal.getProxyToken(), proxyPrincipal.getSignature(), crypto)) {
                    throw new EzSecurityTokenException("Unable to verify proxy user token");
                }
            }
        }
        if (principalToken != null) {
            if (!EzSecurityTokenUtils.verifyTokenSignature(principalToken, crypto)) {
                throw new EzSecurityTokenException("Unable to verify proxy user token");
            }
        }
        if (principal != null) {
            if (!SecurityID.ReservedSecurityId.isReserved(issuer) ||
                    SecurityID.ReservedSecurityId.EFE != SecurityID.ReservedSecurityId.fromEither(issuer)) {
                if (!EzSecurityTokenUtils.verifyPrincipalSignature(principal, crypto)) {
                    throw new EzSecurityTokenException("Unable to EzSecurityPrincipal signature user token");
                }
            }
        }
    }

    private long getTokenNotAfter() {
        return System.currentTimeMillis()+tokenTtlMillis;
    }

    private long getProxyTokenNotAfter() {
        return System.currentTimeMillis()+proxyTokenTtlMillis;
    }

    private void validateProxyRequest(ProxyTokenRequest request, PKeyCrypto crypto) throws TokenSignatureVerificationException, TokenExpiredException {
        // Check the timestamp
        if (validTimestamp(request.getValidity().getNotAfter())) {
            // verify the signature
            if (EzSecurityTokenUtils.verifyProxyTokenRequest(request, crypto)) {
                log.debug("Signature was verified...");
            } else {
                log.info("request signature was invalid. Public key doesn't match private (requestUUID {})",
                        EzSecurityContext.getRequestId());
                throw new TokenSignatureVerificationException("Failed to verify signature for application: " +
                        request.getValidity().getIssuer());
            }
        } else {
            log.info("request was expired. Received: {}, current{} (requestUUID {})", request.getValidity().getNotAfter(),
                    System.currentTimeMillis(), EzSecurityContext.getRequestId());
            throw new TokenExpiredException("Failed to process request because token with timestamp '" +
                    request.getValidity().getNotAfter() + "' is expired");
        }
        log.debug("The token request was valid");
    }

    private void validateTokenRequest(TokenRequest request, String signature, AppInstance registration) throws TokenExpiredException, TokenSignatureVerificationException {
        if (ignoreSignedDn) {
            log.info("Not verifying TokenRequest signature because server is in mock mode (requestUUID {})",
                    EzSecurityContext.getRequestId());
            return;
        }
        // Check the timestamp
        log.debug("Checking if token request is valid...");
        if (validTimestamp(request.getTimestamp())) {
            log.debug("Timestamp was valid...");
            // verify the signature
            if (signature != null && !signature.isEmpty() &&
                    EzSecurityTokenUtils.verifyTokenRequestSignature(request, signature, registration.getCrypto()))
            {
                log.debug("Signature was verified...");
            } else {
                log.info("request signature was invalid. Public key doesn't match private (requestUUID {})",
                        EzSecurityContext.getRequestId());
                throw new TokenSignatureVerificationException("Failed to verify signature for application: " +
                        request.getSecurityId());
            }
        } else {
            log.info("request was expired. Received: {}, current{} (requestUUID {})", request.getTimestamp(),
                    System.currentTimeMillis(), EzSecurityContext.getRequestId());
            throw new TokenExpiredException("Failed to process request because token with timestamp '" +
                    request.getTimestamp() + "' is expired");
        }
        log.debug("The token request was valid");
    }

    /**
     * Utility method to check if a timestamp is too old
     * @param timestamp timestamp to check
     * @return true if the timestamp is valid, false otherwise
     */
    protected boolean validTimestamp(long timestamp) {
        long now = System.currentTimeMillis();

        // timestamp is not older than expiration, and is not newer than now
        return (timestamp > now - requestExpirationMillis) && (timestamp < (now + wiggleMillis));
    }

    private boolean isFirstRequest(TokenRequest request) {
        return !request.isSetTokenPrincipal() && !request.isSetProxyPrincipal() && !request.isSetPrincipal();
    }

    private EzSecurityPrincipal getPrincipalFromRequest(TokenRequest request) throws IOException, PKeyCryptoException, EzSecurityTokenException {
        EzSecurityPrincipal principal;
        if (request.isSetPrincipal()) {
            principal = new EzSecurityPrincipal(request.getPrincipal());
        } else if (request.isSetTokenPrincipal()) {
            principal = new EzSecurityPrincipal(request.getTokenPrincipal().getTokenPrincipal());
        } else if (request.isSetProxyPrincipal()) {
            ProxyUserToken put = EzSecurityTokenUtils.deserializeProxyUserToken(request.getProxyPrincipal().getProxyToken());

            principal = new EzSecurityPrincipal();
            principal.setPrincipal(put.getX509().getSubject());
            principal.setIssuer(put.getX509().getIssuer());
            principal.setValidity(EzSecurityTokenUtils.generateValidityCaveats(put.getIssuedBy(), put.getIssuedTo(), put.getNotAfter()));
            principal.getValidity().setIssuedFor(put.getIssuedFor());
            principal.getValidity().setNotBefore(put.getNotBefore());
            principal.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(principal, crypto));
        } else if (request.type == TokenType.APP) {
            principal = new EzSecurityPrincipal(request.getSecurityId(), EzSecurityTokenUtils.generateValidityCaveats(
                    "EzSecurity",
                    request.getSecurityId(),
                    getProxyTokenNotAfter()));
            principal.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(principal, crypto));
        } else {
            throw new EzSecurityTokenException("No principal available in request!");
        }
        return principal;
    }

    private ValidityCaveats getValidityCaveatsFromRequest(TokenRequest request) {
        ValidityCaveats caveats;
        if (request.isSetTokenPrincipal()) {
            caveats = request.getTokenPrincipal().getValidity();
        } else {
            caveats = EzSecurityTokenUtils.generateValidityCaveats(request.getSecurityId(),
                    request.getTargetSecurityId(), getTokenNotAfter());
        }
        return caveats;
    }

    /**
     * Pull out the authorizations from the token request, if present. These will be present when the token is passed
     * in the request
     *
     * @param request a token request, may or may not contain an EzSecurityToken
     * @return a set of auths that should be used as a filter, or null if not present
     */
    private Set<String> getAuthFilterFromTokenRequest(TokenRequest request) {
        Set<String> preAuthFilter = null;
        if (request.isSetTokenPrincipal()) {
            preAuthFilter = request.getTokenPrincipal().getAuthorizations().getFormalAuthorizations();
        }
        return preAuthFilter;
    }


    protected Set<String> filterCommunityAuthorizations(Set<String> auths, Set<String> appAuths, Set<String> targetAppAuths,
                                                        List<String> chain) throws AppNotRegisteredException {

        // Set up the filter by intersecting all apps
        if (targetAppAuths != null) {
            appAuths.retainAll(targetAppAuths);
        }
        if (chain != null) {
            for (String securityIdLink : chain) {
                appAuths.addAll(Sets.newHashSet(fetchApplication(securityIdLink).getRegistration().getCommunityAuthorizations()));
            }
        }

        auths = Sets.intersection(auths, appAuths);
        return Sets.filter(auths, Predicates.and(Predicates.notNull(), Predicates.not(Predicates.equalTo(""))));
    }

    protected Set<String> filterAuthorizations(Set<String> auths, Set<String> appAuths, Set<String> targetAppAuths,
                                               Set<String> preFiltered, List<String> chain
    ) throws AppNotRegisteredException {

        if (preFiltered != null) {
            appAuths = Sets.intersection(appAuths, preFiltered);
        } else if (chain != null && !chain.isEmpty()) {
            appAuths = getAuthorizationsFromChain(Sets.newHashSet(chain), appAuths);
        }

        auths = Sets.intersection(auths, appAuths);
        if (targetAppAuths != null) {
            auths = Sets.intersection(auths, targetAppAuths);
        }

        return Sets.filter(auths, Predicates.and(Predicates.notNull(), Predicates.not(Predicates.equalTo(""))));
    }

    /**
     *
     * @param chain
     * @param initialSet
     * @return
     * @throws AppNotRegisteredException
     */
    protected Set<String> getAuthorizationsFromChain(Set<String> chain, Set<String> initialSet) throws AppNotRegisteredException {
        Set<String> auths = initialSet;
        AppInstance currApp;

        if(auths == null) {
            auths = new TreeSet<>();
        }
        for(String securityIdLink : chain) {
            currApp = fetchApplication(securityIdLink);
            auths.retainAll(currApp.getAuthorizations());
        }

        return auths;
    }


    private void updatePrincipalValidity(EzSecurityPrincipal principal, String issueTo) throws IOException, PKeyCryptoException {
        principal.setValidity(EzSecurityTokenUtils.generateValidityCaveats(issueTo, null, getProxyTokenNotAfter()));
        principal.getValidity().setSignature(EzSecurityTokenUtils.principalSignature(principal, crypto));
    }

    private void updateToken(EzSecurityToken token, ValidityCaveats caveats, EzSecurityPrincipal principal) throws IOException, PKeyCryptoException {
        caveats.setNotAfter(getTokenNotAfter());
        token.setValidity(caveats);
        token.setTokenPrincipal(principal);
        token.getValidity().setSignature(EzSecurityTokenUtils.tokenSignature(token, crypto));
    }

    private void updateRefreshToken(EzSecurityToken token) throws IOException, PKeyCryptoException {
        String issuedTo = token.getValidity().getIssuedTo();

        token.getValidity().setNotAfter(getTokenNotAfter());
        updatePrincipalValidity(token.getTokenPrincipal(), issuedTo);
        token.getValidity().setSignature(EzSecurityTokenUtils.tokenSignature(token, crypto));
    }

    /**
     * Updates the Chain
     * @param securityId
     * @param targetSecurityId
     * @param principal
     */
    public static void updateChain(String securityId, String targetSecurityId, EzSecurityPrincipal principal) {
        log.debug("update chain {} {}", securityId, targetSecurityId);
        if(principal.getRequestChain() == null) {
            principal.setRequestChain(Lists.<String>newArrayList());
        }
        String lastElm = null;

        if(!principal.getRequestChain().isEmpty()) {
            lastElm = principal.getRequestChain().get(principal.getRequestChain().size()-1);
        } else {
            if(securityId != null) {
                principal.getRequestChain().add(securityId);
            }
            if(targetSecurityId != null && !targetSecurityId.equals(securityId)) {
                principal.getRequestChain().add(targetSecurityId);
            }
            log.debug("updated chain {}", principal.getRequestChain());
            return;
        }

        if(lastElm != null && securityId != null && !lastElm.equals(securityId)) {
            principal.getRequestChain().add(securityId);
        }

        if(lastElm != null && targetSecurityId != null && !lastElm.equals(targetSecurityId)) {
            principal.getRequestChain().add(targetSecurityId);
        }
        log.debug("updated chain {}", principal.getRequestChain());
    }

    private boolean hasAppAccess(String fromApp, String toAppName) {

        Set<Long> fromAppAuths = ezGroups.getAuthorizations(Collections.<String>emptyList(), TokenType.APP, fromApp, null);
        Set<Long> toAppMask = ezGroups.getAppAccessGroup(toAppName);

        return toAppMask.size() <= 0 || Sets.intersection(toAppMask, fromAppAuths).size() > 0;
    }
}
