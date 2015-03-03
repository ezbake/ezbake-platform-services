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

package ezbake.security.service.registration.handler;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.common.properties.EzProperties;
import ezbake.ezca.EzCA;
import ezbake.ezca.ezcaConstants;
import ezbake.persist.EzPersistUtil;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.*;
import ezbake.crypto.utils.EzSSL;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbake.thrift.authentication.EzX509;
import ezbake.thrift.authentication.ThriftPeerUnavailableException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.net.util.Base64;
import org.apache.thrift.TException;

import ezbake.thrift.ThriftClientPool;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;

/**
 */
public class EzSecurityRegistrationHandler extends EzBakeBaseThriftService implements EzSecurityRegistration.Iface {
    private static final Logger log = LoggerFactory.getLogger(EzSecurityRegistrationHandler.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(EzSecurityRegistrationHandler.class);

    public static final String APP_CERTS_ONLY_DEPLOYER = "ezbake.security.registration.certs.only.deployer";
    public static final String SECURITY_ENCRYPT = "ezbake.security.certs.encrypt";
    public static final String PKEY_MODE = "ezbake.security.registration.pkey.mode";
    public static final String PkeyDevMode = "dev";
    private List<EzSecurityRegistrationHandler> injectedHandlers = new ArrayList<>();

    private EzProperties ezConfiguration;
    private RegistrationManager regManager;
    private EzbakeSecurityClient securityClient;
    private ThriftClientPool clientPool;
    private String passphrase;
    
    private boolean encryptPk;
    /**
     * This constructor should only be used by ThriftRunner
     */
    public EzSecurityRegistrationHandler() {
        this.ezConfiguration = null;
        this.regManager = null;
        this.clientPool = null;
        this.securityClient = null;
        this.encryptPk = false;
    }

    public EzSecurityRegistrationHandler (Properties properties) throws AccumuloException, AccumuloSecurityException, PKeyCryptoException, IOException {
        ezConfiguration = new EzProperties(properties, true);
        regManager = new AccumuloRegistrationManager(properties);
        clientPool = new ThriftClientPool(ezConfiguration);
        securityClient = new EzbakeSecurityClient(ezConfiguration);
        this.passphrase = calculatePassphrase(getCrypto(properties));
        this.encryptPk = Boolean.parseBoolean(properties.getProperty(SECURITY_ENCRYPT));
    }

    @Inject
    public EzSecurityRegistrationHandler(Properties configuration, RegistrationManager regManager, ThriftClientPool clientPool) throws IOException, PKeyCryptoException {
        this.ezConfiguration = new EzProperties(configuration, true);
        this.regManager = regManager;
        this.clientPool = clientPool;
        this.securityClient = new EzbakeSecurityClient(configuration, clientPool);
        this.passphrase = calculatePassphrase(getCrypto(configuration));
        this.encryptPk = Boolean.parseBoolean(configuration.getProperty(SECURITY_ENCRYPT));
    }


    public static EzSecurityRegistrationHandler getHandler(Properties configuration) {
        Injector injector = Guice.createInjector(new HandlerModule(configuration));
        return injector.getInstance(EzSecurityRegistrationHandler.class);
    }


    @Override
    public TProcessor getThriftProcessor() {
        EzSecurityRegistrationHandler handler = getHandler(getConfigurationProperties());
        injectedHandlers.add(handler);

        return new EzSecurityRegistration.Processor<>(handler);
    }


    @Override
    public void shutdown() {
        log.info("Shutting down all EzSecurithHandler instances. Instances to stop: {}", injectedHandlers.size());
        if (injectedHandlers != null) {
            Iterator<EzSecurityRegistrationHandler> handlers = injectedHandlers.iterator();
            while(handlers.hasNext()) {
                EzSecurityRegistrationHandler handler = handlers.next();
                // TODO: Add shutdown code
                handlers.remove();
            }
        }
    }


    @Override
    public boolean ping() {
        log.info("Received ping");
        return true;
    }


    public String registerApp(EzSecurityToken ezToken, String appName, String level,
                              List<String> authorizations, String id,
                              Set<String> admins, String appDn) throws RegistrationException, SecurityIDExistsException {
        return registerApp(ezToken, appName, level, authorizations, null, id, admins, appDn);
    }

    @Override
    public String registerApp(EzSecurityToken ezToken, String appName, String level,
                              List<String> authorizations, List<String> communityAuthorizations, String id,
                              Set<String> admins, String appDn) throws RegistrationException, SecurityIDExistsException {
        log.info("registerApp - requester: {}, registration name: {}, classification: {}, auths: {}, id: {}",
                ezToken.getValidity().getIssuedTo(), appName, level, authorizations, id);
        validateRequest(ezToken);

        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectCreate.name(), ezToken).arg("Security Id", id);
        try {

            String owner = ezToken.getTokenPrincipal().getPrincipal();
            String[] auths = authsFromToken(ezToken);

            if (id == null || id.isEmpty()) {
                // Generate a security ID if one not passed
                id = getGeneratedSecurityId(auths);
                log.info("generated a new Security ID: {} for a new application: {}", id, appName);
            } else {
                // Check if the ID is reserved
                if (SecurityID.isReserved(id)) {
                    log.error("Request to register with reserved security ID: {}", id);
                    throw new RegistrationException("Unable to register applciation. Security ID " + id + " is reserved");
                }

                // See if the application is already registered
                try {
                    AppPersistenceModel registration = regManager.getRegistration(auths, id, null, null);
                    if (registration.getAppName().equals(appName)) {
                        // Application must have already been registered
                        log.info("Application already registered: {} - {}", appName, id);
                        throw new SecurityIDExistsException("Application already registered");
                    } else {
                        // Another application registered already - generate a new security ID for this one
                        log.info("Application already registered with security ID: {}. Current appName: {}", id,
                                registration.getAppName());
                        id = getGeneratedSecurityId(auths);
                        log.info("Using generated security id: {} for application: {}", id, appName);
                    }
                } catch (SecurityIDNotFoundException e) {
                    // that's good - we can use this security ID
                    log.info("Security ID was not found");
                }
            }

            // Make sure the owner is added as an admin
            if (admins != null && !admins.contains(owner)) {
                admins.add(owner);
            } else {
                admins = Sets.newHashSet(owner);
            }

            // Build the app persistence model and register
            AppPersistenceModel model = new AppPersistenceModel();
            model.setId(id);
            model.setOwner(owner);
            model.setAppName(appName);
            model.setAppDn(appDn);
            model.setAuthorizationLevel(level);
            model.setFormalAuthorizations(authorizations);
            model.setCommunityAuthorizations(communityAuthorizations);
            model.setAdmins(admins);
            regManager.register(model);
        } catch (Exception e) {
            event.failed();
            throw e;
        }  finally {
            auditLogger.logEvent(event);
        }

        return id;
    }

    @Override
    public void promote(EzSecurityToken ezToken, String id) throws RegistrationException, PermissionDeniedException, SecurityIDNotFoundException {
        log.info("promote - requester: {}, registration id: {}", ezToken.getValidity().getIssuedTo(), id);
        validateRequest(ezToken, true);

        final String[] auths = authsFromToken(ezToken);
        AppPersistenceModel app = regManager.getRegistration(auths, id, null, null);

        EzCA.Client client = null;
        
        AuditEvent eventCert = AuditEvent.event(AuditEventType.FileObjectCreate.name(), ezToken).arg("Security Id", id);
        AuditEvent eventMod = AuditEvent.event(AuditEventType.FileObjectModify.name(), ezToken).arg("Security Id", id);
        
        try {
            if (Strings.isNullOrEmpty(app.getX509Cert())) {
                // Generate the private key, and CSR
                log.info("promote - generating private key for id: {}", id);
                RSAKeyCrypto crypto = new RSAKeyCrypto();
                String privateKey = crypto.getPrivatePEM();
                String csr = crypto.getCSR("CN=" + id);

                log.info("promote - invoking CA {} to generate a certificate for {}", ezcaConstants.SERVICE_NAME, id);
                client = clientPool.getClient(ezcaConstants.SERVICE_NAME, EzCA.Client.class);

                String cert = client.csr(ezToken, csr);

                log.info("promote - Received Certificate From EzCA uploading to database id: {} ", id);
                AppPersistenceModel update = new AppPersistenceModel();
                
                if(this.encryptPk) {
                    log.info("Ez Security Registration is encrypting the pk.");
                    update.setEncrypting(true);
                    update.setPasscode(this.passphrase);
                    
                    log.info("Using passphrase {}", this.passphrase);
                }
                
                update.setId(id);
                update.setPrivateKey(privateKey);
                update.setPublicKey(crypto.getPublicPEM());
                update.setX509Cert(cert);
                regManager.update(update, null);
            }

        
            log.info("promote - promoting id: {} to active status", id);
            
            try {
                regManager.approve(authsFromToken(ezToken), id);
            }
            catch(Exception e) {
                eventMod.failed();
                throw e;
            }
            finally {
                auditLogger.logEvent(eventMod);
            }
        }
        catch (TException e) {
            eventCert.failed();
            log.error("Thrift Exception thrown calling out to CA service {}", ezcaConstants.SERVICE_NAME, e);
            throw new RegistrationException("Error Invoking External Thrift Service " + ezcaConstants.SERVICE_NAME +
                    " To Obtain Certificate");
        } catch (AppPersistCryptoException e) {
            eventCert.failed();
            log.error("Exception encrypting the app private key", e);
            throw new RegistrationException("Unable to encrypt the application private key");
        } finally {
            auditLogger.logEvent(eventCert);
            clientPool.returnToPool(client);
        }
    }

    @Override
    public void update(EzSecurityToken ezToken, ApplicationRegistration appReg) throws RegistrationException, SecurityIDNotFoundException {
        log.info("update - requester: {}, registration id: {}", ezToken.getValidity().getIssuedTo(), appReg.id);
        validateRequest(ezToken);
        AppPersistenceModel appModel = AppPersistenceModel.fromExternal(appReg);
        
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectModify.name(), ezToken).arg("Security Id", appReg.id);
        
        try {
            regManager.update(appModel, RegistrationStatus.PENDING);
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void denyApp(EzSecurityToken ezToken, String id) throws SecurityIDNotFoundException, PermissionDeniedException, RegistrationException  {
        log.info("denyApp - requester: {}, registration id: {}", ezToken.getValidity().getIssuedTo(), id);
        validateRequest(ezToken, true);

        Object[] objs = ezToken.getAuthorizations().getFormalAuthorizations().toArray();
        String[] auths = Arrays.copyOf(objs, objs.length, String[].class);

        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectModify.name(), ezToken).arg("Security Id", id);
        
        try {
            regManager.deny(auths, id);
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public void deleteApp(EzSecurityToken ezSecurityToken, String id) throws RegistrationException, SecurityIDNotFoundException, PermissionDeniedException {
        log.info("deleteApp - requester: {}, registration id: {}", ezSecurityToken.getValidity().getIssuedTo(), id);
        validateRequest(ezSecurityToken);
        String[] auths = authsFromToken(ezSecurityToken);

        AuditEvent eventDelete = AuditEvent.event(AuditEventType.FileObjectDelete.name(), ezSecurityToken).arg("Security Id", id);
        AuditEvent eventAccess = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezSecurityToken).arg("Application ID", id);
        
        // Get the registration as the owner
        AppPersistenceModel app;
        try {
            app = regManager.getRegistration(auths, id, ezSecurityToken.getTokenPrincipal().getPrincipal(), null);
        } catch (SecurityIDNotFoundException e) {
            eventAccess.failed();
            // If the token is an admin, then try as admin
            if (EzSecurityTokenUtils.isEzAdmin(ezSecurityToken)) {
                app = regManager.getRegistration(auths, id, null, null);
            } else {
                throw e;
            }
        }
        finally {
            auditLogger.logEvent(eventAccess);
        }


        try {
            if (app.getStatus() == RegistrationStatus.ACTIVE) {
                throw new PermissionDeniedException("Unable to delete an active application");
            }
            regManager.delete(auths, id);
        }
        catch(Exception e) {
            eventDelete.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(eventDelete);
        }
        
        
    }

    @Override
    public void demote(EzSecurityToken ezToken, String id) throws SecurityIDNotFoundException, RegistrationException, PermissionDeniedException {
        log.info("demote - requester: {}, registration id: {}", ezToken.getValidity().getIssuedTo(), id);
        validateRequest(ezToken, true);
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectModify.name(), ezToken).arg("Security Id", id);
        
        try {
            regManager.setStatus(id, RegistrationStatus.PENDING);
        }
        catch(Exception e) {
            event.failed();
            
        }
        finally {
            auditLogger.logEvent(event);
        }
    }

    @Override
    public ApplicationRegistration getRegistration(EzSecurityToken ezToken, String id) throws PermissionDeniedException, RegistrationException, SecurityIDNotFoundException {
        log.info("getRegistration - requester: {}, registration id: {}", ezToken.getValidity().getIssuedTo(), id);
        validateRequest(ezToken);

        String owner = ezToken.getTokenPrincipal().getPrincipal();
        String requester = ezToken.getValidity().getIssuedTo();
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken).arg("Security Id", id);
        
        if (EzSecurityTokenUtils.isEzAdmin(ezToken)) {
            owner = null;
        } else if (SecurityID.ReservedSecurityId.isReserved(requester)) {
            owner = null;
        } else if (ezToken.type == TokenType.APP) {
            // Check if the token was issued to the app itself, if so allow
            if (owner != null && owner.equals(id)) {
                owner = null;
            }
        }

        AppPersistenceModel appReg = null;
        try {
            log.debug("Invoking getReg with {}",  owner);
            appReg  = regManager.getRegistration(authsFromToken(ezToken), id, owner, null);
        
            if(appReg == null) {
                throw new PermissionDeniedException("You are not in the Administration Group for this Registration.");
            }
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
        
        return AppPersistenceModel.toExternal(appReg);
    }

    public List<ApplicationRegistration> getAllRegistrations(EzSecurityToken ezToken) throws RegistrationException, TException {
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken);
        List<ApplicationRegistration> registrations= null;
        try {
            registrations = getAllRegistrations(ezToken, null);
        }
        catch(Exception e) {
            event.failed();
        }
        finally {
            auditLogger.logEvent(event);
        }
        
        return registrations;
    }

    @Override
    public List<ApplicationRegistration> getAllRegistrations(EzSecurityToken ezToken, RegistrationStatus status) throws RegistrationException {
        log.info("getAllRegistrations - requester: {}, status: {}", ezToken.getValidity().getIssuedTo(), status);
        validateRequest(ezToken);
        String owner = ezToken.getTokenPrincipal().getPrincipal();
        AuditEvent event = AuditEvent.event(AuditEventType.AdminOrRootLevelAccess.name(), ezToken).arg("status", status);
        List<AppPersistenceModel> registrations = null;
        
        try {
        
            if (EzbakeSecurityClient.isEzAdmin(ezToken)) {
                owner = null;
            }
            registrations = regManager.all(authsFromToken(ezToken), owner, status);
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
        
        return AppPersistenceModel.generateExternalList(registrations);
    }

    @Override
    public List<ApplicationRegistration> getRegistrations(EzSecurityToken ezToken) throws RegistrationException {
        log.info("getRegistrations - requester: {}", ezToken.getValidity().getIssuedTo());
        validateRequest(ezToken);
        String owner = ezToken.getTokenPrincipal().getPrincipal();
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken);
        
        List<AppPersistenceModel> registrations = null;
        
        try {
            if (EzbakeSecurityClient.isEzAdmin(ezToken)) {
                owner = null;
            }
            registrations = regManager.all(authsFromToken(ezToken), owner, null);
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
        
        return AppPersistenceModel.generateExternalList(registrations);
    }

    @Override
    public RegistrationStatus getStatus(EzSecurityToken ezToken, String id) throws RegistrationException, SecurityIDNotFoundException {
        log.info("getStatus - requester: {},  info for: {}", ezToken.getValidity().getIssuedTo(), id);
        validateRequest(ezToken);
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken).arg("Security Id", id);
        RegistrationStatus status = null;
        
        try {
            status =regManager.getStatus(authsFromToken(ezToken), id);
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }
        
        return status; 
    }

    @Override
    public AppCerts getAppCerts(EzSecurityToken ezToken, String id) throws RegistrationException, SecurityIDNotFoundException {
        log.info("getAppSecurityInfo - requester: {},  info for: {}", ezToken.getValidity().getIssuedTo(), id);
        validatePrivilegedPeer(ezToken, new EzX509());
        AuditEvent event = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken).arg("", id);
        
        // Get actual application things
        AppPersistenceModel app = regManager.getRegistration(authsFromToken(ezToken), id, null, RegistrationStatus.ACTIVE);
        AppPersistenceModel ca;
        AppPersistenceModel ezSec;
        
        try {
 
            if (app == null) {
                throw new SecurityIDNotFoundException("Registration not found or inactive");
            }
      
            if(app.getSalt() != null) {
                app.setEncrypting(true);
                app.setPasscode(this.passphrase);
            }

            
            try {
                ca = regManager.getRegistration(authsFromToken(ezToken),
                        SecurityID.ReservedSecurityId.CA.getCn(), null, null);
            } catch (SecurityIDNotFoundException e ){
                // Try looking up by old style id
                ca = regManager.getRegistration(authsFromToken(ezToken),
                        SecurityID.ReservedSecurityId.CA.getId(), null, null);
            }

            
            try {
                ezSec = regManager.getRegistration(authsFromToken(ezToken),
                        SecurityID.ReservedSecurityId.EzSecurity.getCn(), null, null);
            } catch (SecurityIDNotFoundException e) {
                // Try looking up by old style id
                ezSec = regManager.getRegistration(authsFromToken(ezToken),
                        SecurityID.ReservedSecurityId.EzSecurity.getId(), null, null);
            }
        }
        catch(Exception e) {
            event.failed();
            throw e;
        }
        finally {
            auditLogger.logEvent(event);
        }

        try {
            return EzPersistUtil.getAppCerts(app, ca, ezSec);
        } catch (IOException e) {
            log.error("Failed loading App Certs", e);
            throw new RegistrationException("Failed to load App Certs - " + e.getMessage());
        }
    }

    /**
     * Generate a security ID
     * @return a random security ID
     */
    protected String generateSecurtyId() {
        byte[] bytes = new byte[64];
        new SecureRandom().nextBytes(bytes);
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        return String.valueOf(buff.getLong() & 0x7fffffffffffffffL);
    }

    /**
     * Try to generate a security ID, making sure that it doesn't already exist
     * Currently retries 5 times if it does exist, but then gives up
     * @param auths scan auths for determining whether or not the security ID exists
     * @return the generated security ID
     * @throws RegistrationException
     */
    protected String getGeneratedSecurityId(String[] auths) throws RegistrationException {
        String generated = generateSecurtyId();
        if (regManager.containsId(auths, generated)) {
            generated = null;
            int tries = 5;
            while (tries > 0) {
                String temp = generateSecurtyId();
                if (!regManager.containsId(auths, generated)) {
                    generated = temp;
                    break;
                }
                tries --;
            }
        }
        if (generated == null) {
            throw new RegistrationException("Unable to generate a unique security id for the registration");
        }
        return generated;
    }


    /**
     * Does the validation of the passed in EzSecurityToken, also makes sure it is a UserInfo token
     * @param token token to validate
     * @throws RegistrationException
     */
    private void validateRequest(EzSecurityToken token) throws RegistrationException {
        try {
            validateRequest(token, false);
        } catch (PermissionDeniedException e) {
            // Don't want to throw permission denied exception from every function... this should never happen,
            // so if it does, just throw generic exception
            throw new RegistrationException(e.getMessage());
        }
    }

    /**
     * Does the validation of the passed in EzSecurityToken, also makes sure it is a UserInfo token
     * @param token token to validate
     * @param requireAdmin whether to throw an exception if the user is not an admin
     * @throws RegistrationException
     */
    private void validateRequest(EzSecurityToken token, boolean requireAdmin) throws RegistrationException, PermissionDeniedException {
        // First validate the token
        try {
            securityClient.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            log.error("Rejecting request from user: {} - Received token failed validation",
                    token.getTokenPrincipal().getPrincipal());
            throw new RegistrationException("Received an invalid security token");
        }

        // Check for admin (if required)
        if(requireAdmin && !EzbakeSecurityClient.isEzAdmin(token)) {
            log.error("Rejecting request from user: {} - invoked method requires EzAdmin",
                    token.getTokenPrincipal().getPrincipal());
            throw new PermissionDeniedException("You do not have permission to invoke this method.");
        }
    }

    private void validatePrivilegedPeer(EzSecurityToken token, EzX509 x509) throws RegistrationException {
        validateRequest(token);
        if (ezConfiguration.getBoolean(APP_CERTS_ONLY_DEPLOYER, true)) {
            // This should only be called by the deployer
            String tokenSecurityId = token.getValidity().getIssuedTo();
            SecurityID.ReservedSecurityId deployer = SecurityID.ReservedSecurityId.Deployer;
            try {
                if (!deployer.getCn().equals(x509.getPeerSecurityID()) && !deployer.getCn().equals(tokenSecurityId)) {
                    throw new RegistrationException("Unauthorized call to getAppCerts");
                }
            } catch (ThriftPeerUnavailableException e) {
                throw new RegistrationException("Peer certificate information unavailable. Are you using mutually " +
                        "authenticated SSL?");
            }
        }
    }

    private String[] authsFromToken(EzSecurityToken token) {
        Set<String> auths = token.getAuthorizations().getFormalAuthorizations();
        if (auths == null || auths.size() == 0) {
            return new String[0];
        } else {
            return auths.toArray(new String[auths.size()]);
        }
    }
    
	@Override
	public void addAdmin(EzSecurityToken ezToken, String id, String admin)
			throws PermissionDeniedException, SecurityIDNotFoundException, RegistrationException {
		validateRequest(ezToken);
		String[] auths = authsFromToken(ezToken);

		regManager.addAdmin(auths, id, admin);
	}

	@Override
	public void removeAdmin(EzSecurityToken ezToken, String id, String admin)
			throws PermissionDeniedException, SecurityIDNotFoundException, RegistrationException {
		validateRequest(ezToken);
		regManager.removeAdmin(authsFromToken(ezToken), id, admin);
	}
	
	private String calculatePassphrase(PKeyCrypto crypto) throws PKeyCryptoException{
	    String cn = SecurityID.ReservedSecurityId.Registration.getCn();
	    byte[] signedData = crypto.sign(cn.getBytes());
	    return Base64.encodeBase64String(signedData);
	}

    private static PKeyCrypto getCrypto(Properties configuration) throws IOException {
        PKeyCrypto crypto;
        switch (configuration.getProperty(PKEY_MODE, "")) {
            case PkeyDevMode:
                crypto = new RSAKeyCrypto();
                log.warn("Private-key mode set to {}. If key encryption is done, keys will not be recoverable",
                        PkeyDevMode);
                break;
            default:
                crypto = EzSSL.getCrypto(configuration);
        }
        return crypto;
    }

}
