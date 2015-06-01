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

package ezbake.locksmith.service;

import com.google.inject.Guice;
import com.google.inject.Inject;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.locksmith.db.AbstractLocksmithManager;
import ezbake.security.lock.smith.thrift.KeyExistsException;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.locksmith.db.AesLocksmithManager;
import ezbake.locksmith.db.RsaLocksmithManager;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.KeyNotFoundException;
import ezbake.security.lock.smith.thrift.KeyType;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;

import java.io.IOException;
import java.util.*;

public class EzLocksmithHandler extends EzBakeBaseThriftService implements EzLocksmith.Iface {

    private static final Logger log = LoggerFactory.getLogger(EzLocksmithHandler.class);
    private static final AuditLogger auditLog = AuditLogger.getAuditLogger(EzLocksmithHandler.class);

    private AesLocksmithManager aesManager;
    private RsaLocksmithManager rsaManager;

    private EzbakeSecurityClient secureClient;


    @Inject
    public EzLocksmithHandler(EzbakeSecurityClient securityClient, AesLocksmithManager aesManager, RsaLocksmithManager rsaManager) {
        this.secureClient = securityClient;
        this.aesManager = aesManager;
        this.rsaManager = rsaManager;
    }

    /**
     * This constructor should only be used to instantiate the EzBakeBaseThriftService with getThriftProcessor.
     * It is to be invoked by thrift-runner
     */
    public EzLocksmithHandler() {
        this.secureClient = null;
        this.aesManager = null;
        this.rsaManager = null;
    }

    private List<EzLocksmithHandler> instances = new ArrayList<>();
    @Override
    public TProcessor getThriftProcessor() {
        EzLocksmithHandler instance = Guice.createInjector(new LocksmithModule(getConfigurationProperties()))
                .getInstance(EzLocksmithHandler.class);
        instances.add(instance);

        return new EzLocksmith.Processor<>(instance);
    }

    @Override
    public void shutdown() {
        log.info("Shutting down {}", EzLocksmithHandler.class.getSimpleName());
        Iterator<EzLocksmithHandler> it = instances.iterator();
        while (it.hasNext()) {
            it.next().stop();
            it.remove();
        }
    }

    public void stop() {
        try {
            secureClient.close();
        } catch (IOException e) {
            log.error("Failure to close the security client: {}", e.getMessage());
        }
        aesManager.close();
        rsaManager.close();
    }

    @Override
    public boolean ping() {
        log.info("Ping received");
        return true;
    }

    @Override
    public void generateKey(EzSecurityToken ezToken, String keyId, KeyType keyType, List<String> sharedWith) throws EzSecurityTokenException, KeyExistsException {
        log.info("Generate Key {}, {}, {}", keyId, keyType, ezToken.getValidity().getIssuedTo());
        secureClient.validateReceivedToken(ezToken);

        String owner = ezToken.getValidity().getIssuedTo();
        if (sharedWith == null) {
            sharedWith = Collections.emptyList();
        }
        
        AuditEvent auditEvnt = AuditEvent.event(AuditEventType.FileObjectCreate.name(), ezToken).arg("keyId", keyId).arg("keyType", keyType).arg("sharedWith", sharedWith);
        
        try {
            switch(keyType) {
                case RSA:
                    rsaManager.generateKey(keyId, owner, sharedWith.toArray(new String[sharedWith.size()]));
                    break;
                case AES:
                    aesManager.generateKey(keyId, owner, sharedWith.toArray(new String[sharedWith.size()]));
                    break;
            }
        } catch(Exception e) {
            log.error("Error {}", e);
            auditEvnt.failed();
            throw e;
        } finally {
            auditLog.logEvent(auditEvnt);
        }
        
        
        
    }

    @Override
    public void removeKey(EzSecurityToken ezToken, String keyId, KeyType keyType) throws EzSecurityTokenException, KeyNotFoundException {
        log.info("Remove Key {}, {}, {}", keyId, keyType, ezToken.getValidity().getIssuedTo());
        secureClient.validateReceivedToken(ezToken);
        String owner = ezToken.getValidity().getIssuedTo();
        
        AuditEvent auditEvnt = AuditEvent.event(AuditEventType.FileObjectDelete.name(), ezToken).arg("keyId", keyId).arg("keyType", keyType);
        try {
            switch(keyType) {
                case RSA:
                    rsaManager.removeKey(keyId, owner);
                    break;
                case AES:
                    aesManager.removeKey(keyId, owner);
                    break;
            }
        } catch(Exception e) {
            auditEvnt.failed();
            log.error("Error {}", e);
            throw e;
        } finally {
            auditLog.logEvent(auditEvnt);
        }
    }

    @Override
    public String retrieveKey(EzSecurityToken ezToken, String keyId, KeyType keyType) throws EzSecurityTokenException, KeyNotFoundException, KeyExistsException {
        log.info("Retrieve Key {}, {}, {}", keyId, keyType, ezToken.getValidity().getIssuedTo());
        secureClient.validateReceivedToken(ezToken);
        String owner = ezToken.getValidity().getIssuedTo();

        // Try to get the key
        AbstractLocksmithManager manager = (keyType == KeyType.RSA) ? rsaManager : aesManager;
        byte[] key;
        
        AuditEvent auditEvnt = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken).arg("keyId", keyId).arg("keyType", keyType);
        
        try {
            key = manager.getKey(keyId, owner);
        } catch (KeyNotFoundException e) {
            log.info("Key {}({}) not found. Generating a new one and assigning owner {}", keyId, keyType, owner);
            // Generate one
            key = manager.generateKey(keyId, owner);
            if (key == null) {
                auditEvnt.failed();
                log.error("Failed to generate a new {} key for {} named {}", keyType, owner, keyId);
                throw new KeyNotFoundException("Unable to generate a new key");
            }
        } finally {
            auditLog.logEvent(auditEvnt);
        }

        String result = null;
        switch(keyType) {
            case RSA:
                result = new String(key);
                break;
            case AES:
                result = Base64.encodeBase64String(key);
                break;
        }

        log.info("Retrieve key returning key {}", keyId);
        return result;
    }

    @Override
    public void uploadKey(EzSecurityToken ezToken, String keyId, String keyData, KeyType keyType) throws EzSecurityTokenException, KeyExistsException {
        log.info("Upload Key {}, {}, {}", keyId, keyType, ezToken.getValidity().getIssuedTo());
        secureClient.validateReceivedToken(ezToken);
        String owner = ezToken.getValidity().getIssuedTo();
        
        AuditEvent auditEvnt = AuditEvent.event(AuditEventType.FileObjectCreate.name(), ezToken).arg("keyId", keyId).arg("keyType", keyType);
        try {
            switch(keyType) {
                case RSA:
                    rsaManager.insertKey(keyId, keyData.getBytes(), owner);
                    break;
                case AES:
                    aesManager.insertKey(keyId, keyData.getBytes(), owner);
                    break;
            }
        } catch(Exception e) {
            auditEvnt.failed();
            log.error("Error {}", e);
            throw e;
        } finally {
            auditLog.logEvent(auditEvnt);
        }
    }

    @Override
    public String retrievePublicKey(EzSecurityToken ezToken, String keyId, String owner) throws EzSecurityTokenException, KeyExistsException {
        log.info("Retrieve Public Key {}, {}", keyId, ezToken.getValidity().getIssuedTo());
        secureClient.validateReceivedToken(ezToken);

        AuditEvent auditEvnt = AuditEvent.event(AuditEventType.FileObjectAccess.name(), ezToken).arg("keyId", keyId).arg("owner", owner);
        String key = null;
        try {
            key = rsaManager.getPublicKey(keyId);
        } catch(Exception e) {
            auditEvnt.failed();
            log.error("Error {}", e);
            throw e;
        } finally {
            auditLog.logEvent(auditEvnt);
        }
        return key;
    }


}
