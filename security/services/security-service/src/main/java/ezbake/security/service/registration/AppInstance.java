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

package ezbake.security.service.registration;

import ezbake.crypto.utils.CryptoUtil;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.RSAKeyCrypto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;

/**
 * User: jhastings
 * Date: 11/18/13
 * Time: 3:23 PM
 */
public class AppInstance {
    private static final Logger logger = LoggerFactory.getLogger(AppInstance.class);

    private AppPersistenceModel registration;
    private Set<String> authorizations;
    private PKeyCrypto crypto;

    public AppInstance() {
        this.registration = new AppPersistenceModel();
    }
    public AppInstance(AppPersistenceModel registration) {
        this.registration = registration;
        initCrypto();
    }
    public String getId() {
        return registration.getId();
    }
    public String getOwner() {
        return registration.getOwner();
    }
    public RegistrationStatus getStatus() {
        return registration.getStatus();
    }

    public AppPersistenceModel getRegistration() {
        return registration;
    }
    public void setRegistration(AppPersistenceModel registration) {
        if (registration != null) {
            this.registration = registration;
        } else {
            this.registration = new AppPersistenceModel();
        }
        initCrypto();
    }

    protected void initCrypto() {
        if (registration != null && registration.getPublicKey() != null) {
            try {
                this.crypto = new RSAKeyCrypto(CryptoUtil.stripPEMString(registration.getPublicKey()), false);
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                // won't have a crypto. Can't do anything fun
            }
        }
    }

    public void setCrypto(PKeyCrypto crypto) {
        this.crypto = crypto;
    }

    public boolean hasCrypto() {
        return this.crypto != null;
    }
    public PKeyCrypto getCrypto() {
        return crypto;
    }

    public void setAuthorizations(Set<String> authorizations) {
        if (authorizations == null) {
            authorizations = Collections.emptySet();
        }
        this.authorizations = new HashSet<String>(authorizations);
    }

    public Set<String> getAuthorizations() {
        return (authorizations == null) ? Collections.<String>emptySet() : authorizations;
    }

    public List<String> getAuthorizationsList() {
        return (authorizations == null)? Collections.<String>emptyList() : new ArrayList<String>(authorizations);
    }

}
