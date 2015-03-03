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

package ezbake.security.persistence.model;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Sets;
import ezbake.crypto.PBECrypto;
import ezbake.persist.EzPersist;
import ezbake.persist.EzPersistBase;
import ezbake.security.common.core.SecurityID;
import ezbake.security.thrift.ApplicationRegistration;
import ezbake.security.thrift.RegistrationStatus;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;


/**
 * User: jhastings
 * Date: 4/24/14
 * Time: 8:23 AM
 */
public class AppPersistenceModel extends EzPersistBase {
    private static Logger log = LoggerFactory.getLogger(AppPersistenceModel.class);

    public enum APP_REG_FIELDS {
        NAME,
        OWNER,
        ADMINS,
        LEVEL,
        VISIBILITIES,
        COMMUNITY_AUTHS,
        STATUS,
        APP_DN,
        SALT,
        PUBLIC_KEY,
        PRIVATE_KEY,
        X509_CERT;
        public String getValue() {
            return toString().toLowerCase();
        }
        public byte[] getBytes() {
           return getValue().getBytes();
        }
        public static APP_REG_FIELDS fromString(String id) {
            if (id != null) {
                for (APP_REG_FIELDS sid : values()) {
                    if (sid.toString().equalsIgnoreCase(id)) {
                        return sid;
                    }
                }
            }
            throw new IllegalArgumentException("Unknown " + APP_REG_FIELDS.class.getSimpleName() + " value: " + id);
        }
    }
    enum APP_LOOKUP_FIELDS {
        OWNER,
        ADMIN;
        public String getValue() {
            return toString().toLowerCase();
        }
        public static APP_LOOKUP_FIELDS fromString(String id) {
            if (id != null) {
                for (APP_LOOKUP_FIELDS sid : values()) {
                    if (sid.toString().equalsIgnoreCase(id)) {
                        return sid;
                    }
                }
            }
            throw new IllegalArgumentException("Unknown " + APP_LOOKUP_FIELDS.class.getSimpleName() + " value: " + id);
        }
    }

    private String appDn;
    private String id;
    private String owner;
    private String appName;
    private String authorizationLevel;
    private List<String> formalAuthorizations;
    private List<String> communityAuthorizations;
    private Set<String> admins;
    private RegistrationStatus status;
    private String publicKey;
    private byte[] privateKey;
    private String x509Cert;
    private byte[] salt;
    private String unencryptedPk;
    private String passcode;
    private boolean encrypting;
    
    public static AppPersistenceModel fromExternal(ApplicationRegistration registration) {
        AppPersistenceModel model = new AppPersistenceModel();

        model.id = registration.getId();
        model.owner = registration.getOwner();
        model.appName = registration.getAppName();
        model.authorizationLevel = registration.getClassification();
        model.formalAuthorizations = registration.getAuthorizations();
        model.communityAuthorizations = registration.getCommunityAuthorizations();
        model.admins = registration.getAdmins();
        model.status = registration.getStatus();
        model.appDn = registration.getAppDn();
        model.privateKey = null;
      
        return model;
    }

    /**
     * Check that 2 strings are equal
     *
     * @param s1 first string
     * @param s2 second string
     * @return true if they are equal
     */
    private static boolean checkEquality(String s1, String s2) {
        if (s1 == null && s2 != null) {
            return false;
        } else if (s1 != null && !s1.equals(s2)) {
            return false;
        }
        return true;
    }

    /**
     * Check that 2 RegistraitonStatuses are equal
     *
     * @param s1 first status
     * @param s2 second status
     * @return true if they are equal
     */
    private static boolean checkEquality(RegistrationStatus s1, RegistrationStatus s2) {
        if (s1 == null && s2 != null) {
            return false;
        } else if (s1 != null && !s1.equals(s2)) {
            return false;
        }
        return true;
    }

    /**
     * Check that 2 collections are equal(ish)
     * @param s1 first collection
     * @param s2 second collection
     * @return true if they are equal(ish)
     */
    private static boolean checkEquality(Collection<?> s1, Collection<?> s2) {
        if ((s1 == null || s1.isEmpty())
                && (s2 != null && !s2.isEmpty())) {
            return false;
        } else if ((s2 == null || s2.isEmpty())
                && (s1 != null && !s1.isEmpty())) {
            return false;
        } else if (s1 != null && s2 != null
                && !s1.equals(s2)) {
            return false;
        }
        return true;
    }

    /**
     * Check that 2 byte arrays are equal
     * @param b1 first byte array
     * @param b2 second byte array
     * @return true if they are equal
     */
    private static boolean checkEquality(byte[] b1, byte[] b2)  {
        if (b1 == null && b2 != null) {
            return false;
        } else if (b1 != null && !Arrays.equals(b1, b2)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null) {
            if (obj.getClass() == getClass()) {
                AppPersistenceModel other = (AppPersistenceModel)obj;
                if (checkEquality(id, other.id)) {
                    if (checkEquality(owner, other.owner)) {
                        if (checkEquality(appName, other.appName)) {
                            if (checkEquality(authorizationLevel, other.authorizationLevel)) {
                                if (checkEquality(status, other.status)) {
                                    if (checkEquality(formalAuthorizations, other.formalAuthorizations)) {
                                        if (checkEquality(communityAuthorizations, other.communityAuthorizations)) {
                                            if (checkEquality(admins, other.admins)) {
                                                if (checkEquality(appDn, other.appDn)) {
                                                    if (checkEquality(publicKey, other.publicKey)) {
                                                        if (checkEquality(privateKey, other.privateKey)) {
                                                            if (checkEquality(salt, other.salt)) {
                                                                return true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        if (id != null) {
            hashCode ^= id.hashCode();
        }
        if (owner != null) {
            hashCode ^= owner.hashCode();
        }
        if (appName != null) {
            hashCode ^= appName.hashCode();
        }
        if (authorizationLevel != null) {
            hashCode ^= authorizationLevel.hashCode();
        }
        if (formalAuthorizations != null && !formalAuthorizations.isEmpty()) {
            hashCode ^= formalAuthorizations.hashCode();
        }
        if (communityAuthorizations != null && !communityAuthorizations.isEmpty()) {
            hashCode ^= communityAuthorizations.hashCode();
        }
        if (admins != null && !admins.isEmpty()) {
            hashCode ^= admins.hashCode();
        }
        if (status != null) {
            hashCode ^= status.hashCode();
        }
        if (appDn != null) {
            hashCode ^= appDn.hashCode();
        }
        if (publicKey != null) {
            hashCode ^= publicKey.hashCode();
        }
        if (privateKey != null) {
            hashCode ^= Arrays.hashCode(privateKey);
        }
        if (salt != null) {
            hashCode ^= Arrays.hashCode(salt);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return String.format("%s:{id:%s,name:%s,dn:%s,owner:%s,status:%s,authorizationLevel:%s," +
                        "formalAuthorizations:%s,communityAuthorizations:%s}",
                getClass().getCanonicalName(),
                id,
                appName,
                appDn,
                owner,
                status,
                authorizationLevel,
                formalAuthorizations,
                communityAuthorizations);
    }

    public static ApplicationRegistration toExternal(AppPersistenceModel model) {
        ApplicationRegistration registration = new ApplicationRegistration();
        registration.setId(model.id);
        registration.setAppName(model.appName);
        registration.setAdmins(model.admins);
        registration.setAuthorizations(model.formalAuthorizations);
        registration.setCommunityAuthorizations(model.communityAuthorizations);
        registration.setClassification(model.authorizationLevel);
        registration.setOwner(model.owner);
        registration.setStatus(model.status);
        registration.setAppDn(model.appDn);
        return registration;
    }

    public static List<ApplicationRegistration> generateExternalList(List<AppPersistenceModel> models) {
        List<ApplicationRegistration> registrations = new ArrayList<ApplicationRegistration>();
        for (AppPersistenceModel model : models) {
            registrations.add(AppPersistenceModel.toExternal(model));
        }
        return registrations;
    }

    public static AppPersistenceModel fromRowsNoPrivate(List<Map.Entry<Key, Value>> rows) {
        AppPersistenceModel registration = new AppPersistenceModel();

        for (Map.Entry<Key, Value> row : rows) {
            String attribute = row.getKey().getColumnFamily().toString();
            byte[] value = row.getValue().get();

            if (registration.getId() == null) {
                String id = row.getKey().getRow().toString();
                registration.setId(id);
            }

            try {
                registration.setAttr(attribute, value);
            } catch (AppPersistCryptoException e) {
                // ignore
            }
        }

        return registration;
    }

    public static AppPersistenceModel fromRows(List<Map.Entry<Key, Value>> rows) throws AppPersistCryptoException {
        AppPersistenceModel registration = new AppPersistenceModel();

        for (Map.Entry<Key, Value> row : rows) {
            String attribute = row.getKey().getColumnFamily().toString();
            byte[] value = row.getValue().get();

            if (registration.getId() == null) {
                String id = row.getKey().getRow().toString();
                registration.setId(id);
            }
            registration.setAttr(attribute, value);
        }

        return registration;
    }

    @Override
    public AppPersistenceModel populateEzPersist(Map<String, String> rows) throws AppPersistCryptoException {
        for(Map.Entry<String, String> entry : rows.entrySet()) {
            String[] parts = EzPersist.keyParts(entry.getKey());
            if (parts == null || parts.length < 2) {
                continue;
            }
            String attribute = parts[1];
            String value = entry.getValue();

            if (id == null) {
                id = parts[0];
            }

            setAttr(attribute, value.getBytes());
        }
        // lookup ID in the mapping, and mutate accordingly
        if (SecurityID.ReservedSecurityId.isReserved(id)) {
            SecurityID.ReservedSecurityId sid = SecurityID.ReservedSecurityId.fromEither(id);
            id = sid.getCn();
            appName = sid.getCn();
        }
        
        if(encrypting) {
            encryptPkAndStoreSalt();
        }
        
        return this;
    }

    @Override
    public Map ezPersistRows() {
        ImmutableMap.Builder builder = ImmutableMap.builder();

        if (appDn != null) {
            String appDnRow = EzPersist.key(APP_REG_FIELDS.APP_DN.getValue());
            builder.put(appDnRow, appDn);
        }

        if (appName != null) {
            String nameRow = EzPersist.key(id, APP_REG_FIELDS.NAME.getValue());
            builder.put(nameRow, appName);
        }

        if (owner != null) {
            String ownerRow = EzPersist.key(id, APP_REG_FIELDS.OWNER.getValue());
            builder.put(ownerRow, owner);
        }
        if (admins != null) {
            String adminsRow = EzPersist.key(id, APP_REG_FIELDS.ADMINS.getValue());
            builder.put(adminsRow, Joiner.on(";").skipNulls().join(admins));
        }
        if (authorizationLevel != null) {
            String levelRow = EzPersist.key(id, APP_REG_FIELDS.LEVEL.getValue());
            builder.put(levelRow, authorizationLevel);
        }
        if (formalAuthorizations != null) {
            String visibilitiesRow = EzPersist.key(id, APP_REG_FIELDS.VISIBILITIES.getValue());
            builder.put(visibilitiesRow, Joiner.on(",").skipNulls().join(formalAuthorizations));
        }
        if (communityAuthorizations != null) {
            String commAuth = EzPersist.key(id, APP_REG_FIELDS.COMMUNITY_AUTHS.getValue());
            builder.put(commAuth, Joiner.on(",").skipNulls().join(communityAuthorizations));
        }
        if (status != null) {
            String statusRow = EzPersist.key(id, APP_REG_FIELDS.STATUS.getValue());
            builder.put(statusRow, status.toString());
        }
        if (publicKey != null) {
            String public_keyRow = EzPersist.key(id, APP_REG_FIELDS.PUBLIC_KEY.getValue());
            builder.put(public_keyRow, publicKey);
        }
        if (privateKey != null) {
            String privateRow = EzPersist.key(id, APP_REG_FIELDS.PRIVATE_KEY.getValue());
            builder.put(privateRow.getBytes(), privateKey);
        }
        if (x509Cert != null) {
            String certRow = EzPersist.key(id, APP_REG_FIELDS.X509_CERT.getValue());
            builder.put(certRow, x509Cert);
        }
        if(salt != null) {
            String saltRow = EzPersist.key(id, APP_REG_FIELDS.SALT.getValue());
            builder.put(saltRow, salt);
        }
        return builder.build();
    }

    private void setAttr(String attribute, byte[] rawValue) throws AppPersistCryptoException {
        if (attribute == null) {
            return;
        }
        String value = new String(rawValue);

        APP_REG_FIELDS field = APP_REG_FIELDS.fromString(attribute);
        switch (field) {
            case APP_DN:
                this.appDn = value;
                break;
            case LEVEL:
                this.authorizationLevel = value;
                break;
            case NAME:
                this.appName = value;
                break;
            case VISIBILITIES:
                this.formalAuthorizations = Splitter.on(",").omitEmptyStrings().splitToList(value);
                break;
            case COMMUNITY_AUTHS:
                communityAuthorizations = Splitter.on(",").omitEmptyStrings().splitToList(value);
                break;
            case STATUS:
                this.status = RegistrationStatus.valueOf(value.toUpperCase());
                break;
            case OWNER:
                this.owner = value;
                break;
            case ADMINS:
                admins = Sets.newHashSet(Splitter.on(";").omitEmptyStrings().split(value));
                break;
            case PUBLIC_KEY:
                this.publicKey = value;
                break;
            case PRIVATE_KEY:
                this.privateKey = rawValue;
                this.unencryptedPk = value;
                if (encrypting && salt == null && passcode != null) {
                    encryptPkAndStoreSalt();
                }
                break;
            case X509_CERT:
                this.x509Cert = value;
                break;
            case SALT:
                this.salt = rawValue;
                break;
        }
    }

    public static Scanner setScanOptions(final Scanner scanner, String id, boolean status) {
        return setScanOptions(scanner, id, status, false, false);
    }
    public static Scanner setScanOptions(final Scanner scanner,
                                         String id,
                                         boolean status,
                                         boolean owner,
                                         boolean admins) {
        scanner.setRange(new Range(id));
        if (status) {
            scanner.fetchColumnFamily(new Text(APP_REG_FIELDS.STATUS.getValue()));
        }
        if (owner) {
            scanner.fetchColumnFamily(new Text(APP_REG_FIELDS.OWNER.getValue()));
        }
        if (admins) {
            scanner.fetchColumnFamily(new Text(APP_REG_FIELDS.ADMINS.getValue()));
        }
        return scanner;
    }



    public Mutation getPrivateKeyMutation(String id, byte pk[]) throws AppPersistCryptoException {
        this.id = id;
        privateKey = pk;
        
        if(unencryptedPk == null)
            unencryptedPk = new String(pk);
        
        Mutation m = new Mutation(id);
        
        if(encrypting && salt == null && passcode != null) {
            encryptPkAndStoreSalt();
        }
        
        if(!encrypting) {
            m.put(APP_REG_FIELDS.PRIVATE_KEY.getBytes(),"".getBytes(), unencryptedPk.getBytes());
            return m;
        }
        
        m.put(APP_REG_FIELDS.PRIVATE_KEY.getBytes(), "".getBytes(), privateKey);
        return m;
    }

    public static Mutation getMutation(String row, String col, String colf, String value) {
        Mutation m = new Mutation(row);
        m.put(col, colf, value);
        return m;
    }

    public static Mutation getMutation(String row, String col, String colf, byte[] value) {
        Mutation m = new Mutation(row);
        m.put(col.getBytes(), colf.getBytes(), value);
        return m;
    }

    public static Mutation getDeleteMutation(String row, String col, String colf) {
        Mutation m = new Mutation(row);
        m.putDelete(col, colf);
        return m;
    }


    public List<Mutation> getObjectDeleteMutations() throws AppPersistCryptoException {
        List<Mutation> mutations = new ArrayList<>();

        if(this.appDn != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.APP_DN.getValue(), ""));
        }
        if (this.owner != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.OWNER.getValue(), ""));
        }
        if (this.admins != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.ADMINS.getValue(), ""));
        }
        if (this.appName != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.NAME.getValue(), ""));
        }
        if (this.authorizationLevel != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.LEVEL.getValue(), ""));
        }
        if (this.formalAuthorizations != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.VISIBILITIES.getValue(), ""));
        }
        if (this.communityAuthorizations != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.COMMUNITY_AUTHS.getValue(), ""));
        }
        if (this.salt != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.SALT.getValue(), ""));
        }
        if (this.status != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.STATUS.getValue(), ""));
        }
        if (this.publicKey != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.PUBLIC_KEY.getValue(), ""));
        }
        if (privateKey != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.PRIVATE_KEY.getValue(), ""));
        }
        if (x509Cert != null) {
            mutations.add(getDeleteMutation(id, APP_REG_FIELDS.X509_CERT.getValue(), ""));
        }
        return mutations;
    }

    public List<Mutation> getObjectMutations() throws AppPersistCryptoException {
        List<Mutation> mutations = new ArrayList<>();

        if(this.appDn != null) {
        	mutations.add(getMutation(id, APP_REG_FIELDS.APP_DN.getValue(), "", appDn));
        }
        if (this.owner != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.OWNER.getValue(), "", owner));
        }
        if (this.admins != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.ADMINS.getValue(), "",
                    Joiner.on(";").skipNulls().join(admins)));
        }
        if (this.appName != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.NAME.getValue(), "", appName));
        }
        if (this.authorizationLevel != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.LEVEL.getValue(), "", authorizationLevel));
        }
        if (this.formalAuthorizations != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.VISIBILITIES.getValue(), "",
                    Joiner.on(",").skipNulls().join(formalAuthorizations)));
        }
        if (this.communityAuthorizations != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.COMMUNITY_AUTHS.getValue(), "",
                    Joiner.on(",").skipNulls().join(communityAuthorizations)));
        }
        if (this.salt != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.SALT.getValue(), "", salt));
        }
        if (this.status != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.STATUS.getValue(), "", status.toString()));
        }
        if (this.publicKey != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.PUBLIC_KEY.getValue(), "", publicKey));
        }
        if (privateKey != null) {
            mutations.add(getPrivateKeyMutation(id, privateKey));
        }
        if (x509Cert != null) {
            mutations.add(getMutation(id, APP_REG_FIELDS.X509_CERT.getValue(), "", x509Cert));
        }

        return mutations;
    }

    public List<Mutation> getLookupMutations() {
        List<Mutation> mutations = new ArrayList<Mutation>();

        if (this.owner != null && !this.owner.isEmpty()) {
            Mutation owner = new Mutation(this.owner);
            owner.put(this.id, this.status.toString(), APP_LOOKUP_FIELDS.OWNER.getValue());
            owner.put(this.status.toString(), this.id, APP_LOOKUP_FIELDS.OWNER.getValue());
            mutations.add(owner);
        }

        if (this.admins != null && !this.admins.isEmpty()) {
            for (String admin : this.admins) {
                log.debug("Adding admin {} to lookup mutation", admin);
                Mutation m = new Mutation(admin);
                m.put(this.id, this.status.toString(), APP_LOOKUP_FIELDS.ADMIN.getValue());
                m.put(this.status.toString(), this.id, APP_LOOKUP_FIELDS.ADMIN.getValue());
                mutations.add(m);
            }
        }

        if (this.status != null) {
            mutations.add(getMutation(this.status.toString(), id, "", id));
        }

        return mutations;
    }

    public List<Mutation> getLookupDeleteMutations() {
        List<Mutation> mutations = new ArrayList<>();

        if (this.owner != null) {
            Mutation owner = new Mutation(this.owner);
            owner.putDelete(this.id, this.status.toString());
            owner.putDelete(this.status.toString(), this.id);
            mutations.add(owner);
        }

        if (this.admins != null) {
            for (String admin : this.admins) {
                Mutation m = new Mutation(admin);
                m.putDelete(this.id, this.status.toString());
                m.putDelete(this.status.toString(), this.id);
                mutations.add(m);
            }
        }

        if (this.status != null) {
            Mutation base = new Mutation(this.status.toString());
            base.putDelete(this.id, "");
            mutations.add(base);
        }

        return mutations;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public RegistrationStatus getStatus() {
        return status;
    }

    public void setStatus(RegistrationStatus status) {
        this.status = status;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAuthorizationLevel() {
        return authorizationLevel;
    }

    public void setAuthorizationLevel(String authorizationLevel) {
        this.authorizationLevel = authorizationLevel;
    }

    public List<String> getFormalAuthorizations() {
        return formalAuthorizations;
    }

    public void setFormalAuthorizations(List<String> formalAuthorizations) {
        this.formalAuthorizations = formalAuthorizations;
    }

    public List<String> getCommunityAuthorizations() {
        return (communityAuthorizations == null) ? Collections.<String>emptyList() : communityAuthorizations;
    }

    public void setCommunityAuthorizations(List<String> communityAuthorizations) {
        this.communityAuthorizations = communityAuthorizations;
    }

    public Set<String> getAdmins() {
        if (admins == null) {
            admins = new HashSet<>();
        }
        return admins;
    }

    public void setAdmins(Set<String> admins) {
        this.admins = admins;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public String getPrivateKey() throws AppPersistCryptoException {
        if(unencryptedPk == null && privateKey != null) {
            unencryptedPk = this.decryptPk();
        }
        return unencryptedPk;
    }

    public void setPrivateKey(String pk) throws AppPersistCryptoException {
        if (pk != null) {
            unencryptedPk = pk;
            privateKey = pk.getBytes();
            this.encryptPkAndStoreSalt();
        }
    }

    public String getX509Cert() {
        return x509Cert;
    }

    public void setX509Cert(String x509Cert) {
        this.x509Cert = x509Cert;
    }
    
    public void setAppDn(String appDn) {
    	this.appDn = appDn;
    }
    
    public String getAppDn() {
    	return this.appDn;
    }
    
    public void setSalt(byte[] salt) {
        this.salt = salt;
    }
    
    public byte[] getSalt() {
        return this.salt;
    }
    
    public void setEncrypting(boolean encrypting) {
        this.encrypting =encrypting;
    }
    
    public boolean getEncrypting() {
        return this.encrypting;
    }
    
    public String getPasscode() {
        return this.passcode;
    }
    
    public void setPasscode(String passcode) {
        this.passcode = passcode;
    }
    
    public byte[] getEncryptedPk() {
        return this.privateKey;
    }
    
    private void encryptPkAndStoreSalt() throws AppPersistCryptoException  {
        String pk;
        
        log.debug("Encrypt Pk and Store Salt {} {}", privateKey, id);
        
        if(privateKey == null || !encrypting) {
            return;
        }
        
        if(this.unencryptedPk == null) {
            this.unencryptedPk = new String(privateKey);
        }
        
        log.debug("Pass code {}", passcode);
        
        if(passcode == null) {
            log.warn("Passcode was null, and therefore couldn't encrypt the Pk");
            return;
        }
        
        PBECrypto pbeCrypto = new PBECrypto(passcode);
        
        byte[] cipherData;
        try {
            log.debug("Using Salt {}", pbeCrypto.getSalt());
            SecretKey key = pbeCrypto.generateKey();
            cipherData = pbeCrypto.encrypt(key, privateKey);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException |
                InvalidAlgorithmParameterException | NoSuchPaddingException | IllegalBlockSizeException |
                BadPaddingException | ShortBufferException e) {
            log.error("Error: {}", e);
            throw new AppPersistCryptoException("Error " + e);
        }

        log.debug("Original Cipher {}", cipherData);
        privateKey = cipherData;
        setSalt(pbeCrypto.getSalt());
        
        log.debug("Successfully Encrypted Pk ");
    }
    
    private String decryptPk() throws AppPersistCryptoException {
        String retVal;

        if(!SecurityID.ReservedSecurityId.isReserved(id) || this.privateKey == null || salt == null) {
            return "";
        }
        
        if(passcode == null) {
            log.warn("Passcode was null when attempting to decrypt the private key");
            return null;
        }
        
        log.debug("Passcode {}", passcode);
        PBECrypto pbeCrypto = new PBECrypto(passcode);
        
        byte[] uncipherData;
        
        try {
            pbeCrypto.setSalt(this.salt);
            log.debug("Using salt {}", pbeCrypto.getSalt());

            SecretKey key = pbeCrypto.generateKey();
            uncipherData = pbeCrypto.decrypt(key, privateKey);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException | NoSuchPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException | ShortBufferException e) {
            log.error("Error: {}", e);
            throw new AppPersistCryptoException("Error " + e);
        }

        retVal = uncipherData == null ? null : new String(uncipherData);
        
        return retVal;
    }
 
    public void encryptKeyIfNotDoneSo() throws AppPersistCryptoException {
        if(salt != null) {
            return;
        }
        
       encryptPkAndStoreSalt();
    }
}
