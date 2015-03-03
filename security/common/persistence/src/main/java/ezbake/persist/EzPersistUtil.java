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

package ezbake.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.crypto.utils.CryptoUtil;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.model.AppPersistCryptoException;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.AppCerts;
import ezbake.security.thrift.RegistrationException;

import java.io.IOException;

public class EzPersistUtil {
    private static Logger log = LoggerFactory.getLogger(EzPersistUtil.class);
    
    public static AppCerts getAppCerts(AppPersistenceModel app, AppPersistenceModel caApm, AppPersistenceModel securityApm) throws RegistrationException, IOException {
        String ca_cert = caApm.getX509Cert();
        if (ca_cert == null) {
            throw new RegistrationException("Unable to retrieve CA certificate");
        }
        log.debug("Load jks for ca cert {}", ca_cert);
        byte[] ca_jks = CryptoUtil.load_jks(ca_cert);

        byte[] ezsec_pub = securityApm.getPublicKey().getBytes();

        AppCerts info = new AppCerts();
        info.setApplication_crt((app.getX509Cert() != null)? app.getX509Cert().getBytes() : null);
        
        try {
            info.setEzbakesecurityservice_pub(ezsec_pub);
            info.setEzbakeca_crt(ca_cert.getBytes());
            info.setEzbakeca_jks(ca_jks);
            
            info.setApplication_priv((app.getPrivateKey() != null) ? app.getPrivateKey().getBytes() : null);
            info.setApplication_pub((app.getPublicKey() != null) ? app.getPublicKey().getBytes() : null);
            info.setApplication_p12(CryptoUtil.load_p12(ca_cert, app.getX509Cert(), app.getPrivateKey()));
            
        } catch (AppPersistCryptoException e) {
            log.error("Error {}", e);
            throw new RegistrationException("Error " + e);
        }
        
        return info;
    }
}
