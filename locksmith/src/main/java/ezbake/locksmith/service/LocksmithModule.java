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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import ezbake.locksmith.db.AesLocksmithManager;
import ezbake.locksmith.db.RsaLocksmithManager;
import ezbake.security.client.EzbakeSecurityClient;

import javax.inject.Singleton;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 8/10/14
 * Time: 9:29 PM
 */
public class LocksmithModule extends AbstractModule {
    private Properties ezProperties;
    public LocksmithModule(Properties ezProperties) {
        this.ezProperties = ezProperties;
    }
    @Override
    protected void configure() {
        bind(Properties.class).toInstance(ezProperties);
    }

    @Provides
    public EzbakeSecurityClient provideSecurityClient() {
        return new EzbakeSecurityClient(ezProperties);
    }

    @Provides
    @Singleton
    public AesLocksmithManager provideAesManager() {
        return new AesLocksmithManager("aes_keys", ezProperties);
    }

    @Provides
    @Singleton
    public RsaLocksmithManager provideRsaManager() {
        return new RsaLocksmithManager("rsa_keys", ezProperties);
    }
}
