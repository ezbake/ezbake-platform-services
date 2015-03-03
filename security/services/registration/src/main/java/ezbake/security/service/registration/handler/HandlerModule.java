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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import ezbake.security.persistence.AppPersistenceModule;
import ezbake.thrift.ThriftClientPool;

import javax.inject.Singleton;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 12/18/14
 * Time: 4:18 PM
 */
public class HandlerModule extends AbstractModule {

    private Properties properties;
    public HandlerModule(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        install(new AppPersistenceModule(properties));
    }

    @Provides
    Properties provideEzConfiguration() {
        return properties;
    }

    @Provides
    @Singleton
    ThriftClientPool clientPoolProvider() {
        return new ThriftClientPool(properties);
    }
}
