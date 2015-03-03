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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import ezbake.security.persistence.api.RegistrationManager;
import ezbake.security.thrift.RegistrationStatus;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * User: jhastings
 * Date: 9/27/13
 * Time: 1:29 PM
 */
public class ClientLookup implements EzbakeRegistrationService {
    private static final Logger logger = LoggerFactory.getLogger(ClientLookup.class);

    private LoadingCache<String, AppInstance> appCache;

    @Inject
    public ClientLookup(Properties properties, RegistrationManager regManager) throws AccumuloSecurityException,
            AccumuloException {
        appCache = CacheBuilder.newBuilder()
                .concurrencyLevel(20)
                .refreshAfterWrite(2, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new RegistrationLoader(properties, regManager));
    }

    public ClientLookup(LoadingCache<String, AppInstance> appCache) {
        this.appCache = appCache;
    }

    @Override
    public AppInstance getClient(String id) {
        try {
            return appCache.get(id);
        } catch (ExecutionException e) {
            logger.error("Failed retrieving application from cache. returning null", e);
            return null;
        }
    }

    protected static class RegistrationLoader extends CacheLoader<String, AppInstance> {
        private ExecutorService executor = Executors.newFixedThreadPool(4);
        private RegistrationManager manager;
        private String[] auths;

        public RegistrationLoader(Properties properties, RegistrationManager registrationManager) throws
                AccumuloSecurityException, AccumuloException {
            manager = registrationManager;
            auths = properties.getProperty("ezsecurity.auths", "U").split(",");
        }

        @Override
        public AppInstance load(String id) throws Exception {
            logger.info("Loading application for: {}", id);
            return new AppInstance(manager.getRegistration(auths, id, null, RegistrationStatus.ACTIVE));
        }

        @Override
        public ListenableFuture<AppInstance> reload(final String id, AppInstance prevApp) {
            logger.debug("Refreshing application for: {}", id);
            ListenableFutureTask<AppInstance> task = ListenableFutureTask.create(new Callable<AppInstance>() {
                @Override
                public AppInstance call() throws Exception {
                    return new AppInstance(manager.getRegistration(auths, id, null, RegistrationStatus.ACTIVE));
                }
            });
            executor.execute(task);
            return task;
        }
    }
}
