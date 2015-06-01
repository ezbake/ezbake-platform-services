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

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.SecurityIDNotFoundException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: jhastings
 * Date: 10/7/13
 * Time: 2:15 PM
 */
public class TestClientLookup {
    private static Properties configuration;
    private static ClientLookup clu;
    private static AccumuloRegistrationManager reg;

    public static final String bob1 = "10000001";
    public static final String bob2 = "10000002";
    public static final String jim1 = "20000001";
    @BeforeClass
    public static void setupTest() throws AccumuloSecurityException, AccumuloException, IOException, RegistrationException, SecurityIDNotFoundException, EzConfigurationLoaderException {
        configuration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

        reg = new AccumuloRegistrationManager(configuration);

        reg.register(bob1, "Bob", "TestApp1", "high", Arrays.asList("X", "Y", "Z"), null, "App Dn 1");
        reg.approve("U".split(","), bob1);

        //Register app 2
        reg.register(bob2, "Bob", "TestApp2", "med", Arrays.asList("X", "Y", "Z"), null, "App Dn 1");
        reg.approve("U".split(","), bob2);

        //Register app 3
        reg.register(jim1, "Jim", "Jim1", "low", Arrays.asList("X", "Y", "Z"), null,"App Dn 1");
        reg.approve("U".split(","), jim1);

        configuration.setProperty("ezsecurity.auths", "U,pending");
        clu = new ClientLookup(configuration, reg);
    }

    private TestTicker ticker;

    @Before
    public void setUp() {
        ticker = new TestTicker();
    }

    @Test
    public void byAppName() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, RegistrationException {
        final String appName = "TestApp1";

        AppInstance nreg = clu.getClient(bob1);
        Assert.assertNotNull("Registration should not be null", nreg);
        Assert.assertEquals("App Name should match requested", appName, nreg.getRegistration().getAppName());
        Assert.assertEquals("high", nreg.getRegistration().getAuthorizationLevel());
        Assert.assertArrayEquals(new String[]{"X", "Y", "Z"}, nreg.getRegistration().getFormalAuthorizations().toArray());
    }

    /**
     * This test isn't very good - it was mostly used for me to verify the logging in the cache
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws InterruptedException
     */
    @Test
    public void testCache() throws AccumuloSecurityException, AccumuloException, InterruptedException {
        ClientLookup manager = new ClientLookup(CacheBuilder.newBuilder()
                .expireAfterWrite(20, TimeUnit.MINUTES)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .build(new ClientLookup.RegistrationLoader(configuration, reg)));

        final String appName = "TestApp2";

        // Initial load
        AppInstance nreg = manager.getClient(bob1);
        Assert.assertNotNull(nreg);

        ticker.advance(13, TimeUnit.MINUTES);
        nreg = manager.getClient(bob1);
        Assert.assertNotNull(nreg);


        ticker.advance(30, TimeUnit.MINUTES);
        nreg = manager.getClient(bob1);
        Assert.assertNotNull(nreg);
    }

    private class TestTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        /**
         * Advances the ticker value by {@code time} in {@code timeUnit}.
         */
        public Ticker advance(long time, TimeUnit timeUnit) {
            return advance(timeUnit.toNanos(time));
        }

        /**
         * Advances the ticker value by {@code nanoseconds}.
         */
        public Ticker advance(long nanoseconds) {
            nanos.addAndGet(nanoseconds);
            return this;
        }

        @Override
        public long read() {
            return nanos.get();
        }
    }
}
