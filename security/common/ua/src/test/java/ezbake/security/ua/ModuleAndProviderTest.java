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

package ezbake.security.ua;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.impl.ua.FileUAService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/14/14
 * Time: 3:14 PM
 */
public class ModuleAndProviderTest {

    Properties properties;
    @Before
    public void setUp() throws EzConfigurationLoaderException {
        properties = new Properties();
        properties.setProperty(UserAttributeServiceProvider.UA_SERVICE_IMPL, FileUAService.class.getCanonicalName());
    }

    @Test
    public void testInstantiation() {

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Properties.class).toInstance(properties);
            }
        }, new UAModule(properties));
        UserAttributeService uaService = injector.getInstance(UserAttributeService.class);

        Assert.assertNotNull(uaService);
    }
}
