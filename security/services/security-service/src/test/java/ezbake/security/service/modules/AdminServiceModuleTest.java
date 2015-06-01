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

package ezbake.security.service.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.security.service.admins.AdministratorService;
import ezbake.security.service.admins.PublishingAdminService;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/15/14
 * Time: 8:55 AM
 */
public class AdminServiceModuleTest {

    @Test
    public void moduleTest() {
        Guice.createInjector(new AdminServiceModule(new Properties())).getInstance(AdministratorService.class);
    }

    @Test
    public void moduleTestWithPublishingService() {
        final Properties p = new Properties();
        p.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
        Guice.createInjector(
                new AdminServiceModule(p),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(p);
                        bind(Properties.class).toInstance(p);
                        bind(ServiceDiscoveryClient.class).toInstance(new ServiceDiscoveryClient(""));
                        bind(CuratorFramework.class).toInstance(CuratorFrameworkFactory.builder()
                                .connectString(zc.getZookeeperConnectionString())
                                .retryPolicy(new RetryNTimes(5, 1000))
                                .build());
                    }
                }
        ).getInstance(PublishingAdminService.class);
    }
}
