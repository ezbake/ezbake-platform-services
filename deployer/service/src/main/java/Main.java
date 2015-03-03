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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.deployer.DeployerModule;
import ezbake.deployer.EzBakeDeployerHandler;
import ezbake.deployer.publishers.local.LocalDeployerModule;
import ezbake.services.deploy.thrift.EzDeployServiceConstants;
import ezbake.thrift.ThriftServerPool;

import java.util.Properties;
import java.util.Scanner;

public class Main {
    public static void main(String args[]) throws Exception {
        //Need to configure EZCONFIGURATION_DIR for this main to work
        //Also needs Accumulo running (local-accumulo will work) but NOT zookeeper.  ThriftServerPool starts zookeeper
        Injector injector = Guice.createInjector(new DeployerModule(), new LocalDeployerModule(), new MainConfigurationModule());
        ThriftServerPool pool = new ThriftServerPool(injector.getInstance(EzConfiguration.class).getProperties(), 15000);
        try {
            pool.startCommonService(injector.getInstance(EzBakeDeployerHandler.class), EzDeployServiceConstants.SERVICE_NAME, "client");
            Scanner scanner = new Scanner(System.in);
            while (!scanner.hasNext()) ;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            pool.shutdown();
        }
    }

    public static class MainConfigurationModule extends AbstractModule {
        @Override
        protected void configure() {
        }

        @Provides
        Properties getEzConfiguration() throws EzConfigurationLoaderException {
            return new EzConfiguration().getProperties();
        }
    }
}
