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

package ezbake.groups.service;

import com.google.inject.Guice;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.groups.graph.EzGroupsGraphModule;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.thrift.EzGroups;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 */
public class EzBakeThriftService extends EzBakeBaseThriftService {
    private static final Logger logger = LoggerFactory.getLogger(EzBakeThriftService.class);

    private static BaseGroupsService instance;

    @Override
    public TProcessor getThriftProcessor() {
        // Implement singleton using double checked locking pattern
        if (instance == null) {
            synchronized (EzBakeThriftService.class) {
                if (instance == null) {
                    instance = createSingleton(getConfigurationProperties());
                }
            }
        }
        return new EzGroups.Processor<>(instance);
    }

    @Override
    public void shutdown() {
        synchronized (EzBakeThriftService.class) {
            if (instance != null) {
                try {
                    instance.close();
                    instance = null;
                } catch (IOException e) {
                    logger.error("Unable to close the graph cleanly", e);
                }
            }
        }
    }

    public static BaseGroupsService getInstance() {
        return instance;
    }

    private static BaseGroupsService createSingleton(Properties configuration) {
        return Guice.createInjector(
                new GroupsServiceModule(configuration),
                new EzGroupsGraphModule(configuration)
        ).getInstance(BaseGroupsService.class);
    }
}
