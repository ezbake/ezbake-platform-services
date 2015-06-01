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

import com.google.inject.name.Names;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.service.admins.AdministratorService;
import ezbake.security.service.admins.PublishingAdminService;

import java.io.File;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/15/14
 * Time: 8:29 AM
 */
public class AdminServiceModule extends WatchedFileModule {
    public static final String ADMIN_FILE =  new File(DirectoryConfigurationLoader.EZCONFIGURATION_DEFAULT_DIR, "admins").getAbsolutePath();
    public static final String PUBLISHING =  "ezbake.security.service.publishing";

    Properties ezProperties;
    public AdminServiceModule(Properties ezConfiguration) {
        this.ezProperties = ezConfiguration;
    }

    @Override
    protected void configure() {
        // Extending WatchedFileModule, but want it's configure to run
        super.configure();

        // Bind our specific things
        String adminFile = ezProperties.getProperty(EzBakePropertyConstants.EZBAKE_ADMINS_FILE, ADMIN_FILE);
        boolean publishing = new EzProperties(ezProperties, true).getBoolean(PUBLISHING, false);

        bindConstant().annotatedWith(Names.named("Admin File")).to(adminFile);
        if (publishing) {
            bind(AdministratorService.class).to(PublishingAdminService.class);
        }
    }
}
