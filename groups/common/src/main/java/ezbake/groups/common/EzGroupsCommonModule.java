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

package ezbake.groups.common;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/23/14
 * Time: 3:21 PM
 */
public abstract class EzGroupsCommonModule extends AbstractModule {

    protected Properties ezProperties;
    public EzGroupsCommonModule(Properties properties) {
        ezProperties = properties;
    }

    @Provides
    public Properties provideEzProperties() {
        return ezProperties;
    }
}
