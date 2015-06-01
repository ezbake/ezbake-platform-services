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

package ezbake.warehaus.tools;

import java.util.Properties;

import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.client.EzbakeSecurityClient;


public abstract class AbstractWarehausAction {
   
   protected Properties config;
   protected EzbakeSecurityClient securityClient;
   
   protected AbstractWarehausAction() {
      try {
         this.config = new EzConfiguration().getProperties();
      } catch (EzConfigurationLoaderException e) {
         throw new RuntimeException(e);
      }
      securityClient = new EzbakeSecurityClient(config);
   }
}
