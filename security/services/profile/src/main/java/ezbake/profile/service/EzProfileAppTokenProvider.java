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

package ezbake.profile.service;

import com.google.inject.Inject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenProvider;

/**
 * User: jhastings
 * Date: 8/10/14
 * Time: 4:08 PM
 */
public class EzProfileAppTokenProvider implements EzSecurityTokenProvider {

    private EzbakeSecurityClient securityClient;
    @Inject
    public EzProfileAppTokenProvider(EzbakeSecurityClient securityClient) {
        this.securityClient = securityClient;
    }

    @Override
    public EzSecurityToken get(String targetSecurityId) throws EzSecurityTokenException {
        return securityClient.fetchAppToken(targetSecurityId);
    }
}
