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

package ezbake.quarantine.service.util;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenUtility {
    private static final Logger log = LoggerFactory.getLogger(TokenUtility.class);

    public static String getSecurityId(EzSecurityToken token) {
        return new EzSecurityTokenWrapper(token).getSecurityId();
    }

    public static void validateToken(EzbakeSecurityClient security, EzSecurityToken token) throws TException {
        try {
            security.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            log.error("Could not validate security token", e);
            throw new TException("Could not validate security token", e);
        }
    }
}
