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

import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import org.apache.thrift.TException;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.client.EzbakeSecurityClient;

public class ExportImportHelper {

    static String confirmToken(EzSecurityToken security, EzbakeSecurityClient securityClient) 
            throws TException {
        if (securityClient != null) {
            try {
                securityClient.validateReceivedToken(security);
            } catch (EzSecurityTokenException e) {
                throw new TException(e);
            }
        }
        String id;
        if (security.getType() == TokenType.APP) {
            id = security.getValidity().getIssuedTo();
        } else {
            id = security.getTokenPrincipal().getPrincipal();
        }
        return id;
    }

    static enum DataType {
        RAW(),
        PARSED();
        
        private DataType() {
        }
    }

}
