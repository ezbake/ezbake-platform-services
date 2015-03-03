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

package ezbake.deployer.utilities;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.TokenType;

/**
 * Uses Security Token to get the user
 */
public class SecurityTokenUserProvider implements UserProvider {
    private final EzSecurityToken token;

    public SecurityTokenUserProvider(EzSecurityToken token) {
        this.token = token;
    }

    @Override
    public String getUser() {
        if (token != null && (token.getType() == null || token.getType() == TokenType.USER)
                && token.getTokenPrincipal() != null) {
            return token.getTokenPrincipal().getPrincipal();
        }
        return "";
    }

    @Override
    public String call() throws Exception {
        return getUser();
    }
}
