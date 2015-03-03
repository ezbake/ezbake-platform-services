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

package ezbake.security.service;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenJson;
import ezbake.security.common.core.TokenJSONProvider;
import ezbake.crypto.PKeyCrypto;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * User: jhastings
 * Date: 7/11/14
 * Time: 8:42 AM
 */
public class ThriftTokenJSONProvider implements TokenJSONProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftTokenJSONProvider.class);

    @Override
    public EzSecurityTokenJson getTokenJSON(EzSecurityToken token, PKeyCrypto signer)
            throws TException, UnsupportedEncodingException {

        EzSecurityTokenJson json = new EzSecurityTokenJson();
        json.setJson(new String(new TSerializer(new TSimpleJSONProtocol.Factory()).serialize(token), "UTF-8"));
        json.setSignature("");
        return json;
    }

}
