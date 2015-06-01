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

package ezbake.intent.query.processor;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.intent.query.utils.Conversions;
import ezbake.intents.common.RedisUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * Created by fyan on 12/11/14.
 */
public class SecurityStorageRedisImpl implements SecurityStorage {
    private RedisUtils redisUtils;

    public SecurityStorageRedisImpl(EzConfiguration configuration) {
        this.redisUtils = new RedisUtils(configuration);
    }

    @Override
    public String storeToken(EzSecurityToken securityToken) throws TException {
        try {
            String uuid = Conversions.generateUUID();
            redisUtils.put(uuid.getBytes(), new TSerializer().serialize(securityToken));
            return uuid;
        } catch (Exception e) {
            throw new TException(e);
        }
    }
}