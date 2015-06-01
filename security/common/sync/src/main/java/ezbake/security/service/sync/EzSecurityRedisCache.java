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

package ezbake.security.service.sync;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 7/18/14
 * Time: 12:11 PM
 */
public interface EzSecurityRedisCache extends Closeable {
    public void put(String key, byte[] value, long expireAt) throws Exception;
    public void put(String key, byte[] value, long expireAt, long timeout, TimeUnit timeUnit) throws Exception;
    public byte[] get(String key) throws Exception;
    public byte[] get(String key, long timeout, TimeUnit timeUnit) throws Exception;
    public boolean exists(String key) throws Exception;
    public boolean exists(String key, long timeout, TimeUnit timeUnit) throws Exception;
    public boolean invalidate() throws Exception;
}
