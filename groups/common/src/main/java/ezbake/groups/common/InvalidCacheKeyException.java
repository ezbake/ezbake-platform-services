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

/**
 * Exception used when a key cannot be parsed
 */
public class InvalidCacheKeyException extends Exception {

    private String key;

    public InvalidCacheKeyException(String key, String message) {
        super(message);
        this.key = key;
    }

    public InvalidCacheKeyException(String key, String message, Throwable t) {
        super(message, t);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
