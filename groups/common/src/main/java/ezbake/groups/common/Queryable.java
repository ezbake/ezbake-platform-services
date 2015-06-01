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

import java.util.concurrent.Callable;

/**
 */
public interface Queryable<T> extends Cachable<T>, Callable<T> {

    /**
     * Run the query and produce a result
     * @return the result of the query
     * @throws Exception
     */
    public T runQuery() throws Exception;

    /**
     * Get the value that should be returned for an invalid query
     *
     * This will be run when it is determined that the query should not be run, and instead,
     * an 'invalid result' should be returned. Typically this would be something like null, or
     * and empty set.
     *
     * @return the result when the query is invalid
     */
    public T getInvalidResult();
}
