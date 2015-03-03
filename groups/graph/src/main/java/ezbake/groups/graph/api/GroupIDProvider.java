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

package ezbake.groups.graph.api;

/**
 * User: jhastings
 * Date: 6/23/14
 * Time: 10:50 AM
 */
public interface GroupIDProvider {

    public static final String zookeeper_namespace = "/ezbake/groups";
    public static final String index_counter = "index/counter";
    public static final String counter_lock = "index/lock";
    public static final String index_valid = "index/valid";
    public static final String valid_lock = "index/valid_lock";

    public void setCurrentID() throws Exception;

    /**
     * Get the current ID. In normal circumstances, this ID has already been allocated, and should only be used as a
     * reference
     *
     * @return the current ID
     */
    public long currentID() throws Exception;

    /**
     * Get the next available ID
     * @return the value of the ID
     */
    public long nextID() throws Exception;
}
