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
 * Date: 7/30/14
 * Time: 3:43 PM
 */
public interface GroupIDPublisher {
    /**
     * Updates the ID provider with the active group ID. Allocation will continue with subsequent values
     */
    public long getCurrentId();
}
