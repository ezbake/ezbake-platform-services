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

package ezbake.groups.graph.impl;

import ezbake.groups.graph.api.GroupIDPublisher;

/**
 * User: jhastings
 * Date: 7/30/14
 * Time: 4:09 PM
 */
public class MockGroupIdPublisher implements GroupIDPublisher {
    private long currentId = 0;

    @Override
    public long getCurrentId() {
        return currentId;
    }

    public synchronized void setCurrentId(long id) {
        currentId = id;
    }
}
