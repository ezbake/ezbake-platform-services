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

import ezbake.groups.graph.api.GroupIDProvider;

import java.util.concurrent.atomic.AtomicLong;

/**
 * User: jhastings
 * Date: 6/23/14
 * Time: 10:51 AM
 */
public class NaiveIDProvider implements GroupIDProvider {
    protected final AtomicLong id = new AtomicLong(0l);

    public void setCurrentID(long id) {
        this.id.set(id);
    }

    @Override
    public void setCurrentID() {
        this.id.set(0);
    }

    /**
     * Get the current ID. In normal circumstances, this ID has already been allocated, and should only be used as a
     * reference
     *
     * @return the current ID
     */
    @Override
    public long currentID() {
        return id.get();
    }

    /**
     * Get the next available ID
     * @return the value of the ID
     */
    @Override
    public long nextID() {
        return id.incrementAndGet();
    }
}
