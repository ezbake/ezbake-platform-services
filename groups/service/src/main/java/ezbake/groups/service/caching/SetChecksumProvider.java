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

 package ezbake.groups.service.caching;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.SortedSet;

public class SetChecksumProvider implements ChecksumProvider<Set<Long>, Long> {

    @Override
    public Long getChecksum(Set<Long> data) {
        SortedSet<Long> sortedSet = Sets.newTreeSet(data);

        Long checksum = 0l;
        for (Long l : sortedSet) {
            checksum ^= l;
        }

        return checksum;
    }
}
