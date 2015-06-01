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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.groups.thrift.EzGroupsConstants;

import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 1:10 PM
 */
public class GroupNameHelper {
    private static final String ROOT_PREFIX = EzGroupsConstants.ROOT+EzGroupsConstants.GROUP_NAME_SEP;

    public String addRootGroupPrefix(String unprefixed) {
        String prefixed = unprefixed;
        if (unprefixed == null || !unprefixed.startsWith(ROOT_PREFIX)) {
            prefixed = Joiner.on(EzGroupsConstants.GROUP_NAME_SEP)
                    .skipNulls()
                    .join(EzGroupsConstants.ROOT, Strings.emptyToNull(unprefixed));
        }
        return prefixed;
    }

    public Set<String> addRootGroupPrefix(String... unprefixeds) {
        Set<String> prefixeds = Sets.newHashSet();
        for (String unprefixed : unprefixeds) {
            prefixeds.add(addRootGroupPrefix(unprefixed));
        }
        return prefixeds;
    }

    public String removeRootGroupPrefix(String prefixed) {
        String removed = prefixed;
        if (prefixed.startsWith(ROOT_PREFIX)) {
            removed = prefixed.replaceFirst(ROOT_PREFIX, "");
        }
        return removed;
    }

    public String getNamespacedAppGroup(String appName) {
        return getNamespacedAppGroup(appName, null);
    }
    public String getNamespacedAppGroup(String appName, String appGroups) {
        return joinAll(EzGroupsConstants.ROOT, EzGroupsConstants.APP_GROUP, appName, appGroups);
    }

    public String getNamespacedAppAccessGroup(String appName) {
        return Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).skipNulls().join(EzGroupsConstants.ROOT,
                EzGroupsConstants.APP_ACCESS_GROUP, appName);
    }

    /**
     * Simply builds the new group name string from the fully qualified original name and the new friendly name.
     *
     * @param fullyQualifiedOriginal original fully qualified name of the group whose name will be changed
     * @param newFriendlyName new friendly name for the group
     * @return a fully qualified name that includes the new friendly name instead of the old friendly name
     */
    public String changeGroupName(String fullyQualifiedOriginal, String newFriendlyName) {
        List<String> parts = Lists.newArrayList(Splitter.on(EzGroupsConstants.GROUP_NAME_SEP)
                .omitEmptyStrings()
                .trimResults().split(fullyQualifiedOriginal));
        if (!parts.isEmpty()) {
            parts.remove(parts.size()-1);
            parts.add(parts.size(), newFriendlyName);
        }
        return joinAll(parts.toArray(new String[parts.size()]));
    }

    private String joinAll(String... parts) {
        return Joiner.on(EzGroupsConstants.GROUP_NAME_SEP).skipNulls().join(parts);
    }
}
