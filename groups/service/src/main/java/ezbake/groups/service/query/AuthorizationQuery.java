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

package ezbake.groups.service.query;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.groups.common.InvalidCacheKeyException;
import ezbake.groups.common.Queryable;
import ezbake.groups.graph.GroupsGraph;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import ezbake.groups.thrift.GroupQueryException;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class AuthorizationQuery implements Queryable<Set<Long>> {

    private GroupsGraph graph;
    private BaseVertex.VertexType type;
    private String id;
    private List<String> chain;

    public AuthorizationQuery(GroupsGraph graph, BaseVertex.VertexType type, String id, List<String> chain) {
        this.graph = graph;
        this.type = type;
        this.id = id;
        this.chain = chain;
    }

    public BaseVertex.VertexType getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public List<String> getChain() {
        if (chain != null) {
            return Lists.newArrayList(chain);
        }
        return null;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public Set<Long> call() throws Exception {
        return graph.getAuthorizations(type, id, chain);
    }

    @Override
    public String getKey() {
        String key;
        if (chain == null || chain.isEmpty()) {
            key = Joiner.on(KEY_SEPARATOR).skipNulls().join(type, id);
        } else {
            key = Joiner.on(KEY_SEPARATOR).skipNulls().join(
                    type,
                    id,
                    Joiner.on(',').skipNulls().join(chain));
        }
        return key;
    }

    @Override
    public String getWildCardKey() {
        return Joiner.on(KEY_SEPARATOR).skipNulls().join(type, id) + "*";
    }

    @Override
    public void updateInstanceByKey(String key) throws InvalidCacheKeyException {
        List<String> parts = Splitter.on(KEY_SEPARATOR).splitToList(key);
        if (parts.size() < 2) {
            throw new InvalidCacheKeyException(key, "Invalid number of key elements: " + parts.size());
        }

        type = BaseVertex.VertexType.valueOf(parts.get(0));
        id = parts.get(1);
        chain = null;
        if (parts.size() > 2) {
            chain = Splitter.on(',').splitToList(parts.get(2));
        }
    }

    @Override
    public Set<Long> runQuery() throws GroupQueryException {
        return graph.getAuthorizations(type, id, chain);
    }

    @Override
    public Set<Long> getInvalidResult() {
        return Collections.emptySet();
    }

    @Override
    public Collection<String> transformToCachable(Set<Long> value) {
        return Collections2.transform(value, new Function<Long, String>() {
            @Nullable
            @Override
            public String apply(Long item) {
                return Long.toString(item);
            }
        });
    }

    @Override
    public Set<Long> getFromCachable(Collection<String> value) throws Exception {
        Set<Long> authorizations = Sets.newTreeSet();
        try {
            for (String member : value) {
                authorizations.add(Long.parseLong(member, 10));
            }
            return authorizations;
        } catch (NumberFormatException e) {
            throw new Exception(e);
        }
    }
}
