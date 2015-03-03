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

package ezbake.services.provenance.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.services.provenance.graph.frames.*;
import ezbake.services.provenance.graph.frames.AgeOffRule;
import ezbake.services.provenance.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AddDocumentHelper {
    private static Logger logger = LoggerFactory.getLogger(GraphDb.class);

    private final EzSecurityToken securityToken;
    private final String uri;

    public AddDocumentHelper(EzSecurityToken securityToken, String uri) {
        this.securityToken = securityToken;
        this.uri = uri;
    }

    // create Document vertex
    public Document createDocumentVertex(TitanGraph graph, FramedGraph<TitanGraph> framedGraph, long id) throws ProvenanceDocumentExistsException {
        // create the Document vertex
        Iterator<Vertex> it = graph.query().has(Document.URI, uri).vertices().iterator();
        if (it.hasNext()) {
            throw new ProvenanceDocumentExistsException("This uri of the document already exists: " + uri);
        }
        Document doc = framedGraph.addVertex(null, Document.class);
        doc.updateProperties(securityToken, uri, id, false);

        return doc;
    }

    // add inheritance to document
    public void addDocumentInheritance(FramedGraph<TitanGraph> framedGraph, Document doc, List<InheritanceInfo> parents) throws
            ProvenanceParentDocumentNotFoundException, ProvenanceCircularInheritanceNotAllowedException {
        List<InheritanceInfo> inheritanceInfoList = new ArrayList<InheritanceInfo>();
        Map<InheritanceInfo, Document> parentMap = new HashMap<>();

        if (parents != null) {
            // the set of parent uri
            HashSet<String> parentExists = new HashSet<String>();
            List<String> parentNotFound = new ArrayList<>();

            for (InheritanceInfo info : parents) {
                if (info.parentUri.isEmpty()) {
                    logger.warn(String.format("Parent document URI empty when add document %s. Ingore this parent.", uri));
                    continue;
                }
                // self -parent not allowed
                if (info.parentUri.equals(doc.getUri())) {
                    throw new ProvenanceCircularInheritanceNotAllowedException("Document cannot have itself as parent");
                }

                Iterator<Document> docIt = framedGraph.query().has(Document.URI, info.parentUri).vertices(Document.class).iterator();
                if (!docIt.hasNext()) {
                    parentNotFound.add(info.parentUri);
                } else {
                    // no duplicate parent
                    if (parentExists.contains(info.parentUri)) {
                        logger.warn(String.format("Document %s already has %s as parent document. Ignore duplicate", uri, info.parentUri));
                        continue;
                    } else {
                        parentExists.add(info.parentUri);
                        inheritanceInfoList.add(info.deepCopy());
                    }
                    if (0 == parentNotFound.size()) {
                        parentMap.put(info, docIt.next());
                    }
                }
            }

            if (parentNotFound.size() > 0) {
                throw new ProvenanceParentDocumentNotFoundException("These parent URIs do not exist", parentNotFound);
            } else {
                // add DerivedFrom and AgeOff edge from parent to child doc
                for (Map.Entry<InheritanceInfo, Document> entry : parentMap.entrySet()) {
                    InheritanceInfo info = entry.getKey();
                    Document parentDoc = entry.getValue();
                    addInheritanceEdges(framedGraph, parentDoc, doc, info);
                }
            }
        }

        // set the inheritanceInfoList to be the filtered/validated list
        doc.setInheritanceInfoList(inheritanceInfoList);
    }

    public Map<AgeOffMapping, AgeOffRule> addAgeOffRules(FramedGraph<TitanGraph> framedGraph, Document doc, List<AgeOffMapping> ageOffRules, boolean validateOnly) throws ProvenanceAgeOffRuleNotFoundException {
        Map<AgeOffMapping, AgeOffRule> rules = new HashMap<>();

        if (ageOffRules != null) {
            // set of ageoff ruleId
            HashSet<Long> ageOffExists = new HashSet<Long>();
            for (AgeOffMapping mapping : ageOffRules) {
                if (ageOffExists.contains(mapping.getRuleId())) {
                    logger.warn(String.format("Document %s already has ageOff RuleId %s. Ignore duplicate", uri, mapping.getRuleId()));
                    continue;
                } else {
                    ageOffExists.add(mapping.getRuleId());
                }
                long ruleId = mapping.ruleId;
                Iterator<AgeOffRule> it = framedGraph.query().has(AgeOffRule.RuleId, ruleId).vertices(AgeOffRule.class).iterator();
                if (!it.hasNext()) {
                    throw new ProvenanceAgeOffRuleNotFoundException("Age off rule not found with id: " + ruleId);
                }
                AgeOffRule rule = it.next();
                if (validateOnly) {
                    rules.put(mapping, rule);
                } else {
                    addAgeOffEdge(framedGraph, rule, doc, mapping);
                }
            }
        }
        return rules;
    }

    // add AgeOff edge for Rule
    public void addAgeOffEdge(FramedGraph<TitanGraph> framedGraph, AgeOffRule rule, Document doc, AgeOffMapping mapping) {
        // add edge AgeOff from the AgeOffRule to the Document
        AgeOff ageOff = framedGraph.addEdge(null, rule.asVertex(), doc.asVertex(), AgeOff.LABEL, AgeOff.class);
        // use mapping relevantDate if set. otherwise, use current time.
        ageOff.updateProperties(securityToken, mapping.getRuleId(), Utils.convertDateTime2Date(mapping.getAgeOffRelevantDateTime()));
    }

    // add the DerivedFrom and AgeOff edges from inheritance
    public void addInheritanceEdges(FramedGraph<TitanGraph> framedGraph, Document parentDoc, Document document, InheritanceInfo info) {
        // add DerivedFrom edge from parent to child doc
        DerivedFrom derivedFrom = framedGraph.addEdge(null, parentDoc.asVertex(), document.asVertex(), DerivedFrom.LABEL, DerivedFrom.class);
        derivedFrom.updateProperties(securityToken);

        // add AgeOff edge from parent to child doc
        if (info.isSetInheritParentAgeOff() && info.inheritParentAgeOff) {
            HashMap<Long, Date> map = parentDoc.getInAgeOffEdgeRuleOldestRelevantDateMap();
            for (Long ruleId : map.keySet()) {
                // create the AgeOff edge from parent to doc
                AgeOff ageOff = framedGraph.addEdge(null, parentDoc.asVertex(), document.asVertex(), AgeOff.LABEL, AgeOff.class);
                // use inheritanceInfo relevantDate if set, otherwise, inherit from parent
                java.util.Date relevantDatetime = info.isSetAgeOffRelevantDateTime() ? Utils.convertDateTime2Date(info.ageOffRelevantDateTime) : map.get(ruleId);
                ageOff.updateProperties(securityToken, ruleId, relevantDatetime);
            }
        }
    }

    // returns the uri that are not involved in cycles in dependency order
    public List<String> getValidDocumentEntriesInOrder(final Map<String, AddDocumentEntry> documentsMap) {
        // build uri - parent map
        Map<String, Set<String>> parentMap = buildParentMap(documentsMap.values());
        // figure out all the roots (have no parent)
        Set<String> roots = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : parentMap.entrySet()) {
            if (entry.getValue().size() == 0) {
                roots.add(entry.getKey());
            }
        }

        // build uri - child map
        Map<String, Set<String>> childMap = buildChildMap(documentsMap.values());

        // build the trees
        List<String> nodes = new ArrayList<>();
        for (String root : roots) {
            nodes.add(root);
            nodes.addAll(getUrisInOrder(childMap, parentMap, root));
        }
        nodes.retainAll(documentsMap.keySet());
        parentMap.clear();
        childMap.clear();

        return nodes;
    }

    private Map<String, Set<String>> buildChildMap(final Collection<AddDocumentEntry> documents) {
        // uri to its children
        Map<String, Set<String>> childMap = new HashMap<>();

        // construct all uri - child -map
        for (AddDocumentEntry document : documents) {
            String uri = document.getUri();

            if (!childMap.containsKey(uri)) {
                childMap.put(uri, new HashSet<String>());
            }

            if (document.isSetParents()) {
                for (InheritanceInfo info : document.getParents()) {
                    String parentUri = info.getParentUri();

                    if (!childMap.containsKey(parentUri)) {
                        childMap.put(parentUri, new HashSet<String>());
                    }
                    childMap.get(parentUri).add(uri);
                }
            }
        }

        return childMap;
    }

    private Map<String, Set<String>> buildParentMap(final Collection<AddDocumentEntry> documents) {
        // uri to its children
        Map<String, Set<String>> parentMap = new HashMap<>();

        // construct all uri - child -map
        for (AddDocumentEntry document : documents) {
            String uri = document.getUri();

            if (!parentMap.containsKey(uri)) {
                parentMap.put(uri, new HashSet<String>());
            }

            if (document.isSetParents()) {
                for (InheritanceInfo info : document.getParents()) {
                    String parentUri = info.getParentUri();

                    if (!parentMap.containsKey(parentUri)) {
                        parentMap.put(parentUri, new HashSet<String>());
                    }
                    parentMap.get(uri).add(parentUri);
                }
            }
        }

        return parentMap;
    }

    private List<String> getUrisInOrder(Map<String, Set<String>> childMap, Map<String, Set<String>> parentMap, String parent) {
        List<String> uris = new ArrayList<>();

        Set<String> nextLayer = new HashSet<>();
        for (String child : childMap.get(parent)) {
            // add at the lowest level. i.e., if there are more parents in the list, hold for later
            parentMap.get(child).remove(parent);
            if (parentMap.get(child).size() == 0) {
                uris.add(child);
                nextLayer.add(child);
            }
        }

        // recursively add children
        for (String node : nextLayer) {
            uris.addAll(getUrisInOrder(childMap, parentMap, node));
        }

        return uris;
    }

}


