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

package ezbake.services.provenance.graph.frames;

import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import ezbake.services.provenance.thrift.InheritanceInfo;

import java.util.*;

/**
 * This vertex type represents a document.
 */
public interface Document extends BaseVertex {

    public static final String TYPE = "Document";
    public static final String URI = "URI";
    public static final String DocumentId = "DocumentId";
    public static final String DocumentIdEs = "DocumentIdEs";
    public static final String Aged = "Aged";
    public static final String InheritanceInfoList = "InheritanceInfoList";

    /**
     * URI
     * The URI is the unique name for the document throughout the system. It is generated external to the provenance service.
     */
    @Property(URI)
    public void setUri(String uri);

    @Property(URI)
    public String getUri();

    /**
     * DocumentId
     * This is a 64-bit number unique to this document. These IDs should start at 0 and increase as new documents are added over time with no to minimal gaps in assignment.
     */
    @Property(DocumentId)
    public void setDocumentId(long documentId);

    @Property(DocumentId)
    public long getDocumentId();

    @Property(DocumentIdEs)
    public void setDocumentIdEs(long documentId);

    @Property(DocumentIdEs)
    public long getDocumentIdEs();

    /**
     * Aged
     * A boolean indicating whether this vertex has been aged-off.
     */
    @Property(Aged)
    public void setAged(boolean aged);

    @Property(Aged)
    public Boolean getAged();

    /**
     * InheritanceInfoList
     * A copy of the <list>InheritanceInfo that is passed in at document creation time.
     */
    @Property(InheritanceInfoList)
    public void setInheritanceInfoListInternal(InheritanceInfo[] inheritanceInfos);

    @Property(InheritanceInfoList)
    public InheritanceInfo[] getInheritanceInfoListInternal();

    @JavaHandler
    public void setInheritanceInfoList(List<InheritanceInfo> inheritanceInfos);

    @JavaHandler
    public List<InheritanceInfo> getInheritanceInfoList();

    @JavaHandler
    public InheritanceInfo getInheritanceInfoOfParent(String parentUri);

    @JavaHandler
    public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, String uri, long documentId, boolean aged);

    @JavaHandler
    public Iterator<AgeOff> getInAgeOffEdges();

    @JavaHandler
    public Iterator<AgeOff> getInAgeOffEdgesFromParent(String parentUri);

    @JavaHandler
    public Iterator<AgeOff> getInAgeOffEdgesFromParentHasRule(String parentUri, long ruleId);

    @JavaHandler
    public Iterator<AgeOff> getInAgeOffEdgesFromAgeOffRule(long ruleId);

    @JavaHandler
    public Iterator<AgeOff> getOutAgeOffEdgesHasRule(long ruleId);

    @JavaHandler
    public Iterator<Document> getParentDocuments();

    @JavaHandler
    public Iterator<Document> getChildDocuments();

    @JavaHandler
    public Date getInAgeOffOldestRelevantDateHasRule(long ruleId);

    @JavaHandler
    public void resetInheritanceInfo(String parentUri);

    @JavaHandler
    public void addInheritanceInfo(InheritanceInfo info);

    @JavaHandler
    public HashMap<Long, Date> getInAgeOffEdgeRuleOldestRelevantDateMap();

    public abstract class Impl extends BaseVertexImpl implements Document {

        public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, String uri, long documentId, boolean aged) {
            this.setUri(uri);
            this.setDocumentId(documentId);
            this.setDocumentIdEs(documentId);
            this.setAged(aged);
            super.updateCommonProperties(securityToken, TYPE);
        }

        public void setInheritanceInfoList(List<InheritanceInfo> inheritanceInfos) {
            this.setInheritanceInfoListInternal(inheritanceInfos.toArray(new InheritanceInfo[inheritanceInfos.size()]));
        }

        public List<InheritanceInfo> getInheritanceInfoList() {
            return new ArrayList<InheritanceInfo>(Arrays.asList(this.getInheritanceInfoListInternal()));
        }

        // get the InheritanceInfo maps to parentUri
        public InheritanceInfo getInheritanceInfoOfParent(String parentUri) {
            for (InheritanceInfo info : this.getInheritanceInfoList()) {
                if (info.getParentUri().equals(parentUri)) {
                    return info;
                }
            }
            return null;
        }

        // get all in AgeOff edges
        public Iterator<AgeOff> getInAgeOffEdges() {
            return frameEdges(gremlin().inE(AgeOff.LABEL), AgeOff.class).iterator();
        }

        // get in AgeOff edges from Document with parentUri
        public Iterator<AgeOff> getInAgeOffEdgesFromParent(String parentUri) {
            GremlinPipeline pipe = new GremlinPipeline();
            return frameEdges(pipe.start(it()).inE(AgeOff.LABEL).as("edge").outV().has(URI, parentUri).back("edge"), AgeOff.class).iterator();
        }

        // get in AgeOff edges with rule ruleId from Document with parentUri
        public Iterator<AgeOff> getInAgeOffEdgesFromParentHasRule(String parentUri, long ruleId) {
            GremlinPipeline pipe = new GremlinPipeline();
            return frameEdges(pipe.start(it()).inE(AgeOff.LABEL).as("edge").has(AgeOff.Rule, ruleId).outV().has(URI, parentUri).back("edge"), AgeOff.class).iterator();
        }

        // get out AgeOff edges with rule ruleId
        public Iterator<AgeOff> getOutAgeOffEdgesHasRule(long ruleId) {
            GremlinPipeline pipe = new GremlinPipeline();
            return frameEdges(pipe.start(it()).outE(AgeOff.LABEL).has(AgeOff.Rule, ruleId), AgeOff.class).iterator();
        }

        // get in AgeOff edges with rule ruleId
        public Iterator<AgeOff> getInAgeOffEdgesFromAgeOffRule(long ruleId) {
            GremlinPipeline pipe = new GremlinPipeline();
            return frameEdges(pipe.start(it()).inE(AgeOff.LABEL).as("edge").outV().has(AgeOffRule.RuleId, ruleId).back("edge"), AgeOff.class).iterator();
        }

        // get all Document vertex I derive from
        public Iterator<Document> getParentDocuments() {
            return frameVertices(gremlin().inE(DerivedFrom.LABEL).outV(), Document.class).iterator();
        }

        // get all Document vertex derived from me
        public Iterator<Document> getChildDocuments() {
            return frameVertices(gremlin().outE(DerivedFrom.LABEL).inV(), Document.class).iterator();
        }

        // reset the inheritanceInfo flags matching parentUri
        public void resetInheritanceInfo(String parentUri) {
            List<InheritanceInfo> inheritanceInfos = this.getInheritanceInfoList();
            for (InheritanceInfo info : inheritanceInfos) {
                if (info.getParentUri().equals(parentUri)) {
                    info.setInheritParentAgeOff(false);
                    info.setTrackParentAgeOff(false);
                }
            }
            this.setInheritanceInfoList(inheritanceInfos);
        }

        // add inheritanceInfo to the list
        public void addInheritanceInfo(InheritanceInfo info) {
            List<InheritanceInfo> inheritanceInfos = this.getInheritanceInfoList();
            inheritanceInfos.add(info);
            this.setInheritanceInfoList(inheritanceInfos);
        }

        // get the oldest relevantDate of all the in AgeOff edges with rule ruleId
        public Date getInAgeOffOldestRelevantDateHasRule(long ruleId) {
            GremlinPipeline pipe = new GremlinPipeline();
            Iterator<AgeOff> edges = frameEdges(pipe.start(it()).inE(AgeOff.LABEL).has(AgeOff.Rule, ruleId), AgeOff.class).iterator();
            Date oldestRelevantDate = null; // if don't have any edge
            while (edges.hasNext()) {
                Date relevantDate = edges.next().getAgeOffRelevantDateTime();
                if (oldestRelevantDate == null) {
                    oldestRelevantDate = relevantDate;
                } else if (relevantDate.before(oldestRelevantDate)) {
                    oldestRelevantDate = relevantDate;
                }
            }
            return oldestRelevantDate;
        }

        // get a map <ruleId, oldestRelevantDate> of all in AgeOff edges
        public HashMap<Long, Date> getInAgeOffEdgeRuleOldestRelevantDateMap() {
            // get all in AgeOff edges
            Iterator<AgeOff> ageOffIt = this.getInAgeOffEdges();
            // use hash to reduce duplicated AgeOff Rule
            // key = ruleId, value = relevantDate
            HashMap<Long, Date> map = new HashMap<Long, Date>();
            while (ageOffIt.hasNext()) {
                AgeOff ageOff = ageOffIt.next();
                long ruleId = ageOff.getRuleId();
                Date relevantDate = ageOff.getAgeOffRelevantDateTime();

                if (map.containsKey(ruleId)) {
                    // rule already exists, update time if smaller
                    if (relevantDate.before(map.get(ruleId))) {
                        map.put(ruleId, relevantDate);
                    }
                } else {
                    map.put(ruleId, relevantDate);
                }
            }

            return map;
        }
    }
}
