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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;

import java.util.Date;

/**
 * If documents are inserted with InheritenceInfo.inhertParentAgeOff == True, one edge of this type should be created running from the parent to the child vertex
 * for each of the inbound AgeOff edges running to the parent vertex. For each, the properties AgeOffRule and ageOffRelevantDateTime should be copied from the
 * corresponding edge running to the parent vertex.
 */
public interface AgeOff extends BaseEdge {

    public static final String LABEL = "AgeOff";
    public static final String Rule = "Rule";
    public static final String AgeOffRelevantDateTime = "AgeOffRelevantDateTime";

    /**
     * Rule
     * <p/>
     * The ID of the AgeOff rule that applies to this edge
     */
    @Property(Rule)
    public void setRuleId(long ruleId);

    @Property(Rule)
    public long getRuleId();

    /**
     * AgeOffRelevantDateTime
     * <p/>
     * The time to be used in age off computations
     */
    @Property(AgeOffRelevantDateTime)
    public void setAgeOffRelevantDateTime(Date timeStamp);

    @Property(AgeOffRelevantDateTime)
    public Date getAgeOffRelevantDateTime();

    @JavaHandler
    public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, long ruleId, Date ageOffRelevantDateTime);

    @JavaHandler
    public Document getInDocument();

    @JavaHandler
    public Document getOutDocument();

    public abstract class Impl extends BaseEdgeImpl implements AgeOff {

        public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, long ruleId, Date ageOffRelevantDateTime) {
            this.setRuleId(ruleId);
            this.setAgeOffRelevantDateTime(ageOffRelevantDateTime);
            super.updateCommonProperties(securityToken);
        }

        public Document getInDocument() {
            return frame(it().getVertex(Direction.IN), Document.class);
        }

        public Document getOutDocument() {
            return frame(it().getVertex(Direction.OUT), Document.class);
        }
    }
}
