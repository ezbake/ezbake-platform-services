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

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.Initializer;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerContext;

import java.util.Date;

/**
 * This vertex type represents a rule used to age-off data.
 * In reality, the rules are really all the same, and what can be specified here is simply a name for the rule and a time window.
 * The time window is used to determine what data should be kept and what data should be aged-off according to this rule.
 * <p/>
 * For example, an age off rule might be named “PII Retention Window” and have a value of 30 days (this is only an example, I don’t know what the policy is for PII data).
 * <p/>
 * Thus, when an age off batch process is kicked-off (presumably with the current time as the input, though admins could choose a different time if needed),
 * any data older than 30 days should be removed from the system.
 */
public interface AgeOffRule extends BaseVertex {

    public static final String TYPE = "AgeOffRule";
    public static final String RuleId = "RuleId";
    public static final String RuleIdEs = "RuleIdEs";
    public static final String Name = "Name";
    public static final String Duration = "Duration";
    public static final String MaximumExecutionPeriod = "MaximumExecutionPeriod";

    /**
     * RuleId
     * An ID generated for this rule. AgeOff edges must reference this rule.
     */
    @Property(RuleId)
    public long getRuleId();

    @Property(RuleId)
    public void setRuleId(long id);

    @Property(RuleIdEs)
    public long getRuleIdEs();

    @Property(RuleIdEs)
    public void setRuleIdEs(long id);

    /**
     * Name
     * <p/>
     * The name to apply to this rule.
     * Note that all users/applications on the system may be able to read this name, so do not choose a name that would need to be classified
     * higher than the minimum classification level required to access the network.
     */
    @Property(Name)
    public String getName();

    @Property(Name)
    public void setName(String name);

    /**
     * Duration
     * The time duration for which data falling under this rule may be retained in the system. The units for Duration shall be seconds.
     */
    @Property(Duration)
    public long getDuration();

    @Property(Duration)
    public void setDuration(long duration);

    /**
     * MaximumExecutionPeriod
     * The maximum allowable period (in days) between automatic execution of an age off for this rule. Allowed values are 1-90
     */
    @Property(MaximumExecutionPeriod)
    public int getMaximumExecutionPeriod();

    @Property(MaximumExecutionPeriod)
    public void setMaximumExecutionPeriod(int maxPeriodDays);

    @JavaHandler
    public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, String name, long id, long duration, int maxPeriod);


    public abstract class Impl extends BaseVertexImpl implements AgeOffRule {

        @Override
        public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, String name, long id, long duration, int maxPeriod) {
            this.setDuration(duration);
            this.setName(name);
            this.setRuleId(id);
            this.setRuleIdEs(id);
            this.setMaximumExecutionPeriod(maxPeriod);
            super.updateCommonProperties(securityToken, TYPE);
        }
    }
}
