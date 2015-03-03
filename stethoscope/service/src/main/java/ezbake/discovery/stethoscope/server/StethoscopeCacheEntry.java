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

package ezbake.discovery.stethoscope.server;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class StethoscopeCacheEntry  {
    private String applicationName;
    private String serviceName;
    private long time;

    public StethoscopeCacheEntry(String applicationName, String serviceName, long time) {
        this.applicationName = applicationName;
        this.serviceName = serviceName;
        this.time = time;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getServiceName() {
        return serviceName;
    }

     @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }

        StethoscopeCacheEntry rhs = (StethoscopeCacheEntry) obj;
        return new EqualsBuilder()
                .append(applicationName, rhs.applicationName)
                .append(serviceName, rhs.serviceName)
                .append(time, rhs.time)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(applicationName)
                .append(serviceName)
                .append(time)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("applicationName", applicationName)
                .append("serviceName", serviceName)
                .append("time", time)
                .toString();
    }
}
