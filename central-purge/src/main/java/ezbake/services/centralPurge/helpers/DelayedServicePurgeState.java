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

package ezbake.services.centralPurge.helpers;

import ezbake.services.centralPurge.thrift.ServicePurgeState;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Created by jpercivall on 7/24/14.
 * This class is just a wrapper for the ServicePurgeState that implements Delayed. This allows
 * EzCentralPurgeServiceHandler to implement a delay queue to check if any running service
 * should be polled for an update.
 */
public class DelayedServicePurgeState implements Delayed {
    ServicePurgeState servicePurgeState;
    String applicationName;
    Long delayMillis;
    public ServicePurgeState getServicePurgeState() {
        return servicePurgeState;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getServiceName() {
        return serviceName;
    }

    String serviceName;

    public DelayedServicePurgeState(ServicePurgeState servicePurgeState, String applicationName, String serviceName){
        this.servicePurgeState=servicePurgeState;
        this.applicationName=applicationName;
        this.serviceName=serviceName;
        this.delayMillis=System.currentTimeMillis()+servicePurgeState.getPurgeState().getSuggestedPollPeriod();
    }
    @Override
    public long getDelay(TimeUnit unit) {
        long delay=delayMillis-System.currentTimeMillis();
        if(delay==0)
            return 0;
        switch (unit){
            case NANOSECONDS:   return delay*1000*1000;
            case MICROSECONDS: return delay*1000;
            case MILLISECONDS: return delay;
            case SECONDS: return delay/1000;
            case MINUTES: return delay/1000/60;
            case HOURS: return delay/1000/60/60;
            case DAYS: return delay/1000/60/60/24;
        }
        return 0;
    }

    @Override
    public int compareTo(Delayed otherDelayed) {
        long otherDelay= otherDelayed.getDelay(MICROSECONDS);
        long thisDelay=this.getDelay(MICROSECONDS);
        if(thisDelay < otherDelay){
            return -1;
        }
        else if (thisDelay>otherDelay){
            return 1;
        }
        else {
            return 0;
        }
    }
}