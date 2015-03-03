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

package ezbake.replay;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HistoryData {
    private final Lock lock = new ReentrantLock();
    private final Map<String, ReplayHistory> users = Maps.newHashMap();
    private Logger logger = LoggerFactory.getLogger(HistoryData.class);

    public RequestHistory addBroadcast(String groupId, String uri, String start, String finish,
                             String topic, String userDn, long date) {
        RequestHistory history = new RequestHistory();
        history.setCount("0");
        history.setUri(uri);
        history.setStart(start);
        history.setFinish(finish);
        history.setTopic(topic);
        history.setGroupId(groupId);
        history.setTotal("retrieving");
        history.setStatus("pending");
        logger.debug("add history for " + userDn + " uri = " + uri + "start = " + start);
        updateEntry(userDn, date, history);
        return history;
    }

    public void updateEntry(String userDn, long date, RequestHistory history) {
        lock.lock();
        try {
            ReplayHistory replayHistory = users.get(userDn);
            if (replayHistory == null) {
                replayHistory = new ReplayHistory();
                users.put(userDn, replayHistory);
            }

            try {
                // If we get a full valid object just put it in the history
                history.validate();
                replayHistory.putToReplayHistory(Long.toString(date), history);
            } catch (TException e) {
                // Update the pieces that we have
                RequestHistory toUpdate = replayHistory.getReplayHistory().get(Long.toString(date));
                if (toUpdate == null) {
                    throw new IllegalStateException("Cannot update with partial entry if no entry exists");
                }
                if (history.isSetCount()) {
                    toUpdate.setCount(history.getCount());
                }
                if (history.isSetFinish()) {
                    toUpdate.setFinish(history.getFinish());
                }
                if (history.isSetGroupId()) {
                    toUpdate.setGroupId(history.getGroupId());
                }
                if (history.isSetStart()) {
                    toUpdate.setStart(history.getStart());
                }
                if (history.isSetStatus()) {
                    toUpdate.setStatus(history.getStatus());
                }
                if (history.isSetTopic()) {
                    toUpdate.setTopic(history.getTopic());
                }
                if (history.isSetTotal()) {
                    toUpdate.setTotal(history.getTotal());
                }
                if (history.isSetUri()) {
                    toUpdate.setUri(history.getUri());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public ReplayHistory getUserHistory(final String userDn) {
        Preconditions.checkNotNull(userDn);
        lock.lock();
        try {
           return users.get(userDn);
        } finally {
            lock.unlock();
        }
    }

    public void removeEntry(String userDn, String timestamp) {
        lock.lock();
        try {
            ReplayHistory userHistory = users.get(userDn);
            if (userHistory != null) {
                userHistory.getReplayHistory().remove(timestamp);
                if (userHistory.getReplayHistorySize() == 0) {
                    users.remove(userDn);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}


