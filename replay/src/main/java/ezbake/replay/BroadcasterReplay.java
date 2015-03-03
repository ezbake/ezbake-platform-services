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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.openshift.OpenShiftUtil;
import ezbake.common.properties.EzProperties;
import ezbake.data.common.TimeUtil;
import ezbake.data.common.TokenUtils;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.thrift.ThriftClientPool;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.util.AuditLoggerConfigurator;
import ezbake.warehaus.*;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcasterReplay extends EzBakeBaseThriftService implements ReplayService.Iface {
    private static Logger logger = LoggerFactory.getLogger(BroadcasterReplay.class);
    private EzProperties props;
    private HistoryData historyData = new HistoryData();
    protected ThriftClientPool pool = null;
    protected EzbakeSecurityClient securityClient;
    private static AuditLogger auditLogger;
    private static int REPLAY_INTERVAL_DEFAULT = 30; // 30 mins

    public void replay(String uri, DateTime start, DateTime finish, EzSecurityToken token, 
            String groupId, String topic, boolean replayLatestOnly, GetDataType type, int replayIntervalMinutes)
            throws org.apache.thrift.TException {
        TokenUtils.validateSecurityToken(token, props);
        String dn = token.getTokenPrincipal().getPrincipal();
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "replay");
        auditArgs.put("uri", uri);
        auditArgs.put("topic", topic);
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);
        run(groupId, uri, start, finish, topic, dn, replayLatestOnly, type, replayIntervalMinutes, token);
    }

    @Override
    public void shutdown() {
        if (pool != null) {
             pool.close();
        }
    }

    @Override
    public TProcessor getThriftProcessor() {
        props = new EzProperties(getConfigurationProperties(), true);
        pool = new ThriftClientPool(props);
        securityClient = new EzbakeSecurityClient(props);
        AuditLoggerConfigurator.setAdditivity(true);
        auditLogger = AuditLogger.getAuditLogger(BroadcasterReplay.class);
        return new ReplayService.Processor(this);
    }

    public boolean ping() {
        WarehausService.Client warehaus = null;
        try {
            warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
            return warehaus.ping();
        } catch (TException ex) {
            logger.error("Warehaus ping failed", ex);
            return false;
        } finally {
            if (warehaus != null) {
                pool.returnToPool(warehaus);
            }
        }
    }

    @Override
    public ReplayHistory getUserHistory(EzSecurityToken token) throws TException, NoReplayHistory {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getUserHistory");
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        TokenUtils.validateSecurityToken(token, props);
        String userDn = token.getTokenPrincipal().getPrincipal();
        ReplayHistory userHistory = historyData.getUserHistory(userDn);

        if (userHistory == null) {
            throw new NoReplayHistory(String.format("No history for user %s", userDn));
        }
        return userHistory;
    }

    @Override
    public void removeUserHistory(EzSecurityToken token, long timestamp) throws TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "removeUserHistory");
        auditLog(token, AuditEventType.FileObjectDelete, auditArgs);
        
        TokenUtils.validateSecurityToken(token, props);
        String userDn = token.getTokenPrincipal().getPrincipal();
        String timestampString = Long.toString(timestamp);
        historyData.removeEntry(userDn, timestampString);
    }

    protected void run(String groupId, String uriPrefix, DateTime start, DateTime finish,
                       String handle, String userDn, boolean replayLatestOnly, GetDataType type, 
                       int replayIntervalMinutes, EzSecurityToken token) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(uriPrefix), "uriPrefix cannot be null or empty!");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(handle), "handle cannot be null or empty!");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "groupId cannot be null or empty!");
        WarehausService.Client warehaus = null;
        List<DatedURI> listResults;

        // Use this to keep track of if the client has been returned to the pool (and closed)
        boolean returned = false;

        long dateEntry = new Date().getTime();
        long msgCounter = 0;
        EzBroadcaster ezbroadcaster = null;
        RequestHistory requestHistory = null;
        
        try {
            EzSecurityToken warehausToken = securityClient.fetchDerivedTokenForApp(token, pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME));
            ezbroadcaster = getBroadcaster(token, groupId, handle);


            String startTime = convertThriftTime(start);
            String finishTime = convertThriftTime(finish);

            requestHistory = historyData.addBroadcast(groupId, uriPrefix, startTime, finishTime, handle, userDn, dateEntry);
            
            if (replayIntervalMinutes == 0) {
                replayIntervalMinutes = REPLAY_INTERVAL_DEFAULT;
            }
            long startMillis = TimeUtil.convertFromThriftDateTime(start);
            long finishMillis = TimeUtil.convertFromThriftDateTime(finish);
            long replayIntervalMillis = replayIntervalMinutes * 60 * 1000; // minutes to millis
            long replayInterval = (finishMillis - startMillis) > replayIntervalMillis ? replayIntervalMillis : (finishMillis - startMillis);
            long endMillis = startMillis + replayInterval;
            int cumulativeTotal = 0;
            
            while (endMillis <= finishMillis) {
                warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
                returned = false;
                listResults = warehaus.replay(uriPrefix, replayLatestOnly,
                        TimeUtil.convertToThriftDateTime(startMillis), 
                        TimeUtil.convertToThriftDateTime(endMillis), type, warehausToken);

                // Return to pool to ensure the connection isn't dropped
                pool.returnToPool(warehaus);
                returned = true;
                cumulativeTotal = cumulativeTotal + listResults.size();
    
                requestHistory.setTotal(Integer.toString(cumulativeTotal));
                requestHistory.setStatus("broadcasting");
                historyData.updateEntry(userDn, dateEntry, requestHistory);
                logger.debug("timestamp " + String.valueOf(dateEntry) + " list results size=" + String.valueOf(cumulativeTotal));

                for (DatedURI result : listResults) {
                    String resultUri = result.getUri();
                    DateTime versionDate = result.getTimestamp();
                    long version = TimeUtil.convertFromThriftDateTime(versionDate);
                    try {
                        warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
                        returned = false;
                        BinaryReplay replayResult = warehaus.getParsed(resultUri, version, warehausToken);
                        pool.returnToPool(warehaus);
                        returned = true;

                        ezbroadcaster.broadcast(handle, result.getVisibility(), replayResult.getPacket());
                        requestHistory.setCount(Long.toString(++msgCounter));
                        requestHistory.setLastBroadcast(convertThriftTime(TimeUtil.convertToThriftDateTime(version)));
                        historyData.updateEntry(userDn, dateEntry, requestHistory);
                    } catch (EntryNotInWarehausException e) {
                        logger.warn("Entry not found for URI {}", resultUri);
                    }
                }
                if (endMillis == finishMillis) {
                    break;
                }
                startMillis = endMillis;
                endMillis = startMillis + replayInterval;
                if (endMillis > finishMillis) {
                    endMillis = finishMillis;
                }
            }
            logger.debug("replay complete");

            requestHistory.setTotal(Integer.toString(cumulativeTotal));
            if (cumulativeTotal == 0) {
                logger.warn("Replay query returned no results.");
                requestHistory.setStatus("no matches");
            } else {
                requestHistory.setStatus("complete");
            }
            historyData.updateEntry(userDn, dateEntry, requestHistory);
        } catch (TException ex) {
            updateStatus(requestHistory, ex.getMessage(), userDn, dateEntry);
            logger.error("TException caught msg:", ex);
        } catch (IOException ex) {
            updateStatus(requestHistory, ex.getMessage(), userDn, dateEntry);
            logger.error("IOException caught:", ex);
        } finally {
            if (pool != null && warehaus != null && !returned) {
                pool.returnToPool(warehaus);
            }
            try {
                if (ezbroadcaster != null) {
                    ezbroadcaster.close();
                }
            } catch (IOException ex) {
                logger.error("Issue closing broadcaster", ex);
            }
        }
    }

    private void updateStatus(RequestHistory request, String status, String userDn, long date) {
        if (!StringUtils.isEmpty(status)) {
            if (request != null) {
                request.setStatus(status);
            }
        }
        historyData.updateEntry(userDn, date, request);
    }

    private String convertThriftTime(DateTime datetime)
    {
        String returnTime = "";
        if (datetime != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss MM/dd/yyyy Z");
            long time = TimeUtil.convertFromThriftDateTime(datetime);
            returnTime = dateFormat.format(time);
        }
        return returnTime;
    }

    private EzBroadcaster getBroadcaster(EzSecurityToken token, String groupId, String topic) {
        EzBroadcaster ezbroadcaster = null;
        if (props.getBoolean(EzBroadcaster.PRODUCTION_MODE, true) || OpenShiftUtil.inOpenShiftContainer()) {
            EzLocksmith.Client locksmith = null;
            try {
                locksmith = pool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class);
                String locksmithSecurityId = pool.getSecurityId(EzLocksmithConstants.SERVICE_NAME);
                String key = locksmith.retrievePublicKey(securityClient.fetchDerivedTokenForApp(token, locksmithSecurityId), topic, null);
                ezbroadcaster = EzBroadcaster.create(props, groupId, key, topic, false);
            } catch (TException e) {
                throw new RuntimeException("Could not initialize broadcaster without key from locksmith", e);
            } finally {
                pool.returnToPool(locksmith);
            }
        } else {
            ezbroadcaster = EzBroadcaster.create(getConfigurationProperties(), groupId);
        }
        ezbroadcaster.registerBroadcastTopic(topic);
        return ezbroadcaster;
    }
    
    private void auditLog(EzSecurityToken userToken, AuditEventType eventType, 
            Map<String, String> args) {
        AuditEvent event = new AuditEvent(eventType, userToken);
        for (String argName : args.keySet()) {
            event.arg(argName, args.get(argName));
        }
        if (auditLogger != null) {
            auditLogger.logEvent(event);
        }
    }
}
