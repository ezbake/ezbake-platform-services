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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import ezbake.base.thrift.Date;
import ezbake.base.thrift.*;
import ezbake.base.thrift.TimeZone;
import ezbake.services.centralPurge.thrift.*;
import ezbake.services.provenance.thrift.PurgeInfo;
import ezbake.services.provenance.thrift.PurgeInitiationResult;
import ezbake.util.AuditEvent;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by jpercivall on 7/18/14.
 */
public class EzCentralPurgeServiceHelpers {

    private static final String
            ToBePurged="toBePurged",
            AppStates="appStates",
            CompletelyPurged="completelyPurged",
            AgeOffRuleId ="ageoffruleid",
            AgeOffEventInfoString ="ageoffeventinfo",
            TimeCreated = "timecreated",
            PurgeBitVector="purgeBitvector",
            User="user",
            Resolved="resolved",
            PurgeStateString ="state",
            TimeInitiated="timeinitiated",
            TimeLastPoll="timelastpoll",
            URI = "uri",
            Hour = "hour",
            Minute = "minute",
            AfterUTC = "afterUTC",
            Year = "year",
            Month = "month",
            Day = "day",
            Second = "second",
            Millisecond = "millisecond",
            Timezone = "timezone",
            ApplicationPurgeStatus = "applicationpurgestatus",
            NotPurgedSet = "notpurgedset",
            SuggestedPollPeriord = "suggestedpollperiod",
            TimeStampString = "timestamp",
            ServiceName = "servicename",
            PurgeInfo = "purgeinfo",
            CentralPurgeTypeString = "centralpurgeinfo",
            Description = "description",
            URIsNotFoundSet = "urisnotfoundset",
            PurgeName = "purgename",
            ApplicationName = "applicationname",
            ApplicationPurgeStateString = "applicationpurgestate",
            URIsFoundSet = "urisfoundset",
            Purged = "purged",
            Cancel_Status = "cancelstatus";


    public static final String PurgeId="purgeId",
            CentralPurgeStateString="centralpurgestate",
            CentralAgeOffStateString="centralageoffstate",
            CentralPurgeStatusString="centralPurgestatus",
            AgeOffEventId ="ageoffeventid";

    public static BasicDBList encodeURISet(Set<Long> longSet){
        BasicDBList basicDBList = new BasicDBList();

        for( Long uri : longSet){
            BasicDBObject dbURI = new BasicDBObject();
            dbURI.append(URI,uri);
            basicDBList.add(dbURI);
        }
        return basicDBList;
    }

    public static Set<Long> decodeURISet(BasicDBList encodedLongSet){
        Set<Long> uriSet = new HashSet<>();
        for(Object uriObject : encodedLongSet){
            Long uri = ((BasicDBObject) uriObject).getLong(URI);
            uriSet.add(uri);
        }
        return uriSet;
    }


    public static DBObject encodeDateTime(DateTime dateTime){
        BasicDBObjectBuilder timeZoneBuilder = BasicDBObjectBuilder.start();

        Date date = dateTime.getDate();
        Time time = dateTime.getTime();
        TimeZone timeZone=time.getTz();

        timeZoneBuilder.add(Hour,timeZone.getHour());
        timeZoneBuilder.add(Minute,timeZone.getMinute());
        timeZoneBuilder.add(AfterUTC,timeZone.isAfterUTC());


        BasicDBObjectBuilder basicDBObjectBuilder=BasicDBObjectBuilder.start()
                .add(Year,date.getYear())
                .add(Month,date.getMonth())
                .add(Day,date.getDay())
                .add(Hour,time.getHour())
                .add(Minute,time.getMinute())
                .add(Second,time.getSecond())
                .add(Millisecond,time.getMillisecond())
                .add(Timezone,timeZoneBuilder.get());


        return basicDBObjectBuilder.get();
    }
    public static DateTime decodeDateTime(DBObject basicDBObject){
        Date date = new Date();
        Time time = new Time();
        TimeZone timeZone = new TimeZone();

        date.setYear(((Integer) basicDBObject.get(Year)).shortValue());
        date.setMonth(((Integer) basicDBObject.get(Month)).shortValue());
        date.setDay(((Integer) basicDBObject.get(Day)).shortValue());

        time.setHour(((Integer) basicDBObject.get(Hour)).shortValue());
        time.setMinute(((Integer) basicDBObject.get(Minute)).shortValue());
        time.setSecond(((Integer) basicDBObject.get(Second)).shortValue());
        time.setMillisecond(((Integer) basicDBObject.get(Millisecond)).shortValue());

        DBObject dbObjectTimeZone = (DBObject) basicDBObject.get(Timezone);
        timeZone.setHour(((Integer) dbObjectTimeZone.get(Hour)).shortValue());
        timeZone.setMinute(((Integer) dbObjectTimeZone.get(Minute)).shortValue());
        timeZone.setAfterUTC((Boolean) dbObjectTimeZone.get(AfterUTC));

        DateTime dateTime = new DateTime();
        dateTime.setDate(date);
        time.setTz(timeZone);
        dateTime.setTime(time);

        return dateTime;
    }

    public static DBObject encodePurgeInitiationResult(PurgeInitiationResult purgeInitiationResult){
        BasicDBList basicDBList = new BasicDBList();
        for( String uri : purgeInitiationResult.getUrisNotFound()){
            BasicDBObject dbURI = new BasicDBObject();
            dbURI.append(URI,uri);
            basicDBList.add(dbURI);
        }

        DBObject setToBePurged = encodeURISet(purgeInitiationResult.getToBePurged());

        BasicDBObjectBuilder purgeInitiationResultBuilder = BasicDBObjectBuilder.start()
                .add(PurgeId, purgeInitiationResult.getPurgeId())
                .add(ToBePurged,setToBePurged)
                .add(URIsNotFoundSet,basicDBList);

        return purgeInitiationResultBuilder.get();
    }
    public static PurgeInitiationResult decodePurgeInitiationResult(DBObject purgeInitiationResultEncoded){
        PurgeInitiationResult purgeInitiationResultDecoded = new PurgeInitiationResult();
        purgeInitiationResultDecoded.setPurgeId((Long) purgeInitiationResultEncoded.get(PurgeId));

        List<String> urisNotFound = new LinkedList<>();
        BasicDBList basicDBList = (BasicDBList) purgeInitiationResultEncoded.get(URIsNotFoundSet);
        for(Object uriObject : basicDBList){
            String uri = ((BasicDBObject) uriObject).getString(URI);
            urisNotFound.add(uri);
        }
        purgeInitiationResultDecoded.setUrisNotFound(urisNotFound);

        Set<Long> toBePurged = decodeURISet((BasicDBList) purgeInitiationResultEncoded.get(ToBePurged));
        purgeInitiationResultDecoded.setToBePurged(toBePurged);

        return purgeInitiationResultDecoded;
    }

    public static DBObject encodePurgeState(PurgeState purgeState){
        BasicDBObjectBuilder purgeStateBuilder = BasicDBObjectBuilder.start()
                .add(PurgeId,purgeState.getPurgeId())
                .add(ApplicationPurgeStatus,purgeState.getPurgeStatus().getValue())
                .add(Purged,encodeURISet(purgeState.getPurged()))
                .add(NotPurgedSet, encodeURISet(purgeState.getNotPurged()))
                .add(SuggestedPollPeriord,purgeState.getSuggestedPollPeriod())
                .add(TimeStampString,encodeDateTime(purgeState.getTimeStamp()))
                .add(Cancel_Status,purgeState.getCancelStatus().getValue());

        return purgeStateBuilder.get();
    }
    public static PurgeState decodePurgeState(DBObject purgeStateEncoded){
        PurgeState purgeState = new PurgeState();

        purgeState.setPurgeId((Long) purgeStateEncoded.get(PurgeId));
        purgeState.setPurgeStatus(PurgeStatus.findByValue((Integer) purgeStateEncoded.get(ApplicationPurgeStatus)));
        purgeState.setCancelStatus(CancelStatus.findByValue((Integer) purgeStateEncoded.get(Cancel_Status)));
        purgeState.setPurged(decodeURISet((BasicDBList) purgeStateEncoded.get(Purged)));
        purgeState.setNotPurged(decodeURISet((BasicDBList) purgeStateEncoded.get(NotPurgedSet)));
        purgeState.setSuggestedPollPeriod((Integer) purgeStateEncoded.get(SuggestedPollPeriord));
        purgeState.setTimeStamp(decodeDateTime((DBObject) purgeStateEncoded.get(TimeStampString)));

        return purgeState;
    }

    public static DBObject encodeServicePurgeState(ServicePurgeState servicePurgeState){
        BasicDBObjectBuilder servicePurgeStateBuilder = BasicDBObjectBuilder.start()
                .add(PurgeStateString, encodePurgeState(servicePurgeState.getPurgeState()))
                .add(TimeInitiated, encodeDateTime(servicePurgeState.getTimeInitiated()))
                .add(TimeLastPoll,encodeDateTime(servicePurgeState.getTimeLastPoll()));

        return servicePurgeStateBuilder.get();
    }
    public static ServicePurgeState decodeServicePurgeState(DBObject servicePurgeStateEncoded){
        ServicePurgeState servicePurgeStateDecoded = new ServicePurgeState();

        servicePurgeStateDecoded.setPurgeState(decodePurgeState((DBObject) servicePurgeStateEncoded.get(PurgeStateString)));
        servicePurgeStateDecoded.setTimeInitiated(decodeDateTime((DBObject) servicePurgeStateEncoded.get(TimeInitiated)));
        servicePurgeStateDecoded.setTimeLastPoll(decodeDateTime((DBObject) servicePurgeStateEncoded.get(TimeLastPoll)));

        return servicePurgeStateDecoded;
    }

    public static BasicDBList encodeApplicationPurgeState(ApplicationPurgeState applicationPurgeState){
        BasicDBList basicDBList = new BasicDBList();
        Map<String,ServicePurgeState> servicePurgeStateMap = applicationPurgeState.getServicePurgestates();

        for( String serviceName : servicePurgeStateMap.keySet()){
            BasicDBObject dbServicePurgestate = new BasicDBObject();
            dbServicePurgestate.append(ServiceName, serviceName);
            dbServicePurgestate.append(PurgeStateString, encodeServicePurgeState(servicePurgeStateMap.get(serviceName)));
            basicDBList.add(dbServicePurgestate);
        }

        return basicDBList;
    }
    public static ApplicationPurgeState decodeApplicationPurgeState(BasicDBList applicationPurgeStateEncoded){
        ApplicationPurgeState applicationPurgeStateDecoded = new ApplicationPurgeState();

        Map<String,ServicePurgeState> servicePurgeMap = new HashMap();

        for( Object servicePurgeState_obj : applicationPurgeStateEncoded){
            DBObject servicePurgeState_v1 = (DBObject) servicePurgeState_obj;
            servicePurgeMap.put((String) servicePurgeState_v1.get(ServiceName),decodeServicePurgeState((DBObject) servicePurgeState_v1.get(PurgeStateString)));
        }

        applicationPurgeStateDecoded.setServicePurgestates(servicePurgeMap);

        return applicationPurgeStateDecoded;
    }

    public static DBObject encodeAgeOffEventInfo(AgeOffEventInfo ageOffEventInfo){
        BasicDBObjectBuilder ageOffEventInfoBuilder = BasicDBObjectBuilder.start()
                .add(User, ageOffEventInfo.getUser())
                .add(CompletelyPurged,encodeURISet(ageOffEventInfo.getCompletelyPurgedSet()))
                .add(AgeOffEventId,ageOffEventInfo.getId())
                .add(PurgeBitVector,encodeURISet(ageOffEventInfo.getPurgeSet()))
                .add(TimeCreated,encodeDateTime(ageOffEventInfo.getTimeCreated()))
                .add(Description,ageOffEventInfo.getDescription())
                .add(Resolved,ageOffEventInfo.isResolved());
        return ageOffEventInfoBuilder.get();
    }
    public static AgeOffEventInfo decodeAgeOffEventInfo(DBObject ageOffEventInfoEncoded){
        AgeOffEventInfo ageOffEventInfoDecoded = new AgeOffEventInfo();

        ageOffEventInfoDecoded.setId((Long) ageOffEventInfoEncoded.get(AgeOffEventId));
        ageOffEventInfoDecoded.setCompletelyPurgedSet(decodeURISet((BasicDBList) ageOffEventInfoEncoded.get(CompletelyPurged)));
        ageOffEventInfoDecoded.setPurgeSet(decodeURISet((BasicDBList) ageOffEventInfoEncoded.get(PurgeBitVector)));
        ageOffEventInfoDecoded.setUser((String) ageOffEventInfoEncoded.get(User));
        ageOffEventInfoDecoded.setTimeCreated(decodeDateTime((DBObject) ageOffEventInfoEncoded.get(TimeCreated)));
        ageOffEventInfoDecoded.setDescription((String) ageOffEventInfoEncoded.get(Description));
        ageOffEventInfoDecoded.setResolved((Boolean) ageOffEventInfoEncoded.get(Resolved));

        return ageOffEventInfoDecoded;
    }

    public static BasicDBList encodeApplicationStateMap(Map<String,ApplicationPurgeState> appMap) {
        BasicDBList basicDBList = new BasicDBList();

        for( String key :  appMap.keySet()){

            ApplicationPurgeState applicationPurgeState = appMap.get(key);
            BasicDBObject dbApplicationPurgeState = new BasicDBObject();
            dbApplicationPurgeState.append(ApplicationName,key);
            dbApplicationPurgeState.append(ApplicationPurgeStateString,encodeApplicationPurgeState(applicationPurgeState));
            basicDBList.add(dbApplicationPurgeState);
        }

        return basicDBList;
    }
    public static Map<String,ApplicationPurgeState> decodeApplicationStateMap(BasicDBList encodedAppMap){
        Map<String,ApplicationPurgeState > appMap= new HashMap<>();
        for( Object appMap_obj : encodedAppMap){
            DBObject appMap_v1 = (DBObject) appMap_obj;
            appMap.put((String) appMap_v1.get(ApplicationName),decodeApplicationPurgeState((BasicDBList) appMap_v1.get(ApplicationPurgeStateString)));
        }

        return appMap;
    }

    public static DBObject encodeCentralAgeOffEventState(CentralAgeOffEventState centralAgeOffEventState){
        BasicDBObjectBuilder centralAgeOffEventStateBuilder = BasicDBObjectBuilder.start()
                .add(AgeOffEventInfoString,encodeAgeOffEventInfo(centralAgeOffEventState.getAgeOffEventInfo()))
                .add(AppStates, encodeApplicationStateMap(centralAgeOffEventState.getApplicationStates()))
                .add(CentralPurgeStatusString,centralAgeOffEventState.getCentralStatus().getValue())
                .add(AgeOffRuleId,centralAgeOffEventState.getAgeOffRuleId());
        return centralAgeOffEventStateBuilder.get();
    }
    public static CentralAgeOffEventState decodeCentralAgeOffEventState (DBObject encodedCentralAgeOffEventState){
        CentralAgeOffEventState centralAgeOffEventStateDecoded = new CentralAgeOffEventState();

        centralAgeOffEventStateDecoded.setAgeOffEventInfo(decodeAgeOffEventInfo((DBObject) encodedCentralAgeOffEventState.get(AgeOffEventInfoString)));
        centralAgeOffEventStateDecoded.setApplicationStates(decodeApplicationStateMap((BasicDBList) encodedCentralAgeOffEventState.get(AppStates)));
        centralAgeOffEventStateDecoded.setCentralStatus(CentralPurgeStatus.findByValue((Integer) encodedCentralAgeOffEventState.get(CentralPurgeStatusString)));
        centralAgeOffEventStateDecoded.setAgeOffRuleId((Long) encodedCentralAgeOffEventState.get(AgeOffRuleId));

        return centralAgeOffEventStateDecoded;
    }

    public static DBObject encodePurgeInfo(PurgeInfo purgeInfo){
        BasicDBList basicDBListUris = new BasicDBList();

        for( String uri :  purgeInfo.getDocumentUris()){
            BasicDBObject dbURI = new BasicDBObject();
            dbURI.append(URI,uri);
            basicDBListUris.add(dbURI);
        }

        BasicDBList basicDBListUrisNotFound = new BasicDBList();

        for( String uriNotFound :  purgeInfo.getDocumentUrisNotFound()){
            BasicDBObject dbURINotFound = new BasicDBObject();
            dbURINotFound.append(URI,uriNotFound);
            basicDBListUris.add(dbURINotFound);
        }

        BasicDBObjectBuilder purgeInfoBuilder = BasicDBObjectBuilder.start()
                .add(PurgeId,purgeInfo.getId())
                .add(PurgeBitVector,encodeURISet(purgeInfo.getPurgeDocumentIds()))
                .add(Description,purgeInfo.getDescription())
                .add(PurgeName,purgeInfo.getName())
                .add(User,purgeInfo.getUser())
                .add(CompletelyPurged,encodeURISet(purgeInfo.getCompletelyPurgedDocumentIds()))
                .add(Resolved,purgeInfo.isResolved())
                .add(URIsFoundSet,basicDBListUris)
                .add(URIsNotFoundSet,basicDBListUrisNotFound)
                .add(TimeStampString,encodeDateTime(purgeInfo.getTimeStamp()));
        return purgeInfoBuilder.get();
    }
    public static PurgeInfo decodePurgeInfo(DBObject purgeInfoEncoded){
        PurgeInfo purgeInfoDecoded = new PurgeInfo();

        List<String> documentURIs = new LinkedList<>();

        for( Object uri_obj : (BasicDBList) purgeInfoEncoded.get(URIsFoundSet)){
            DBObject uri_v1 = (DBObject) uri_obj;
            documentURIs.add((String) uri_v1.get(URI));
        }
        List<String> urisNotFound = new LinkedList<>();

        for( Object uriNotFound_obj : (BasicDBList) purgeInfoEncoded.get(URIsNotFoundSet)){
            DBObject uriNotFound_v1 = (DBObject) uriNotFound_obj;
            documentURIs.add((String) uriNotFound_v1.get(URI));
        }

        purgeInfoDecoded.setDocumentUris(documentURIs);
        purgeInfoDecoded.setDocumentUrisNotFound(urisNotFound);
        purgeInfoDecoded.setId((Long) purgeInfoEncoded.get(PurgeId));
        purgeInfoDecoded.setPurgeDocumentIds(decodeURISet((BasicDBList) purgeInfoEncoded.get(PurgeBitVector)));
        purgeInfoDecoded.setDescription((String) purgeInfoEncoded.get(Description));
        purgeInfoDecoded.setName((String) purgeInfoEncoded.get(PurgeName));
        purgeInfoDecoded.setUser((String) purgeInfoEncoded.get(User));
        purgeInfoDecoded.setCompletelyPurgedDocumentIds(decodeURISet((BasicDBList) purgeInfoEncoded.get(CompletelyPurged)));
        purgeInfoDecoded.setResolved((Boolean) purgeInfoEncoded.get(Resolved));
        purgeInfoDecoded.setTimeStamp(decodeDateTime((DBObject) purgeInfoEncoded.get(TimeStampString)));

        return purgeInfoDecoded;
    }

    public static DBObject encodeCentralPurgeState (CentralPurgeState centralPurgeState){
        BasicDBObjectBuilder centralPurgeStateBuilder = BasicDBObjectBuilder.start()
                .add(PurgeInfo,encodePurgeInfo(centralPurgeState.getPurgeInfo()))
                .add(AppStates, encodeApplicationStateMap(centralPurgeState.getApplicationStates()))
                .add(CentralPurgeStatusString,centralPurgeState.getCentralStatus().getValue())
                .add(CentralPurgeTypeString,centralPurgeState.getCentralPurgeType().getValue());

        return centralPurgeStateBuilder.get();
    }
    public static CentralPurgeState decodeCentralPurgeState (DBObject centralPurgeStateEncoded){
        CentralPurgeState centralPurgeStateDecoded = new CentralPurgeState();

        centralPurgeStateDecoded.setApplicationStates(decodeApplicationStateMap((BasicDBList) centralPurgeStateEncoded.get(AppStates)));
        centralPurgeStateDecoded.setPurgeInfo(decodePurgeInfo((DBObject) centralPurgeStateEncoded.get(PurgeInfo)));
        centralPurgeStateDecoded.setCentralStatus(CentralPurgeStatus.findByValue((Integer) centralPurgeStateEncoded.get(CentralPurgeStatusString)));
        centralPurgeStateDecoded.setCentralPurgeType(CentralPurgeType.findByValue((Integer) centralPurgeStateEncoded.get(CentralPurgeTypeString)));

        return centralPurgeStateDecoded;
    }

    public static void logEventToPlainLogs(Logger logger, AuditEvent evt){
        final String successStr =  evt.isSuccess() ? "SUCCESS" : "FAILED";

        if (evt.isSuccess())
            logger.info("{} [message={}]", new Object[]{successStr, evt.msg()});
        else
            logger.error("{} [message={}]", new Object[]{successStr, evt.msg()});
    }

}
