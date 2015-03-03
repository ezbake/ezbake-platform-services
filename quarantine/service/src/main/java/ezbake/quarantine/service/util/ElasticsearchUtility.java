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

package ezbake.quarantine.service.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import ezbake.base.thrift.*;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.quarantine.thrift.*;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.permissions.PermissionUtils;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class ElasticsearchUtility {
    public static final String QUARANTINE_ELASTIC_INDEX = "quarantine";
    public static final String METADATA_TYPE = "metadata";
    private static SimpleDateFormat sdf;
    private Gson gson;
    private Visibility systemHighVisibility;
    private String quarantineSecurityId;
    private ThriftClientPool pool;
    private EzbakeSecurityClient security;

    // Field names for the type mapping
    public static final String OBJECT_CONTENT = "_content";
    public static final String OBJECT_VISIBILITY = "_visibility";
    public static final String OBJECT_METADATA_VISIBILITY = "_object_metadata_visibility";
    public static final String OBJECT_STATUS = "_status";
    public static final String OBJECT_EVENTS = "_events";
    public static final String OBJECT_PIPE = "_pipe";
    public static final String OBJECT_PIPELINE = "_pipeline";
    public static final String OBJECT_LATEST_EVENT = "_latest_event";
    public static final String OBJECT_SECURITY_ID = "_security_id";

    public static final String EVENT_TEXT = "_event_text";
    public static final String EVENT_TIME = "_event_time";
    public static final String EVENT_TYPE = "_event_type";
    public static final String EVENT_ADDITIONAL_METADATA = "_event_additional_metadata";
    public static final String METADATA_KEY = "_meta_key";
    public static final String METADATA_VALUE = "_meta_value";
    public static final String METADATA_VISIBILITY = "_meta_vis";

    public ElasticsearchUtility(String systemVisibility, String quarantineSecurityId, EzbakeSecurityClient client, Properties props) {
        this(systemVisibility, quarantineSecurityId, client, new ThriftClientPool(props));
    }

    public ElasticsearchUtility(String systemVisibility, String quarantineSecurityId, EzbakeSecurityClient client, ThriftClientPool pool) {
        this.gson = new Gson();
        this.systemHighVisibility = new Visibility().setFormalVisibility(systemVisibility);
        this.quarantineSecurityId = quarantineSecurityId;
        this.pool = pool;
        this.security = client;
    }

    static {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    public static String getTimeString(long millis) {
        Date date = new Date(millis);
        return sdf.format(date);
    }

    public static long getTimeFromString(String time) throws TException {
        try {
            return sdf.parse(time).getTime();
        } catch (ParseException e) {
            throw new TException("Couldn't parse time string", e);
        }
    }

    /**
     * Converts the source map from Elasticsearch into a QuarantinedObject.
     */
    public QuarantinedObject getQuarantinedObjectFromSource(Map<String, Object> source) throws TException {
        QuarantinedObject object = new QuarantinedObject();
        object.setContent(getBytesFromBase64String((String) source.get(OBJECT_CONTENT)));
        object.setPipeId((String) source.get(OBJECT_PIPE));
        object.setPipelineId((String) source.get(OBJECT_PIPELINE));
        object.setVisibility(ThriftUtils.deserialize(Visibility.class, getBytesFromBase64String((String)source.get(OBJECT_VISIBILITY))));
        return object;
    }

    /**
     * Converts a Quarantined object and it's associated metadata into a String suitable for Elasticsearch. Some objects are stored differently than
     * a pure JSON conversion of the POJO, which is why this method needs to be used. This should be used for the single event use case.
     *
     * @throws TException
     */
    public String getDocumentFromObject(QuarantinedObject qo, EzSecurityToken token, ObjectStatus status, String eventText, EventType type, long timestamp, AdditionalMetadata metadata) throws TException {
        QuarantineEvent event = new QuarantineEvent()
                .setEvent(eventText)
                .setAdditionalMetadata(metadata)
                .setTimestamp(timestamp)
                .setType(type);
        return getDocumentFromObject(qo, token, status, Lists.newArrayList(event));
    }

    /**
     * Converts a Quarantined object and a list of associated events into a JSON string for insertion into Elasticsearch. This should be used for
     * importing a full quarantined object.
     *
     * @throws TException
     */
    public String getDocumentFromObject(QuarantinedObject qo, EzSecurityToken token, ObjectStatus status, List<QuarantineEvent> objectEvents) throws TException {
        EzGroups.Client groupClient = null;
        try {
            // Use the groups service to attribute group permissions to the metadata that we put into Quarantine.
            groupClient = pool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class);
            Visibility metadataVisibility = new Visibility(systemHighVisibility);
            if (qo.isSetApplicationName()) {
                String groupName = EzGroupsConstants.APP_GROUP + EzGroupsConstants.GROUP_NAME_SEP + qo.getApplicationName();
                EzSecurityTokenWrapper tokenForGroupsService = security.fetchDerivedTokenForApp(token, pool.getSecurityId(EzGroupsConstants.SERVICE_NAME));
                Set<Long> groupsMask = groupClient.getGroupsMask(tokenForGroupsService, Sets.newHashSet(groupName));
                metadataVisibility.setAdvancedMarkings(new AdvancedMarkings().setPlatformObjectVisibility(new PlatformObjectVisibilities().setPlatformObjectReadVisibility(groupsMask)));
            }

            String latestEventText = "";
            if (objectEvents.size() > 0) {
                latestEventText = objectEvents.get(objectEvents.size() - 1).getEvent();
            }
            Map<String, Object> newDocumentMap = Maps.newHashMap();
            newDocumentMap.put(OBJECT_CONTENT, getBase64(qo.getContent()));
            newDocumentMap.put(OBJECT_VISIBILITY, getBase64SerializedString(qo.getVisibility()));
            newDocumentMap.put(OBJECT_STATUS, status.toString());
            newDocumentMap.put(OBJECT_PIPE, qo.getPipeId());
            newDocumentMap.put(OBJECT_PIPELINE, qo.getPipelineId());
            newDocumentMap.put(OBJECT_METADATA_VISIBILITY, ThriftUtils.serializeToBase64(metadataVisibility));
            newDocumentMap.put(OBJECT_LATEST_EVENT, latestEventText);
            newDocumentMap.put(OBJECT_SECURITY_ID, new EzSecurityTokenWrapper(token).getSecurityId());
            List<Map<String, Object>> events = Lists.newArrayList();
            for (QuarantineEvent event : objectEvents) {
                Map<String, Object> eventMap = Maps.newHashMap();
                eventMap.put(EVENT_TEXT, event.getEvent());
                eventMap.put(EVENT_TYPE, event.getType());
                eventMap.put(EVENT_TIME, getTimeString(event.getTimestamp()));
                List<Map<String, Object>> additionalMetadata = Lists.newArrayList();
                if (event.isSetAdditionalMetadata() && event.getAdditionalMetadata().isSetEntries()) {
                    for (Map.Entry<String, MetadataEntry> entry : event.getAdditionalMetadata().getEntries().entrySet()) {
                        Map<String, Object> newEntry = Maps.newHashMap();
                        newEntry.put(METADATA_KEY, entry.getKey());
                        newEntry.put(METADATA_VALUE, entry.getValue().getValue());
                        if (entry.getValue().isSetVisibility()) {
                            newEntry.put(METADATA_VISIBILITY, getBase64SerializedString(entry.getValue().getVisibility()));
                        } else {
                            newEntry.put(METADATA_VISIBILITY, getBase64SerializedString(systemHighVisibility));
                        }
                        additionalMetadata.add(newEntry);
                    }
                }
                eventMap.put(EVENT_ADDITIONAL_METADATA, additionalMetadata);
                events.add(eventMap);
            }
            newDocumentMap.put(OBJECT_EVENTS, events);
            return gson.toJson(newDocumentMap);
        } finally {
            pool.returnToPool(groupClient);
        }
    }

    public BoolQueryBuilder addSecurityIdQuery(EzSecurityToken token, BoolQueryBuilder query) {
        if (!TokenUtility.getSecurityId(token).equals(quarantineSecurityId)) {
            query.must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_SECURITY_ID, TokenUtility.getSecurityId(token)));
        }
        return query;
    }

    public List<QuarantineEvent> getEventsFromMap(List<Map<String, Object>> maps, String id, String pipeId, String pipelineId, EzSecurityToken token) throws TException {
        List<QuarantineEvent> events = Lists.newArrayList();
        for (Map<String, Object> map : maps) {
            QuarantineEvent event = new QuarantineEvent();
            event.setEvent((String)map.get(EVENT_TEXT));
            event.setTimestamp(getTimeFromString((String) map.get(EVENT_TIME)));
            event.setType(EventType.valueOf((String)map.get(EVENT_TYPE)));
            event.setId(id);
            event.setPipeId(pipeId);
            event.setPipelineId(pipelineId);
            AdditionalMetadata additionalMetadata = new AdditionalMetadata();
            if (map.containsKey(EVENT_ADDITIONAL_METADATA)) {
                for (Map<String, Object> metadata : (List<Map<String, Object>>)map.get(EVENT_ADDITIONAL_METADATA)) {
                    MetadataEntry entry = new MetadataEntry();
                    entry.setValue((String)metadata.get(METADATA_VALUE));
                    Visibility visibility = systemHighVisibility;
                    if (metadata.containsKey(METADATA_VISIBILITY)) {
                        visibility = ThriftUtils.deserialize(Visibility.class, getBytesFromBase64String((String) metadata.get(METADATA_VISIBILITY)));
                    }
                    entry.setVisibility(visibility);
                    if (PermissionUtils.getPermissions(token.getAuthorizations(), visibility, true).contains(Permission.READ)) {
                        additionalMetadata.putToEntries((String)metadata.get(METADATA_KEY), entry);
                    }
                }
                event.setAdditionalMetadata(additionalMetadata);
            }
            events.add(event);
        }
        return events;
    }

    public Map<String, Object> getEventMap(String eventText, long timestamp, AdditionalMetadata additionalMetadata) throws TException {
        Map<String, Object> event = Maps.newHashMap();
        event.put(ElasticsearchUtility.EVENT_TEXT, eventText);
        event.put(ElasticsearchUtility.EVENT_TYPE, EventType.ERROR.toString());
        event.put(ElasticsearchUtility.EVENT_TIME, ElasticsearchUtility.getTimeString(timestamp));
        List<Map<String, Object>> metadata = Lists.newArrayList();
        if (additionalMetadata != null && additionalMetadata.isSetEntries()) {
            for (Map.Entry<String, MetadataEntry> entry : additionalMetadata.getEntries().entrySet()) {
                Map<String, Object> newEntry = Maps.newHashMap();
                newEntry.put(ElasticsearchUtility.METADATA_KEY, entry.getKey());
                newEntry.put(ElasticsearchUtility.METADATA_VALUE, entry.getValue().getValue());
                if (entry.getValue().isSetVisibility()) {
                    newEntry.put(ElasticsearchUtility.METADATA_VISIBILITY, getBase64SerializedString(entry.getValue().getVisibility()));
                } else {
                    newEntry.put(ElasticsearchUtility.METADATA_VISIBILITY, getBase64SerializedString(systemHighVisibility));
                }
                metadata.add(newEntry);
            }
            event.put(ElasticsearchUtility.EVENT_ADDITIONAL_METADATA, metadata);
        }
        return event;
    }

    public static void addScriptToUpdateRequest(UpdateRequestBuilder builder, Map<String, Object> newEventMap, ObjectStatus status, String newLatestEvent) {
        builder.setScript("ctx._source." + ElasticsearchUtility.OBJECT_EVENTS + " += newEvent;" +
                "ctx._source." + ElasticsearchUtility.OBJECT_STATUS + " = newStatus;" +
                "ctx._source." + ElasticsearchUtility.OBJECT_LATEST_EVENT + " = eventText")
                .addScriptParam("newEvent", newEventMap)
                .addScriptParam("newStatus", status.toString())
                .addScriptParam("eventText", newLatestEvent);
    }

    private String getBase64SerializedString(TBase t) throws TException {
        return getBase64(ThriftUtils.serialize(t));
    }

    private String getBase64(byte[] bytes) {
        return new String(Base64.encodeBase64(bytes));
    }

    private byte[] getBytesFromBase64String(String encoded) {
        return Base64.decodeBase64(encoded);
    }

    public boolean hasPermission(String base64EncodedVis, EzSecurityToken token) throws TException {
        Visibility vis = ThriftUtils.deserialize(Visibility.class, getBytesFromBase64String(base64EncodedVis));
        return PermissionUtils.getPermissions(token.getAuthorizations(), vis, true).contains(Permission.READ);
    }

    public static String getMetadataMapping() {
        return "{\n" +
                "    \"" + METADATA_TYPE + "\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"" + OBJECT_CONTENT + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : false\n" +
                "            },\n" +
                "            \"" + OBJECT_VISIBILITY + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : false\n" +
                "            },\n" +
                "            \"" + OBJECT_STATUS + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_METADATA_VISIBILITY + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_PIPE + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_SECURITY_ID + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_PIPELINE + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_LATEST_EVENT + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + OBJECT_EVENTS + "\" : {\n" +
                "                \"type\" : \"object\",\n" +
                "                \"properties\" : {\n" +
                "                     \"" + EVENT_TEXT + "\" : {\"type\" : \"string\"},\n" +
                "                     \"" + EVENT_TYPE + "\" : {\"type\" : \"string\"},\n" +
                "                     \"" + EVENT_TIME + "\" : {\n" +
                "                         \"type\" : \"date\",\n" +
                "                         \"format\" : \"date_hour_minute_second_millis\",\n" +
                "                         \"store\" : \"true\"\n" +
                "                      },\n" +
                "                     \"" + EVENT_ADDITIONAL_METADATA + "\" : {\n" +
                "                         \"type\" : \"object\",\n" +
                "                         \"properties\" : {\n" +
                "                             \"" + METADATA_KEY + "\" : {\"type\" : \"string\"},\n" +
                "                             \"" + METADATA_VALUE + "\" : {\"type\" : \"string\"},\n" +
                "                             \"" + METADATA_VISIBILITY + "\" : {\"type\" : \"string\", \"store\" : false}\n" +
                "                         }\n" +
                "                     }\n" +
                "                 }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
    }
}
