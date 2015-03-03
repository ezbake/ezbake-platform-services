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

package ezbake.quarantine.service;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Permission;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.elastic.common.ElasticUtils;
import ezbake.data.elastic.common.VisibilityFilterConfig;
import ezbake.quarantine.service.util.ElasticsearchUtility;
import ezbake.quarantine.service.util.EncryptionUtility;
import ezbake.quarantine.service.util.IDGenerationUtility;
import ezbake.quarantine.service.util.TokenUtility;
import ezbake.quarantine.thrift.*;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.util.AuditLoggerConfigurator;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This service is used to place content that failed ingest into Quarantine.
 */
public class QuarantineService extends EzBakeBaseThriftService implements Quarantine.Iface {
    private static final Logger log = LoggerFactory.getLogger(QuarantineService.class);
    public static final String SYSTEM_VISIBILITY_PROP = "quarantine.system.visibility";
    private Client elastic;
    private EzbakeSecurityClient security;
    private String quarantineSecurityId;
    private boolean forceRefresh;
    private ElasticsearchUtility utility;
    private static AuditLogger auditLogger;

    private static final VisibilityFilterConfig READ_CONFIG = new VisibilityFilterConfig(ElasticsearchUtility.OBJECT_METADATA_VISIBILITY, Sets.newHashSet(Permission.READ));

    @Override
    public TProcessor getThriftProcessor() {
        EzProperties props = new EzProperties(getConfigurationProperties(), true);
        quarantineSecurityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
        String systemVisibility = props.getProperty(SYSTEM_VISIBILITY_PROP, null);
        if (systemVisibility == null) {
            log.error("The {} property must be set for quarantine to work properly", SYSTEM_VISIBILITY_PROP);
            throw new RuntimeException("System visibility not set for Quarantine");
        }
        security = new EzbakeSecurityClient(props);
        utility = new ElasticsearchUtility(systemVisibility, quarantineSecurityId, security, props);

        init(props);

        return new Quarantine.Processor(this);
    }

    // No arg constructor for ThriftRunner
    public QuarantineService() {}

    /**
     * Inject some dependencies for test purposes.
     */
    public QuarantineService(Properties props, ElasticsearchUtility utility, EzbakeSecurityClient security) {
        this.quarantineSecurityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
        this.utility = utility;
        this.security = security;
        init(new EzProperties(props, true));
    }

    private void init(EzProperties props) {

        // setup logging
        AuditLoggerConfigurator.setAdditivity(true);
        auditLogger = AuditLogger.getAuditLogger(QuarantineService.class);
        
        // setup elastic
        ElasticsearchConfigurationHelper elasticsearchHelper = new ElasticsearchConfigurationHelper(props);
        String clusterName = elasticsearchHelper.getElasticsearchClusterName();
        String hostname = elasticsearchHelper.getElasticsearchHost();
        int port = elasticsearchHelper.getElasticsearchPort();
        final TransportClient client =
                new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build());

        for (final String host : hostname.split(",")) {
            client.addTransportAddress(new InetSocketTransportAddress(host, port));
        }
        elastic = client;
        boolean indexExists = elastic.admin().indices().prepareExists(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX).get().isExists();
        if (!indexExists) {
            elastic.admin().indices().prepareCreate(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                    .addMapping(ElasticsearchUtility.METADATA_TYPE, ElasticsearchUtility.getMetadataMapping()).get();
        }
        forceRefresh = props.getBoolean(EzBakePropertyConstants.ELASTICSEARCH_FORCE_REFRESH_ON_PUT, false);
    }

    /**
     * This method takes the given quarantined object and places it into the quarantine area in Elasticsearch
     * for later inspection.
     *
     * @param qo QuarantinedObject to be placed in Quarantine
     * @param error the error associated with this object
     * @param additionalMetadata additional metadata that will be associated with this object
     * @param token the security token sent from the client which has signed this request
     * @throws TException
     */
    @Override
    public void sendToQuarantine(QuarantinedObject qo, String error, AdditionalMetadata additionalMetadata, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        try {
            TokenUtility.validateToken(security, token);
            HashMap<String, String> auditArgs = Maps.newHashMap();
            auditArgs.put("action", "sendToQuarantine");
            auditArgs.put("pipelineId", qo.getPipelineId());
            auditArgs.put("pipeId", qo.getPipeId());
            auditLog(token, AuditEventType.FileObjectCreate, auditArgs);
            
            long timestamp = System.currentTimeMillis();

            String id = IDGenerationUtility.getId(qo);
            log.debug("Inserting ID {} into quarantine", id);
            ObjectStatus status = qo.isSerializable() ? ObjectStatus.QUARANTINED : ObjectStatus.CANNOT_BE_REINGESTED;

            log.debug("Putting ID {} into quarantine with timestamp {}", id, timestamp);

            GetResponse getResponse = elastic.prepareGet().setIndex(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX).setId(id).get();
            if (getResponse.isExists()) {
                Map<String, Object> newValue = utility.getEventMap(error, timestamp, additionalMetadata);
                UpdateRequestBuilder builder = elastic.prepareUpdate(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX, ElasticsearchUtility.METADATA_TYPE, id)
                        .setRefresh(forceRefresh);
                ElasticsearchUtility.addScriptToUpdateRequest(builder, newValue, status, error);
                builder.get();
            } else {
                String newDoc = utility.getDocumentFromObject(qo, token, status, error, EventType.ERROR, timestamp, additionalMetadata);
                elastic.prepareIndex(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX, ElasticsearchUtility.METADATA_TYPE, id)
                                            .setRefresh(forceRefresh)
                                            .setSource(newDoc).execute().actionGet();
            }
        } catch (NoSuchAlgorithmException e) {
            log.error("Could not instantiate message digest instance", e);
            throw new TException("Could not instantiate message digest instance", e);
        } catch (UnsupportedEncodingException e) {
            // This should never happen
            log.error("Unsupported encoding exception thrown.", e);
            throw new TException(e);
        }
    }

    @Override
    public List<String> getObjectsForPipeline(String pipelineId, Set<ObjectStatus> statuses, int offset, int pageSize, EzSecurityToken token) throws ObjectNotQuarantinedException, TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getObjectsForPipeline");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                                    .setTypes(ElasticsearchUtility.METADATA_TYPE)
                                    .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                                            .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))))
                                    .addField(ElasticsearchUtility.OBJECT_VISIBILITY)
                                    .setSize(pageSize)
                                    .setFrom(offset * pageSize)
                                    .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                                    .get();

        if (response.getHits().getTotalHits() > 0) {
            List<String> results = Lists.newArrayList();
            for (SearchHit hit : response.getHits().getHits()) {
                results.add(hit.getId());
            }
            return results;
        } else {
            throw new ObjectNotQuarantinedException(String.format("No objects found in quarantine for pipeline ID %s with statuses %s", pipelineId, Joiner.on(',').join(statuses)));
        }
    }

    @Override
    public Set<EventWithCount> getObjectCountPerPipe(String pipelineId, Set<ObjectStatus> statuses, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getObjectCountPerPipe");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        Set<EventWithCount> results = Sets.newHashSet();
        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                                            .setTypes(ElasticsearchUtility.METADATA_TYPE)
                                            .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                                                    .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))))
                                            .addFacet(FacetBuilders.termsFacet("pipes").field(ElasticsearchUtility.OBJECT_PIPE))
                                            .get();
        if (response.getFacets() != null) {
            TermsFacet facet = (TermsFacet)response.getFacets().facetsAsMap().get("pipes");
            for (TermsFacet.Entry entry : facet.getEntries()) {
                SearchResponse pipeResponse = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                                                        .setTypes(ElasticsearchUtility.METADATA_TYPE)
                                                        .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, entry.getTerm()))
                                                                .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                                                                .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))))
                                                        .addSort(SortBuilders.fieldSort(ElasticsearchUtility.EVENT_TIME).order(SortOrder.DESC))
                                                        .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                                                        .setSize(1).get();
                if (pipeResponse.getHits().totalHits() > 0) {
                    Map<String, Object> source = pipeResponse.getHits().getAt(0).getSource();
                    List<Object> events = (List<Object>)source.get(ElasticsearchUtility.OBJECT_EVENTS);
                    Map<String, Object> event = (Map<String, Object>)events.get(events.size() - 1);
                    EventWithCount eventWithCount = new EventWithCount().setCount(pipeResponse.getHits().totalHits()).setStatus(ObjectStatus.valueOf((String)source.get(ElasticsearchUtility.OBJECT_STATUS)))
                                                        .setEvent(new QuarantineEvent().setEvent((String) event.get(ElasticsearchUtility.EVENT_TEXT))
                                                                .setType(EventType.valueOf((String) event.get(ElasticsearchUtility.EVENT_TYPE)))
                                                                .setTimestamp(ElasticsearchUtility.getTimeFromString((String)event.get(ElasticsearchUtility.EVENT_TIME)))
                                                                .setPipeId((String) source.get(ElasticsearchUtility.OBJECT_PIPE))
                                                                .setPipelineId((String) source.get(ElasticsearchUtility.OBJECT_PIPELINE))
                                                                .setId((String) source.get("_id")));
                    results.add(eventWithCount);
                }
            }
        }
        return results;
    }

    @Override
    public EventWithCount getLatestEventForPipeline(String pipelineId, Set<ObjectStatus> statuses, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getLatestEventForPipeline");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);
        
        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                        .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))))
                .addSort(SortBuilders.fieldSort(ElasticsearchUtility.EVENT_TIME).order(SortOrder.DESC))
                .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                .setSize(1)
                .get();
        if (response.getHits().totalHits() > 0) {
            Map<String, Object> source = response.getHits().getAt(0).getSource();
            List<Map<String, Object>> events = (List<Map<String, Object>>)source.get(ElasticsearchUtility.OBJECT_EVENTS);
            Map<String, Object> event = events.get(events.size() - 1);
            return new EventWithCount().setEvent(new QuarantineEvent().setEvent((String) event.get(ElasticsearchUtility.EVENT_TEXT))
                    .setType(EventType.valueOf((String) event.get(ElasticsearchUtility.EVENT_TYPE)))
                    .setTimestamp(ElasticsearchUtility.getTimeFromString((String)event.get(ElasticsearchUtility.EVENT_TIME)))
                    .setPipeId((String) source.get(ElasticsearchUtility.OBJECT_PIPE))
                    .setPipelineId((String) source.get(ElasticsearchUtility.OBJECT_PIPELINE))
                    .setId((String) source.get("_id"))).setCount(response.getHits().getTotalHits());
        } else {
            throw new TException("Could not retrieve most recent event for pipeline " + pipelineId);
        }
    }

    @Override
    public List<String> getPipelinesForUser(EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getPipelinesForUser");
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        // Get the user's list of groups and use that to figure out what applications they belong to
        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery())))
                .addFacet(FacetBuilders.termsFacet("pipelines").field(ElasticsearchUtility.OBJECT_PIPELINE)
                        .facetFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG)))
                .get();

        if (response.getFacets() != null) {
            TermsFacet facet = (TermsFacet)response.getFacets().facetsAsMap().get("pipelines");
            return Lists.transform(facet.<TermsFacet.Entry>getEntries(), new Function<TermsFacet.Entry, String>() {
                @Override
                public String apply(TermsFacet.Entry input) {
                    return input.getTerm().toString();
                }
            });
        }

        return Lists.newArrayList();
    }

    @Override
    public QuarantineEvent getLatestEventForPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getLatestEventForPipe");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);
        
        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, pipeId))
                        .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                        .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))))
                .addSort(SortBuilders.fieldSort(ElasticsearchUtility.EVENT_TIME).order(SortOrder.DESC))
                .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                .setSize(1)
                .get();
        if (response.getHits().totalHits() > 0) {
            Map<String, Object> source = response.getHits().getAt(0).getSource();
            List<Map<String, Object>> events = (List<Map<String, Object>>)source.get(ElasticsearchUtility.OBJECT_EVENTS);
            Map<String, Object> event = events.get(events.size() - 1);
            return new QuarantineEvent().setEvent((String) event.get(ElasticsearchUtility.EVENT_TEXT))
                    .setType(EventType.valueOf((String) event.get(ElasticsearchUtility.EVENT_TYPE)))
                    .setTimestamp(ElasticsearchUtility.getTimeFromString((String)event.get(ElasticsearchUtility.EVENT_TIME)))
                    .setPipeId((String) source.get(ElasticsearchUtility.OBJECT_PIPE))
                    .setPipelineId((String) source.get(ElasticsearchUtility.OBJECT_PIPELINE))
                    .setId((String) source.get("_id"));
        } else {
            throw new TException("Could not retrieve most recent event for pipeline " + pipelineId);
        }
    }

    @Override
    public Set<EventWithCount> getEventCountPerPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getEventCountPerPipe");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        Set<EventWithCount> results = Sets.newHashSet();

        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                        .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses)))
                .addAggregation(AggregationBuilders.terms("latest_event").size(0).field(ElasticsearchUtility.OBJECT_LATEST_EVENT))
                .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                .get();
        if (response.getAggregations() != null) {
            Terms terms = response.getAggregations().get("latest_event");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                SearchResponse pipeResponse = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                        .setTypes(ElasticsearchUtility.METADATA_TYPE)
                        .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, pipeId))
                                .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                                .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))
                                .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_LATEST_EVENT, bucket.getKey()))))
                        .addSort(SortBuilders.fieldSort(ElasticsearchUtility.EVENT_TIME).order(SortOrder.DESC))
                        .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                        .setSize(1).get();
                if (pipeResponse.getHits().totalHits() > 0) {
                    SearchHit hit = pipeResponse.getHits().getAt(0);
                    Map<String, Object> source = hit.getSource();
                    List<Object> events = (List<Object>)source.get(ElasticsearchUtility.OBJECT_EVENTS);
                    Map<String, Object> event = (Map<String, Object>)events.get(events.size() - 1);
                    EventWithCount eventWithCount = new EventWithCount().setCount(pipeResponse.getHits().totalHits()).setStatus(ObjectStatus.valueOf((String)source.get(ElasticsearchUtility.OBJECT_STATUS)))
                            .setEvent(new QuarantineEvent().setEvent((String) event.get(ElasticsearchUtility.EVENT_TEXT))
                                    .setType(EventType.valueOf((String) event.get(ElasticsearchUtility.EVENT_TYPE)))
                                    .setTimestamp(ElasticsearchUtility.getTimeFromString((String) event.get(ElasticsearchUtility.EVENT_TIME)))
                                    .setPipeId((String) source.get(ElasticsearchUtility.OBJECT_PIPE))
                                    .setPipelineId((String) source.get(ElasticsearchUtility.OBJECT_PIPELINE))
                                    .setId(hit.getId()));
                    results.add(eventWithCount);
                }
            }
        }
        return results;
    }

    @Override
    public long getCountPerPipe(String pipelineId, String pipeId, Set<ObjectStatus> statuses, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getCountPerPipe");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                        .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))
                        .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, pipeId))))
                .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                .get();
        return response.getHits().getTotalHits();
    }

    @Override
    public IdsResponse getObjectsForPipeAndEvent(String pipelineId, String pipeId, String event, Set<ObjectStatus> statuses, int pageNumber, int pageSize, EzSecurityToken token) throws TException, ObjectNotQuarantinedException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getObjectsForPipeAndEvent");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("event", event);
        auditArgs.put("statuses", Joiner.on(',').join(statuses));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        IdsResponse idsResponse = new IdsResponse();

        SearchResponse response = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, pipeId))
                        .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                        .must(QueryBuilders.inQuery(ElasticsearchUtility.OBJECT_STATUS, statuses))
                        .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_LATEST_EVENT, event))))
                .setSize(pageSize)
                .setFrom(pageNumber * pageSize)
                .setPostFilter(ElasticUtils.getVisibilityFilter(token, READ_CONFIG))
                .execute().actionGet();
        if (response.getHits().totalHits() > 0) {
            idsResponse.setTotalResults(response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> source = hit.getSource();
                String id = hit.getId();
                List<Map<String, Object>> events = (List<Map<String, Object>>)source.get(ElasticsearchUtility.OBJECT_EVENTS);
                Map<String, Object> latestEvent = events.get(events.size() - 1);
                idsResponse.addToIds(new IdAndStatus().setId(id)
                        .setStatus(ObjectStatus.valueOf((String) source.get(ElasticsearchUtility.OBJECT_STATUS)))
                        .setTimestamp(ElasticsearchUtility.getTimeFromString((String) latestEvent.get(ElasticsearchUtility.EVENT_TIME))));
            }
            return idsResponse;
        } else {
            throw new TException("Could not retrieve most recent event for pipeline " + pipelineId);
        }
    }

    @Override
    public List<QuarantineResult> getQuarantinedObjects(List<String> ids, EzSecurityToken token) throws ObjectNotQuarantinedException, TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getQuarantinedObjects");
        for (String id : ids) {
            auditArgs.put("id", id);
            auditLog(token, AuditEventType.FileObjectAccess, auditArgs);
        }

        return getQuarantinedObjectsInternal(ids, token);
    }

    private List<QuarantineResult> getQuarantinedObjectsInternal(List<String> ids, EzSecurityToken token) throws ObjectNotQuarantinedException, TException {
        List<QuarantineResult> results = Lists.newArrayList();

        MultiGetResponse response = elastic.prepareMultiGet()
                                        .add(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX, ElasticsearchUtility.METADATA_TYPE, ids)
                                        .get();

        for (MultiGetItemResponse getResponse : response.getResponses()) {
            GetResponse get = getResponse.getResponse();
            if (get != null && get.isExists()) {
                String id = get.getId();
                Map<String, Object> source = get.getSource();
                String base64Vis = (String)source.get(ElasticsearchUtility.OBJECT_VISIBILITY);
                if (utility.hasPermission(base64Vis, token)) {
                    ObjectStatus status = ObjectStatus.valueOf((String) source.get(ElasticsearchUtility.OBJECT_STATUS));
                    QuarantinedObject qo = utility.getQuarantinedObjectFromSource(source);
                    List<QuarantineEvent> events = utility.getEventsFromMap((List<Map<String, Object>>)source.get(ElasticsearchUtility.OBJECT_EVENTS), id, qo.getPipeId(), qo.getPipelineId(), token);
                    results.add(new QuarantineResult().setId(id).setStatus(status).setObject(qo).setEvents(events));
                }
            }
        }

        if (results.isEmpty()) {
            throw new ObjectNotQuarantinedException("No objects found in quarantine for given list of IDs. You may not have the proper authorizations to view this object.");
        }
        return results;
    }

    @Override
    public void updateStatus(List<String> ids, ObjectStatus status, String updateComment, EzSecurityToken token) throws TException, InvalidUpdateException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "updateStatus");
        for (String id : ids) {
            auditArgs.put("id", id);
            auditLog(token, AuditEventType.FileObjectModify, auditArgs);
        }

        // Check if any of the objects we are attempting to update are in the CANNOT_BE_REINGESTED state
        SearchResponse reingestCheckResponse = elastic.prepareSearch(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                .setTypes(ElasticsearchUtility.METADATA_TYPE)
                .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.inQuery("_id", ids))
                        .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_STATUS, ObjectStatus.CANNOT_BE_REINGESTED))))
                .execute().actionGet();
        if (reingestCheckResponse.getHits().getTotalHits() > 0) {
            log.error("Attempted to update ID that was in {} state", ObjectStatus.CANNOT_BE_REINGESTED);
            throw new InvalidUpdateException(String.format("Cannot update set of IDs because one or more of them are in the %s state.", ObjectStatus.CANNOT_BE_REINGESTED));
        }

        updateStatusById(ids, status, updateComment, token);
    }

    @Override
    public void updateStatusOfEvent(String pipelineId, String pipeId, ObjectStatus oldStatus, ObjectStatus newStatus, String oldEvent, String updateComment, EzSecurityToken token) throws InvalidUpdateException, TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "updateStatusOfEvent");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("event", oldEvent);
        auditLog(token, AuditEventType.FileObjectModify, auditArgs);

        // Check if the old status is CANNOT_BE_REINGESTED (which is invalid)
        if (oldStatus == ObjectStatus.CANNOT_BE_REINGESTED) {
            log.error("Attempted to update ID that was in {} state", ObjectStatus.CANNOT_BE_REINGESTED);
            throw new InvalidUpdateException(String.format("Cannot update set of IDs because the old status of this request is %s.", ObjectStatus.CANNOT_BE_REINGESTED));
        }

        long countForThisPipe = getCountPerPipe(pipelineId, pipeId, Sets.newHashSet(oldStatus), token);
        IdsResponse idsAndStatuses = getObjectsForPipeAndEvent(pipelineId, pipeId, oldEvent, Sets.newHashSet(oldStatus), 0, (int)countForThisPipe, token);
        List<String> ids = Lists.transform(idsAndStatuses.getIds(), new Function<IdAndStatus, String>() {
            @Override
            public String apply(IdAndStatus input) {
                return input.getId();
            }
        });
        updateStatusById(ids, newStatus, updateComment, token);
    }

    private void updateStatusById(List<String> ids, ObjectStatus status, String updateComment, EzSecurityToken token) throws TException, InvalidUpdateException {
        long newTimestamp = System.currentTimeMillis();

        if (status == ObjectStatus.CANNOT_BE_REINGESTED) {
            throw new InvalidUpdateException(String.format("%s is not a valid status for update", status.toString()));
        }

        try {
            BulkRequestBuilder builder = new BulkRequestBuilder(elastic);
            Map<String, Object> newValue = Maps.newHashMap();
            newValue.put(ElasticsearchUtility.EVENT_TEXT, updateComment);
            newValue.put(ElasticsearchUtility.EVENT_TYPE, EventType.STATUS_UPDATE.toString());
            newValue.put(ElasticsearchUtility.EVENT_TIME, ElasticsearchUtility.getTimeString(newTimestamp));
            for (int i = 0; i < ids.size(); i++) {
                UpdateRequestBuilder update = new UpdateRequestBuilder(elastic)
                        .setIndex(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                        .setType(ElasticsearchUtility.METADATA_TYPE)
                        .setId(ids.get(i));
                ElasticsearchUtility.addScriptToUpdateRequest(update, newValue, status, updateComment);
                builder.add(update);

                // Send a request for every thousand items to avoid killing elasticsearch with too large of
                // a request.
                if (i > 0 && i % 1000 == 0) {
                    BulkResponse response = elastic.bulk(builder.request().refresh(true)).get();
                    if (response.hasFailures()) {
                        throw new TException("Failed to update items: " + response.buildFailureMessage());
                    }
                    builder = new BulkRequestBuilder(elastic);
                }
            }
            if (builder.numberOfActions() > 0) {
                BulkResponse response = elastic.bulk(builder.request().refresh(true)).get();
                if (response.hasFailures()) {
                    throw new TException("Failed to update items: " + response.buildFailureMessage());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception thrown while attempting to update the status", e);
            throw new TException("Failed to execute update", e);
        }
    }

    @Override
    public ByteBuffer exportData(List<String> ids, String key, EzSecurityToken token) throws ObjectNotQuarantinedException, TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "exportData");
        for (String id : ids) {
            auditArgs.put("id", id);
            auditLog(token, AuditEventType.FileObjectAccess, auditArgs);
        }
        
        List<QuarantineResult> results = getQuarantinedObjectsInternal(ids, token);
        ResultList list = new ResultList().setResults(results);
        byte[] salt = EncryptionUtility.getSalt();
        byte[] iv = EncryptionUtility.getInitializationVector();
        byte[] encryptedContent = EncryptionUtility.encryptData(ThriftUtils.serialize(list), iv, salt, key);
        ExportedData dataToExport = new ExportedData().setSalt(salt).setInitializationVector(iv).setEncryptedContent(encryptedContent);
        return ByteBuffer.wrap(ThriftUtils.serialize(dataToExport));
    }

    @Override
    public ImportResult importData(ByteBuffer dataToImport, String key, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "importData");
        auditLog(token, AuditEventType.FileObjectCreate, auditArgs);

        ImportResult importResult = new ImportResult();
        ExportedData objectToImport = ThriftUtils.deserialize(ExportedData.class, dataToImport.array());
        byte[] salt = objectToImport.getSalt();
        byte[] iv = objectToImport.getInitializationVector();
        byte[] resultListBytes = EncryptionUtility.decryptData(objectToImport.getEncryptedContent(), key, salt, iv);

        try {
            ResultList list = ThriftUtils.deserialize(ResultList.class, resultListBytes);
            log.info("Importing {} QuarantineResult objects", list.getResultsSize());
            importResult.setTotalRecords(list.getResultsSize());
            importResult.setDuplicateRecords(0);
            importResult.setRecordsImported(0);
            BulkRequestBuilder bulkRequestBuilder = elastic.prepareBulk();
            for (QuarantineResult result : list.getResults()) {
                log.info("Importing ID {}", result.getId());
                String id = result.getId();
                ObjectStatus status = result.getStatus();
                List<QuarantineEvent> events = result.getEvents();
                QuarantinedObject object = result.getObject();

                try {
                    getQuarantinedObjectsInternal(Lists.newArrayList(id), token);
                    log.warn("Object ID {} already existed in quarantine, skipping", id);
                    importResult.setDuplicateRecords(importResult.getDuplicateRecords() + 1);
                } catch (ObjectNotQuarantinedException e) {
                    log.debug("Importing {} quarantine events for {}", result.getEventsSize(), id);
                    bulkRequestBuilder.add(new IndexRequestBuilder(elastic).setIndex(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                                                    .setType(ElasticsearchUtility.METADATA_TYPE)
                                                    .setId(id).setSource(utility.getDocumentFromObject(object, token, status, events)));

                    importResult.setRecordsImported(importResult.getRecordsImported() + 1);
                }
            }
            if (bulkRequestBuilder.numberOfActions() > 0) {
                elastic.bulk(bulkRequestBuilder.request().refresh(forceRefresh)).get();
            }
            return importResult;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Could not index objects in elasticsearch", e);
            throw new TException(e);
        }
    }

    @Override
    public void deleteFromQuarantine(List<String> ids, EzSecurityToken token) throws ObjectNotQuarantinedException, TException {
        TokenUtility.validateToken(security, token);
        if (!TokenUtility.getSecurityId(token).equals(quarantineSecurityId)) {
            throw new TException("Objects can only be deleted from the Quarantine UI");
        }
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "deleteFromQuarantine");
        for (String id : ids) {
            auditArgs.put("id", id);
            auditLog(token, AuditEventType.FileObjectDelete, auditArgs);
        }

        try {
            BulkRequestBuilder builder = new BulkRequestBuilder(elastic);
            for (int i = 0; i < ids.size(); i++) {
                builder.add(new DeleteRequest(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX, ElasticsearchUtility.METADATA_TYPE, ids.get(i)));
                if (i > 0 && i % 1000 == 0) {
                    elastic.bulk(builder.request().refresh(true)).get();
                    builder = new BulkRequestBuilder(elastic);
                }
            }
            if (builder.numberOfActions() > 0) {
                elastic.bulk(builder.request().refresh(true)).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception thrown while attempting to delete objects", e);
            throw new TException("Failed to execute delete", e);
        }
    }

    @Override
    public void deleteObjectsByEvent(String pipelineId, String pipeId, ObjectStatus status, String eventText, EzSecurityToken token) throws TException {
        TokenUtility.validateToken(security, token);
        if (!TokenUtility.getSecurityId(token).equals(quarantineSecurityId)) {
            throw new TException("Objects can only be deleted from the Quarantine UI");
        }
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "deleteObjectsByEvent");
        auditArgs.put("pipelineId", pipelineId);
        auditArgs.put("pipeId", pipeId);
        auditArgs.put("eventText", eventText);
        auditLog(token, AuditEventType.FileObjectDelete, auditArgs);

        elastic.prepareDeleteByQuery(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX)
                    .setTypes(ElasticsearchUtility.METADATA_TYPE)
                    .setQuery(utility.addSecurityIdQuery(token, QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPE, pipeId))
                            .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_PIPELINE, pipelineId))
                            .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_STATUS, status))
                            .must(QueryBuilders.matchQuery(ElasticsearchUtility.OBJECT_LATEST_EVENT, eventText))))
                    .execute().actionGet();
    }

    /**
     * This method simply checks if the required Elasticsearch index has been created.
     *
     * @return true if the service is healthy
     */
    @Override
    public boolean ping() {
        return elastic.admin().indices().prepareExists(ElasticsearchUtility.QUARANTINE_ELASTIC_INDEX).get().isExists();
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
