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

package ezbake.services.search;

import static ezbake.util.AuditEvent.event;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import ezbake.data.elastic.thrift.DocumentIdentifier;
import ezbake.data.elastic.thrift.Page;
import ezbake.data.elastic.thrift.UpdateOptions;
import ezbake.data.elastic.thrift.UpdateScript;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.ExistsFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ezbake.base.thrift.CancelStatus;
import ezbake.base.thrift.EzBakeBasePurgeThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.PurgeState;
import ezbake.base.thrift.PurgeStatus;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.data.common.TimeUtil;
import ezbake.data.elastic.common.ElasticUtils;
import ezbake.data.elastic.thrift.BaseFacetValue;
import ezbake.data.elastic.thrift.DateField;
import ezbake.data.elastic.thrift.DateHistogramFacet;
import ezbake.data.elastic.thrift.DateHistogramFacetResult;
import ezbake.data.elastic.thrift.DateInterval;
import ezbake.data.elastic.thrift.DateIntervalType;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.DocumentIndexingException;
import ezbake.data.elastic.thrift.EzElastic;
import ezbake.data.elastic.thrift.Facet;
import ezbake.data.elastic.thrift.FacetRange;
import ezbake.data.elastic.thrift.FacetRequest;
import ezbake.data.elastic.thrift.FacetResult;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.data.elastic.thrift.MalformedQueryException;
import ezbake.data.elastic.thrift.PercolateQuery;
import ezbake.data.elastic.thrift.PercolateRequest;
import ezbake.data.elastic.thrift.Query;
import ezbake.data.elastic.thrift.RangeFacet;
import ezbake.data.elastic.thrift.RangeFacetEntry;
import ezbake.data.elastic.thrift.RangeType;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.data.elastic.thrift.TermsFacet;
import ezbake.data.elastic.thrift.TermsFacetEntry;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.AppNotRegisteredException;
import ezbake.services.centralPurge.thrift.EzCentralPurgeConstants;
import ezbake.services.geospatial.thrift.GeospatialExtractorConstants;
import ezbake.services.geospatial.thrift.GeospatialExtractorService;
import ezbake.services.geospatial.thrift.TCentroid;
import ezbake.services.geospatial.thrift.TLocation;
import ezbake.services.geospatial.thrift.TLocationFinderResult;
import ezbake.services.provenance.thrift.PositionsToUris;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.services.search.utils.BooleanSerializer;
import ezbake.services.search.utils.DateSerializer;
import ezbake.services.search.utils.SSRUtils;
import ezbake.thrift.ThriftClientPool;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.util.AuditLoggerConfigurator;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

@SuppressWarnings("FieldCanBeLocal")
public class SSRServiceHandler extends EzBakeBasePurgeThriftService implements ssrService.Iface {
    private static Logger logger = LoggerFactory.getLogger(SSRServiceHandler.class);
    private Gson gson;
    private JsonParser jsonParser = new JsonParser();
    private ThriftClientPool pool;
    private final LRUMap typeCache = new LRUMap(1000);
    private final String DATE_FACET_KEY = "Report Date";
    private final String INGEST_FACET_KEY = "Ingest Date";
    private final String VISIBILITY_FACET_KEY = "Report Visibility";
    private final String TYPE_FACET_KEY = "Report Type";
    private final String GEO_COUNTRY_FACET_KEY = "Report Country";
    private final String GEO_PROVINCE_FACET_KEY = "Report State/Province";
    private long last24hoursMS = 0;
    private long last48hoursMS = 0;
    private long last72hoursMS = 0;
    private long last7daysMS = 0;
    private long last30daysMS = 0;
    private long last90daysMS = 0;
    private long last365daysMS = 0;
    private String securityId;
    private static AuditLogger auditLogger;
    private boolean isGeoEnabled;
    private int percolatorInboxLimit;
    private final int MAX_PERCOLATOR_RESPONSE = 50;
    private final int MAX_WEEKLY_PERCOLATOR_RESULTS = 350;

    // Configuration constants
    public static String EZELASTIC_APPLICATION_NAME_KEY = "ssr.application.name";
    public static String EZELASTIC_SERVICE_NAME_KEY = "ezelastic.service.name";
    public static String ENABLE_GEO_KEY = "ssr.geo.enable";
    private static final String PERCOLATOR_INBOX_LIMIT = "ssr.percolator.inbox.limit";

    // default date format
    private static final String SSR_DEFAULT_DATE_FORMAT = "yyyyMMdd'T' HHmmss.SSSZ";

    private EzbakeSecurityClient security;

    @SuppressWarnings("unchecked")
    public TProcessor getThriftProcessor() {
        EzElastic.Client documentClient = null;
        AuditEvent evt = null;
        try {
            EzProperties props = new EzProperties(getConfigurationProperties(), true);
            pool = new ThriftClientPool(props);
            documentClient = getDocumentClient();
            security = new EzbakeSecurityClient(props);
            EzSecurityToken ssrServiceToken = security.fetchAppToken();
            evt = event(AuditEventType.ApplicationInitialization.getName(), ssrServiceToken)
                    .arg("event", "init");

            isGeoEnabled = props.getBoolean(ENABLE_GEO_KEY, true);
            percolatorInboxLimit = props.getInteger(PERCOLATOR_INBOX_LIMIT, 500); // default to 500

            // Update the index to ignore malformed values coming in. The malformed values will still be added
            // to _all, but they won't be searchable in their field.
            securityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
            documentClient.closeIndex(ssrServiceToken);
            documentClient.applySettings("{\"index\" : {\"mapping\" : {\"ignore_malformed\" : true}}}", ssrServiceToken);
            documentClient.openIndex(ssrServiceToken);
            gson = new GsonBuilder()
                    .registerTypeAdapter(Date.class, new DateSerializer())
                    .registerTypeAdapter(Boolean.TYPE, new BooleanSerializer())
                    .create();

            AuditLoggerConfigurator.setAdditivity(true);
            auditLogger = AuditLogger.getAuditLogger(SSRServiceHandler.class);

            // setup type mappings
            setupTypeMappings(documentClient, ssrServiceToken);

            return new ssrService.Processor(this);
        } catch (AppNotRegisteredException e) {
            logError(e, evt, "SSR Service not registered in ezsecurity. Exiting.");
            throw new RuntimeException("Could not initialize SSR service", e);
        } catch (TException e) {
            logError(e, evt, "Error starting SSR Service.");
            throw new RuntimeException("Could not initialize SSR service", e);
        } finally {
            if (evt != null)
                auditLogger.logEvent(evt);
            returnAndNullClient(documentClient);
        }
    }

    // setup the necessary type mappings elasticsearch required to run query successfully when
    // these fields are missing from ssr document types
    private void setupTypeMappings(EzElastic.Client documentClient, EzSecurityToken token) {
        try {
            // mapping for ssr_default
            String type = SSRUtils.SSR_DEFAULT_TYPE_NAME;
            if (!typeCache.containsKey(type)) {
                documentClient.setTypeMapping(type, getSSRDefaultTypeMap(type), token);
                typeCache.put(type, true);
            }
        } catch (TException e) {
            logger.error("setupTypeMappings exception", e);
        } catch (IOException e) {
            logger.error("setupTypeMappings exception", e);
        }
    }

    @Override
    public List<IndexResponse> putWithDocs(Map<SSR, String> ssrJsonMap, EzSecurityToken userToken) throws TException {

        List<Document> toIndex = new ArrayList<>();

        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), userToken)
                .arg("event", "putWithDocs");
        HashMap<String, String> auditArgs = Maps.newHashMap();
        userToken = validateAndFetchDerived(userToken);
        EzElastic.Client documentClient = null;
        List<IndexResponse> responses = null;
        try {

            for (Map.Entry<SSR, String> entry : ssrJsonMap.entrySet()) {
                SSR ssr = entry.getKey();
                String type = getTypeFromUri(ssr.getUri());
                if (!typeCache.containsKey(type)) {
                    // If it already exists and just isn't in the cache there is no harm
                    logger.info("Setting up initial mapping for type ({})", type);
                    try {
                        documentClient = getDocumentClient();
                        documentClient.setTypeMapping(type, getSSRTypeMap(type), userToken);
                    } finally {
                        documentClient = returnAndNullClient(documentClient);
                    }
                    typeCache.put(type, true);
                }
                if (ssr.getTimeOfIngest() == null) {
                    ssr.setTimeOfIngest(TimeUtil.getCurrentThriftDateTime());
                }

                toIndex.add(generateDocument(ssr, getCombinedJSON(ssr, entry.getValue())));
                auditArgs.put("uri", ssr.getUri());
            }

            try {
                documentClient = getDocumentClient();
                responses = documentClient.bulkPut(toIndex, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }

            // key: document type, value: list of document ids
            Map<String, List<String>> typeMap = groupByType(responses);
            // key: perlocator id, value: list of document ids to add to inbox
            Map<String, List<PercolatorHit>> percolatorInboxHitMap = new HashMap<>();
            // key: document id, value: percolateHit object
            Map<String, PercolatorHit> idsToInboxHitDocument = new HashMap<>();
            for (String type : typeMap.keySet()) {
                List<PercolateQuery> percolateResponse;
                try {
                    documentClient = getDocumentClient();
                    percolateResponse = documentClient.percolateByIds(typeMap.get(type), type, MAX_PERCOLATOR_RESPONSE, userToken);
                } finally {
                    documentClient = returnAndNullClient(documentClient);
                }

                for (PercolateQuery percolateQuery : percolateResponse) {
                    String matchingDocId = percolateQuery.getMatchingDocId();
                    String percolatorId = percolateQuery.getId();

                    // this document should be added to the percolator inbox
                    if (!percolatorInboxHitMap.containsKey(percolatorId)) {
                        percolatorInboxHitMap.put(percolatorId, new ArrayList<PercolatorHit>());
                    }

                    if (!idsToInboxHitDocument.containsKey(matchingDocId)) {
                        idsToInboxHitDocument.put(matchingDocId, getPercolatorHitDocument(matchingDocId, ssrJsonMap.keySet()));
                    }
                    PercolatorHit hit = idsToInboxHitDocument.get(matchingDocId);
                    // should not happen, but just to be safe.
                    if (hit != null) {
                        percolatorInboxHitMap.get(percolatorId).add(hit);
                    }
                }
            }

            for (Map.Entry<String, List<PercolatorHit>> entry : percolatorInboxHitMap.entrySet()) {
                addPercolatorHits(entry.getKey(), entry.getValue(), userToken);
            }
        } catch (DocumentIndexingException e) {
            logError(e, evt, "Failed to index records");
            throw new TException("Error indexing records - document index exception", e);
        } catch (Exception e) {
            logError(e, evt, "putWithDocs encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            auditLogger.logEvent(evt);
            returnAndNullClient(documentClient);
        }
        return responses;
    }

    // construct PercolatorHit from SSR
    private PercolatorHit getPercolatorHitDocument(String targetDocId, Set<SSR> ssrSet) {
        for (SSR ssr : ssrSet) {
            if (ssr.getUri().equals(targetDocId)) {
                PercolatorHit hitDoc = new PercolatorHit();
                hitDoc.setDocumentId(ssr.getUri());
                hitDoc.setTimeOfIngest(ssr.getTimeOfIngest());

                return hitDoc;
            }
        }

        logger.warn("Document not found from SSR set with id " + targetDocId);
        return null;
    }

    private Map<String, List<String>> groupByType(List<IndexResponse> indexResponses) {
        Map<String, List<String>> typeMap = new HashMap<>();
        for (IndexResponse response : indexResponses) {
            if (response.isSuccess()) {
                String type = response.get_type();
                if (!typeMap.containsKey(type)) {
                    typeMap.put(type, new LinkedList<String>());
                }
                typeMap.get(type).add(response.get_id());
            }
        }
        return typeMap;
    }

    @Override
    public SearchResult search(Query query, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), userToken)
                .arg("event", "search")
                .arg("query", query.toString());

        EzElastic.Client documentClient = null;
        try {
            userToken = validateAndFetchDerived(userToken);
            documentClient = getDocumentClient();
            return documentClient.query(query, userToken);
        } catch (Exception e) {
            logError(e, evt, "search encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public IndexResponse putPercolateQuery(String name, PercolateQuery percolateQuery, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), userToken)
                .arg("event", "putPercolateQuery")
                .arg("percolateQuery", percolateQuery);
        userToken = validateAndFetchDerived(userToken);

        EzElastic.Client documentClient = null;
        try {
            // This sections checks how many results were ingested in the last week that match the query
            DateHistogramFacet dateHistogramFacet = new DateHistogramFacet();
            DateField dateField = new DateField();
            dateField.set_field(SSRUtils.SSR_TIME_OF_INGEST);
            dateHistogramFacet.setField(dateField);

            DateInterval dateInterval = new DateInterval();
            dateInterval.setStaticInterval(DateIntervalType.WEEK);
            dateHistogramFacet.setInterval(dateInterval);

            // This is just a simple DateHistogram Facet request  but is complicated by thrift objects
            FacetRequest facetRequest = new FacetRequest();
            facetRequest.setDateHistogramFacet(dateHistogramFacet);
            Facet facet = new Facet();
            facet.setFacet(facetRequest);
            facet.setLabel("pastWeek");

            Query query = new Query();
            query.addToFacets(facet);
            String percolateQueryDoc = percolateQuery.getQueryDocument();
            JSONObject jsonObject = new JSONObject(percolateQueryDoc);
            // The actual query of the percolator is stored under the key "query"
            Object searchString = jsonObject.get("query");
            query.setSearchString(searchString.toString());
            SearchResult response = null;
            try {
                documentClient = getDocumentClient();
                response = documentClient.query(query, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }

            // If there are too many results then the percolator is rejected for being too broad
            DateHistogramFacetResult dateHistogramFacetResult = response.getFacets().get("pastWeek").getDateFacetResult();
            if (dateHistogramFacetResult.getEntriesSize() > 0 && dateHistogramFacetResult.entries.get(0).getCount() > MAX_WEEKLY_PERCOLATOR_RESULTS)
                throw new TException("Percolate query is too broad. Please be more specific");
            IndexResponse indexResponse = null;
            try {
                documentClient = getDocumentClient();
                indexResponse = documentClient
                        .addPercolateQuery(percolateQuery, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            percolateQuery.setId(indexResponse.get_id());

            // Add this percolator id to the user's main inbox
            addToMainPercolatorInbox(indexResponse.get_id(), userToken);

            // Initialize the individual inbox
            initializeIndividualPercolatorInbox(name, percolateQuery, userToken);

            return indexResponse;

        } catch (Exception e) {
            logError(e, evt, "putPercolateQuery encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public SearchResult queryPercolators(Query query, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), userToken)
                .arg("event", "getPercolateQuery")
                .arg("query", query);
        userToken = validateAndFetchDerived(userToken);

        EzElastic.Client documentClient = null;
        try {
            query.setType(ElasticUtils.PERCOLATOR_TYPE);
            documentClient = getDocumentClient();
            return documentClient.queryPercolate(query, userToken);
        } catch (Exception e) {
            logError(e, evt, "queryPercolators encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    private String getPercolatorInboxId(String percolatorId) {
        return percolatorId + "_inbox";
    }

    private String getPercolatorMainInboxId(EzSecurityToken userToken) throws UnsupportedEncodingException {
        return getEncodedUserPrincipal(userToken) + "_maininbox";
    }

    /*
      This method will add the passed percolatorId to the main inbox for the user who's usertoken is passed. If the inbox
      doesn't exist yet then one will be created.
     */
    private boolean addToMainPercolatorInbox(String id, EzSecurityToken userToken) throws TException, UnsupportedEncodingException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), userToken)
                .arg("event", "addToMainPercolatorInbox")
                .arg("id", id);
        EzElastic.Client documentClient = null;
        boolean result = false;
        try {
            String mainInboxId = getPercolatorMainInboxId(userToken);
            Document response = null;
            try {
                documentClient = getDocumentClient();
                response = documentClient.getWithType(mainInboxId, SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            // If the id is null then the inbox doesn't exist yet and it must be made
            if (response.get_id() == null) {
                if (!typeCache.containsKey(SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD)) {
                    // If it already exists and just isn't in the cache there is no harm
                    logger.info("Setting up initial mapping for type ({})", SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD);
                    try {
                        documentClient = getDocumentClient();
                        documentClient.setTypeMapping(SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, getMainInboxMapping(), userToken);
                    } finally {
                        documentClient = returnAndNullClient(documentClient);
                    }
                    typeCache.put(SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, true);
                }

                Visibility visibility = new Visibility();
                // TODO revisit the visibility level to use
                visibility.setFormalVisibility(userToken.getAuthorizationLevel());

                // Creates a new Main percolator inbox with only one percolator id and the current date as the last flushed date
                JSONObject mainInboxJson = new JSONObject();
                JSONArray jsonArrayIds = new JSONArray();
                jsonArrayIds.put(id);
                mainInboxJson.put(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS, jsonArrayIds);

                // Create the inbox
                Document mainInboxDoc = new Document();
                mainInboxDoc.set_id(mainInboxId);
                mainInboxDoc.set_type(SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD);
                mainInboxDoc.setVisibility(visibility);
                mainInboxDoc.set_jsonObject(mainInboxJson.toString());
                IndexResponse mainPutResponse = null;
                try {
                    documentClient = getDocumentClient();
                    mainPutResponse = documentClient.put(mainInboxDoc, userToken);
                } finally {
                    documentClient = returnAndNullClient(documentClient);
                }
                if (!mainPutResponse.isSuccess()) {
                    throw new TException("Failed to create a main inbox for user " + userToken.getTokenPrincipal().getName());
                }
                result = true;
            } else {
                // This loop is to ensure a version doesn't get overwritten
                boolean successFullyInserted = false;
                while (!successFullyInserted) {
                    try {
                        // If inbox already exits then get the list of ids
                        JSONObject jsonObject = new JSONObject(response.get_jsonObject());
                        JSONArray jsonArrayIds = (JSONArray) jsonObject.get(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS);

                        // This checks to see if the percolator id is already in the list (in the case the percolator is just being updated)
                        String jsonIdsInString = jsonArrayIds.toString();
                        if (!jsonIdsInString.contains(id)) {
                            // If it's not then add it
                            jsonArrayIds.put(id);
                            jsonObject.put(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS, jsonArrayIds);
                            response.set_jsonObject(jsonObject.toString());
                            IndexResponse inboxResponse = null;
                            try {
                                documentClient = getDocumentClient();
                                inboxResponse = documentClient.put(response, userToken);
                            } finally {
                                documentClient = returnAndNullClient(documentClient);
                            }
                            pool.returnToPool(documentClient);
                            if (!inboxResponse.isSuccess())
                                throw new TException("Failed to insert percolatorId into main percolator inbox");
                        } else
                            successFullyInserted = true;
                        // If it attempts to insert the wrong version of the document a VersionConflictEngineExcetion is thrown
                    } catch (VersionConflictEngineException e) {
                        try {
                            documentClient = getDocumentClient();
                            response = documentClient
                                    .getWithType(mainInboxId, SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, userToken);
                        } finally {
                            documentClient = returnAndNullClient(documentClient);
                        }
                    }
                }
                result = true;
            }
        } catch (Exception e) {
            logError(e, evt, "addToMainPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            if (documentClient != null)
                pool.returnToPool(documentClient);
            auditLogger.logEvent(evt);
        }
        return result;
    }

    // create a new percolator individual inbox document.
    private boolean initializeIndividualPercolatorInbox(String name, PercolateQuery percolator, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectCreate.getName(), userToken)
                .arg("event", "initializeIndividualPercolatorInbox")
                .arg("percolatorId", percolator.getId());
        EzElastic.Client documentClient = null;

        try {

            if (!typeCache.containsKey(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD)) {
                // If it already exists and just isn't in the cache there is no harm
                logger.info("Setting up initial mapping for type ({})", SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD);
                String mapping = getIndividualPercolatorInboxMapping();
                try {
                    documentClient = getDocumentClient();
                    documentClient.setTypeMapping(
                            SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, mapping, userToken);
                } finally {
                    documentClient = returnAndNullClient(documentClient);
                }
                typeCache.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, true);
            }

            // Create the percolator hit and add it to the inbox object
            Visibility visibility = new Visibility();
            // TODO revisit the visibility level to use
            visibility.setFormalVisibility(userToken.getAuthorizationLevel());
            JSONObject percolatorInboxMap = new JSONObject();
            SimpleDateFormat ingestFormatter = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);
            percolatorInboxMap.put(SSRUtils.PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED, ingestFormatter.format(new Date()));
            percolatorInboxMap.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_NAME, name);
            percolatorInboxMap.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_SEARCH_TEXT, percolator.getQueryDocument());
            JSONArray docHits = new JSONArray();
            percolatorInboxMap.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS, docHits);
            percolatorInboxMap.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT, false);

            // Put the inbox into ElasticSearch
            Document percolatorInboxDoc = new Document();
            String percolatorInboxId = getPercolatorInboxId(percolator.getId());
            percolatorInboxDoc.set_id(percolatorInboxId);
            percolatorInboxDoc.set_type(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD);
            percolatorInboxDoc.setVisibility(visibility);
            percolatorInboxDoc.set_jsonObject(percolatorInboxMap.toString());
            IndexResponse mainPutResponse = null;
            try {
                documentClient = getDocumentClient();
                mainPutResponse = documentClient.put(percolatorInboxDoc, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            if (!mainPutResponse.isSuccess()) {
                throw new TException("Failed to create a inbox for user " + userToken.getTokenPrincipal().getName() + " for percolator " + percolator.getId());
            }
            return true;
        } catch (Exception e) {
            logError(e, evt, "initializeIndividualPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    // When a document is ingested and there is a percolator that matches that doc this method is called. This method adds the hit to the inbox
    private boolean addPercolatorHits(String percolatorId, List<PercolatorHit> hits, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), userToken)
                .arg("event", "addPercolatorHits")
                .arg("percolatorId", percolatorId)
                .arg("hits", hits.size());
        EzElastic.Client documentClient = null;
        SimpleDateFormat ingestFormatter = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);

        boolean result = false;
        try {
            // get the inbox
            String percolatorInboxId = getPercolatorInboxId(percolatorId);
            Document response = null;
            try {
                documentClient = getDocumentClient();
                response = documentClient.getWithType(percolatorInboxId, SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }

            // If the response of the get has a null id then it doesn't exist
            if (response.get_id() == null) {
                logger.warn("Individual percolator inbox doesn't exist for id " + percolatorId);
                evt.failed();
                return false;
            }

            // This loop is to ensure a version doesn't get overwritten
            boolean successFullyInserted = false;
            while (!successFullyInserted) {
                try {
                    // get existing hit array
                    JSONObject jsonObject = new JSONObject(response.get_jsonObject());

                    // check if hit already exceeds limit
                    if (jsonObject.has(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT) &&
                            jsonObject.getBoolean(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT)) {
                        logger.warn("Individual percolator inbox exceeds hit limit. Will not put new hits in.");
                        return false;
                    }

                    JSONArray jsonArrayIds = (JSONArray) jsonObject.get(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS);

                    // put existing records in a map to eliminate duplicate
                    SimpleDateFormat dateFormat = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);
                    Map<String, Date> hitMap = new HashMap<>();
                    for (int i = 0; i < jsonArrayIds.length(); i++) {
                        JSONObject hit = jsonArrayIds.getJSONObject(i);
                        String docId = hit.getString(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_ID);
                        Date ingestDate = dateFormat.parse(hit.getString(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_TIMEOFINGEST));

                        hitMap.put(docId, ingestDate);
                    }

                    int hitCount = hitMap.values().size();
                    // add new record to the map if not exist yet or ingest time is later
                    for (PercolatorHit hit : hits) {
                        String docId = hit.getDocumentId();
                        Date ingestTime = new Date(TimeUtil.convertFromThriftDateTime(hit.getTimeOfIngest()));

                        // we get a new hit, increment the count
                        if (!hitMap.containsKey(docId)) {
                            hitCount++;
                        }
                        if (!hitMap.containsKey(docId) || (hitMap.get(docId).compareTo(ingestTime) < 0)) {
                            // check if exceeds hit limit
                            if (hitCount <= percolatorInboxLimit) {
                                hitMap.put(docId, ingestTime);
                            } else {
                                logger.warn("Individual percolator inbox exceeds hit limit. Will not put new hits in.");
                                break;
                            }
                        }
                    }

                    // re-construct the json array using the map
                    jsonArrayIds = new JSONArray();
                    for (Map.Entry<String, Date> hit : hitMap.entrySet()) {
                        JSONObject docHit = new JSONObject();
                        docHit.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_ID, hit.getKey());
                        docHit.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_TIMEOFINGEST, ingestFormatter.format(hit.getValue()));
                        jsonArrayIds.put(docHit);
                    }

                    // update the percolate document with the new hit array
                    jsonObject.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS, jsonArrayIds);
                    // update exceedLimit property
                    jsonObject.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT, hitCount > percolatorInboxLimit);
                    response.set_jsonObject(jsonObject.toString());
                    IndexResponse inboxResponse = null;
                    try {
                        documentClient = getDocumentClient();
                        inboxResponse = documentClient.put(response, userToken);
                    } finally {
                        documentClient = returnAndNullClient(documentClient);
                    }

                    if (inboxResponse.isSuccess())
                        successFullyInserted = true;
                    else
                        throw new TException("Failed to insert doc hit into percolator inbox");
                } catch (VersionConflictEngineException e) {
                    try {
                        documentClient = getDocumentClient();
                        response = documentClient.getWithType(
                                percolatorInboxId, SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, userToken);
                    } finally {
                        documentClient = returnAndNullClient(documentClient);
                    }
                }
            }
            result = true;

        } catch (Exception e) {
            logError(e, evt, "addPercolatorHit encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
        return result;
    }

    @Override
    public PercolatorInboxPeek peekPercolatorInbox(String percolatorId, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), userToken)
                .arg("event", "peekPercolatorInbox");
        try {
            return percolatorInboxPeekHelper(percolatorId, userToken);
        } catch (Exception e) {
            logError(e, evt, "peekPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } finally {
            auditLogger.logEvent(evt);
        }
    }

    private PercolatorInboxPeek percolatorInboxPeekHelper(String percolatorId, EzSecurityToken userToken) throws TException, ParseException {
        EzElastic.Client documentClient = null;
        try {
            // Get the inbox from EzElastic
            PercolatorInboxPeek result = new PercolatorInboxPeek();
            String inboxId = getPercolatorInboxId(percolatorId);
            Document percolatorInboxResponse = null;
            try {
                documentClient = getDocumentClient();
                percolatorInboxResponse = documentClient.getWithType(inboxId, SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            if (percolatorInboxResponse.get_id() != null) {

                // Get the list of percolators and the lastFlush date from the inbox
                JSONObject jsonObject = new JSONObject(percolatorInboxResponse.get_jsonObject());
                String name = jsonObject.getString(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_NAME);
                String searchText = jsonObject.getString(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_SEARCH_TEXT);
                JSONArray individualJsonArrayIds = jsonObject.getJSONArray(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS);
                String lastFlushedString = jsonObject.getString(SSRUtils.PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED);
                SimpleDateFormat ingestFormatter = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);
                Date lastFlushedDate = ingestFormatter.parse(lastFlushedString);

                result.setLastFlushed(TimeUtil.convertToThriftDateTime(lastFlushedDate.getTime()));
                result.setCountOfHits(individualJsonArrayIds.length());
                result.setName(name);
                result.setSearchText(searchText);

                return result;
            } else {
                throw new TException("Could not get the percolatorInbox for this percolator id:" + percolatorId);
            }
        } finally {
            returnAndNullClient(documentClient);
        }
    }

    @Override
    public PercolatorHitInbox getAndFlushPercolatorInbox(String percolatorId, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), userToken)
                .arg("event", "getAndFlushPercolatorInbox");
        EzElastic.Client documentClient = null;
        try {
            // Get the inbox from EzElastic
            userToken = validateAndFetchDerived(userToken);
            PercolatorHitInbox result = new PercolatorHitInbox();
            String inboxId = getPercolatorInboxId(percolatorId);
            Document percolatorInboxResponse = null;
            try {
                documentClient = getDocumentClient();
                percolatorInboxResponse = documentClient.getWithType(inboxId, SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            if (percolatorInboxResponse.get_id() == null)
                throw new TException("Could not get the percolatorInbox for this percolator id:" + percolatorId);


            // Get the lastFlush date from the inbox
            JSONObject jsonObject = new JSONObject(percolatorInboxResponse.get_jsonObject());
            String lastFlushedString = jsonObject.getString(SSRUtils.PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED);
            SimpleDateFormat ingestFormatter = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);
            Date lastFlushedDate = ingestFormatter.parse(lastFlushedString);
            result.setLastFlushed(TimeUtil.convertToThriftDateTime(lastFlushedDate.getTime()));
            boolean exceedsLimit = false;
            if (jsonObject.has(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT)) {
                exceedsLimit = jsonObject.getBoolean(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT);
            }
            result.setExceedLimit(exceedsLimit);

            // Get the array of document hit
            JSONArray hitArray = jsonObject.getJSONArray(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS);

            // the query to query SSR by ids
            IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();

            for (int i = 0; i < hitArray.length(); i++) {
                // prepare the doc ids to query again for SSR
                String documentId = hitArray.getJSONObject(i).getString(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_ID);
                idsQueryBuilder.addIds(documentId);
            }

            // query the matching SSRs from database again
            try {
                documentClient = getDocumentClient();

                Query query = new Query(idsQueryBuilder.toString());
                query.setReturnedFields(ImmutableSet.of(SSRUtils.SSR_FIELD));
                // we want all the documents
                query.setPage(new Page(0, (short)hitArray.length()));
                // set the facets
                query.setFacets(buildSSRFacets());
                // we care only documents have ssr field
                addExistSSRFilter(query);
                SearchResult ssrResults = documentClient.query(query, userToken);

                // The matching ssr results for document hits
                result.setHits(processSsrSearchResult(ssrResults, (short)ssrResults.getTotalHits(), 0));
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }

            // Set the last flush date to the current date
            jsonObject.put(SSRUtils.PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED, ingestFormatter.format(new Date()));
            jsonObject.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS, new JSONArray());
            // Clear exceedLimit flag
            jsonObject.put(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_EXCEEDS_LIMIT, false);
            percolatorInboxResponse.set_jsonObject(jsonObject.toString());
            IndexResponse inboxFlushingResponse = null;
            try {
                documentClient = getDocumentClient();
                inboxFlushingResponse = documentClient.put(percolatorInboxResponse, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            if (!inboxFlushingResponse.isSuccess())
                logError(new TException("getAndFlushPercolatorInbox failed to update the inbox"), evt, "getAndFlushPercolatorInbox failed to update the inbox");
            return result;

        } catch (ParseException e) {
            logError(e, evt, "getAndFlushPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } catch (Exception e) {
            logError(e, evt, "getAndFlushPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public Map<String, PercolatorInboxPeek> peekMainPercolatorInbox(EzSecurityToken userToken)
            throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectModify.getName(), userToken)
                .arg("event", "peekMainPercolatorInbox");
        EzElastic.Client documentClient = null;
        Map<String, PercolatorInboxPeek> results;
        try {
            // Get the inbox from EzElastic
            userToken = validateAndFetchDerived(userToken);
            results = new HashMap<>();
            String mainInboxId = getPercolatorMainInboxId(userToken);
            Document mainInboxResponse = null;
            try {
                documentClient = getDocumentClient();
                mainInboxResponse = documentClient.getWithType(
                        mainInboxId, SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }

            if (!mainInboxResponse.get_jsonObject().isEmpty()) {
                // Get the last flushed date and list of percolators from the main inbox
                JSONObject jsonObject = new JSONObject(mainInboxResponse.get_jsonObject());
                JSONArray jsonArrayIds = jsonObject.getJSONArray(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS);

                // Iterate over the percolator ids getting their inboxes
                for (int mainInboxCounter = 0; mainInboxCounter < jsonArrayIds.length(); mainInboxCounter++) {
                    String percolatorId = jsonArrayIds.getString(mainInboxCounter);
                    try {
                        PercolatorInboxPeek percolatorInboxPeek = percolatorInboxPeekHelper(percolatorId, userToken);
                        results.put(percolatorId, percolatorInboxPeek);
                    } catch (Exception e) {
                        logError(e, evt, "percolatorInboxPeek failed [" + e.getClass().getName() + ":" + e.getMessage() + "] for this percolator:" + "(id:" + percolatorId + ")");
                    }
                }
            }

            return results;
        } catch (UnsupportedEncodingException e) {
            logError(e, evt, "peekMainPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } catch (Exception e) {
            logError(e, evt, "peekMainPercolatorInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            if (documentClient != null)
                pool.returnToPool(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public IndexResponse updatePercolateInbox(String name, String percolateId, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectDelete.getName(), userToken)
                .arg("event", "updatePercolateInbox")
                .arg("name", name)
                .arg("percolateId", percolateId);

        userToken = validateAndFetchDerived(userToken);
        EzElastic.Client documentClient = null;
        try {
            documentClient = getDocumentClient();

            DocumentIdentifier docId = new DocumentIdentifier(getPercolatorInboxId(percolateId));
            docId.setType(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD);

            UpdateScript script = new UpdateScript();
            final String nameParam = "name";
            script.setScript("ctx._source." + SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_NAME + " = " + nameParam);
            Map<String, String> parameters = new HashMap<>();
            parameters.put(nameParam, name);
            script.setParameters(parameters);

            UpdateOptions options = new UpdateOptions();
            options.setRetryCount(3);

            return documentClient.update(docId, script, options, userToken);
        } catch (TException e) {
            logError(e, evt, "updatePercolateInbox encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public boolean deletePercolateQuery(String id, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectDelete.getName(), userToken)
                .arg("event", "deletePercolateQuery")
                .arg("percolateId", id);
        userToken = validateAndFetchDerived(userToken);
        EzElastic.Client documentClient = null;
        try {
            documentClient = getDocumentClient();
            boolean result = documentClient.removePercolateQuery(id, userToken);
            pool.returnToPool(documentClient);

            // When deleting a percolator query it also needs to be removed from the main percolator inbox the user
            String mainInboxId = getPercolatorMainInboxId(userToken);
            Document mainInboxResponse = null;
            try {
                documentClient = getDocumentClient();
                mainInboxResponse = documentClient.getWithType(mainInboxId, SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, userToken);
            } finally {
                documentClient = returnAndNullClient(documentClient);
            }
            JSONObject jsonObject = new JSONObject(mainInboxResponse.get_jsonObject());
            JSONArray jsonArrayIds;
            jsonArrayIds = (JSONArray) jsonObject.get(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS);

            // Iterate over the inbox to see if the id is there.
            for (int mainInboxCounter = 0; mainInboxCounter < jsonArrayIds.length(); mainInboxCounter++) {
                if (jsonArrayIds.get(mainInboxCounter).equals(id)) {
                    // This loop ensures no version gets overwritten
                    boolean successFullyInserted = false;
                    while (!successFullyInserted) {
                        try {
                            // Remove the percolator id from the list and insert the main inbox back
                            jsonArrayIds.remove(mainInboxCounter);
                            jsonObject.put(SSRUtils.PERCOLATOR_MAIN_INBOX_IDS, jsonArrayIds);
                            mainInboxResponse.set_jsonObject(jsonObject.toString());
                            IndexResponse inboxResponse = null;
                            try {
                                documentClient = getDocumentClient();
                                inboxResponse = documentClient.put(mainInboxResponse, userToken);
                            } finally {
                                documentClient = returnAndNullClient(documentClient);
                            }
                            if (inboxResponse.isSuccess())
                                successFullyInserted = true;
                            else
                                throw new TException("Failed to insert doc hit into percolator inbox");
                        } catch (VersionConflictEngineException e) {
                            try {
                                documentClient = getDocumentClient();
                                mainInboxResponse = documentClient
                                        .getWithType(mainInboxId, SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD, userToken);
                            } finally {
                                documentClient = returnAndNullClient(documentClient);
                            }
                        }
                    }
                    mainInboxCounter = jsonArrayIds.length();
                }
            }

            // delete the percolator inbox document too
            try {
                documentClient = getDocumentClient();
                documentClient.deleteWithType(getPercolatorInboxId(id), SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD, userToken);
            }finally {
                documentClient = returnAndNullClient(documentClient);
            }
            return result;
        } catch (UnsupportedEncodingException e) {
            logError(e, evt, "deletePercolateQuery encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw new TException(e);
        } catch (Exception e) {
            logError(e, evt, "deletePercolateQuery encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            returnAndNullClient(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public List<PercolateQuery> percolate(Map<SSR, String> ssrJsonMap, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), userToken)
                .arg("event", "percolate");
        EzElastic.Client documentClient = null;
        List<Document> toPercolate = new ArrayList<>();
        userToken = validateAndFetchDerived(userToken);

        try {
            for (Map.Entry<SSR, String> entry : ssrJsonMap.entrySet()) {
                SSR ssr = entry.getKey();
                String type = getTypeFromUri(ssr.getUri());
                if (!typeCache.containsKey(type)) {
                    // If it already exists and just isn't in the cache there is no harm
                    logger.info("Setting up initial mapping for type ({})", type);
                    try {
                        documentClient = getDocumentClient();
                        documentClient.setTypeMapping(
                                type, getSSRTypeMap(type), userToken);
                    } finally {
                        documentClient = returnAndNullClient(documentClient);
                    }
                    typeCache.put(type, true);
                }
                Document document = generateDocument(ssr, getCombinedJSON(ssr, entry.getValue()));
                document.setPercolate(new PercolateRequest());
                toPercolate.add(document);
                evt.arg("uri", ssr.getUri());
            }
            documentClient = getDocumentClient();
            return documentClient.percolate(toPercolate, userToken);
        } catch (Exception e) {
            logError(e, evt, "percolate encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            if (documentClient != null)
                pool.returnToPool(documentClient);
            auditLogger.logEvent(evt);
        }
    }

    @Override
    public SSRSearchResult searchSSR(Query query, EzSecurityToken userToken) throws TException {
        AuditEvent evt = event(AuditEventType.FileObjectAccess.getName(), userToken)
                .arg("event", "searchSSR")
                .arg("query", query.toString());
        try {
            userToken = validateAndFetchDerived(userToken);
            SearchResult datasetResults = new SearchResult();
            EzElastic.Client documentClient = null;
            try {
                documentClient = getDocumentClient();
                query.setReturnedFields(ImmutableSet.of(SSRUtils.SSR_FIELD));
                if (!query.isSetFacets()) {
                    query.setFacets(new ArrayList<Facet>());
                }
                query.getFacets().addAll(buildSSRFacets());

                // we care only documents have ssr field
                addExistSSRFilter(query);

                datasetResults = documentClient.query(query, userToken);
            } catch (MalformedQueryException e) {
                logger.error("Query was malformed");
                throw new TException(e);
            } finally {
                if (documentClient != null)
                    pool.returnToPool(documentClient);
            }

            return processSsrSearchResult(datasetResults, query.getPage().getPageSize(), query.getPage().getOffset());

        } catch (Exception e) {
            logError(e, evt, "searchSSR encountered an exception [" + e.getClass().getName() + ":" + e.getMessage() + "]");
            throw e;
        } finally {
            auditLogger.logEvent(evt);
        }

    }

    // convert the searchResult to SSRSearchResult
    private SSRSearchResult processSsrSearchResult(SearchResult datasetResults, short pageSize, int offset) {
        SSRSearchResult results = new SSRSearchResult();
        results.setTotalHits(datasetResults.getTotalHits());
        results.setPageSize(pageSize);
        results.setOffset(offset);
        results.setMatchingRecords(new ArrayList<SSR>());
        if (datasetResults.isSetHighlights()) {
            results.setHighlights(datasetResults.getHighlights());
        }
        for (Document match : datasetResults.getMatchingDocuments()) {
            String jsonObjectAsString = match.get_jsonObject();
            if (jsonObjectAsString == null) {
                logger.error("Document had no json object");
            }
            JsonElement jsonElement = jsonParser.parse(jsonObjectAsString);
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement ssrObject = jsonObject.get(SSRUtils.SSR_FIELD);
            String ssrJson = ssrObject.toString();
            SSR ssrResult = gson.fromJson(ssrJson, SSR.class);
            ssrResult.setVisibility(match.getVisibility());
            results.addToMatchingRecords(ssrResult);
        }

        results.setFacets(new HashMap<String, FacetCategory>());

        if (results.getTotalHits() > 0) {
            Map<String, FacetCategory> facetValues = new HashMap<>();

            FacetCategory dateCategory = new FacetCategory();
            dateCategory.setField(SSRUtils.SSR_DATE_FIELD);
            dateCategory.setFacetValues(getDateFacets(datasetResults.getFacets().get(DATE_FACET_KEY)));
            facetValues.put(DATE_FACET_KEY, dateCategory);

            // ingest time category
            FacetCategory ingestCategory = new FacetCategory();
            ingestCategory.setField(SSRUtils.SSR_TIME_OF_INGEST);
            ingestCategory.setFacetValues(getDateFacets(datasetResults.getFacets().get(INGEST_FACET_KEY)));
            facetValues.put(INGEST_FACET_KEY, ingestCategory);

            FacetCategory visibilityCategory = new FacetCategory();
            visibilityCategory.setField("ezbake_auths");
            visibilityCategory.setFacetValues(getVisibilityFacets(datasetResults.getFacets().get(VISIBILITY_FACET_KEY)));
            facetValues.put(VISIBILITY_FACET_KEY, visibilityCategory);

            FacetCategory typeCategory = new FacetCategory();
            typeCategory.setField(SSRUtils.SSR_TYPE_FIELD);
            typeCategory.setFacetValues(getTermFacets(datasetResults.getFacets().get(TYPE_FACET_KEY)));
            facetValues.put(TYPE_FACET_KEY, typeCategory);

            if (isGeoEnabled) {
                FacetCategory countryCategory = new FacetCategory();
                countryCategory.setField(SSRUtils.SSR_COUNTRY_FIELD);
                countryCategory.setFacetValues(getTermFacets(datasetResults.getFacets().get(GEO_COUNTRY_FACET_KEY)));
                facetValues.put(GEO_COUNTRY_FACET_KEY, countryCategory);

                FacetCategory provinceCategory = new FacetCategory();
                provinceCategory.setField(SSRUtils.SSR_PROVINCE_FIELD);
                provinceCategory.setFacetValues(getTermFacets(datasetResults.getFacets().get(GEO_PROVINCE_FACET_KEY)));
                facetValues.put(GEO_PROVINCE_FACET_KEY, provinceCategory);
            }

            results.setFacets(facetValues);
        }

        return results;
    }

    // add {"exists":{"field":"_ssr"}} to the filter
    private void addExistSSRFilter(Query query) {
        ExistsFilterBuilder existFilter = FilterBuilders.existsFilter(SSRUtils.SSR_FIELD);
        if (!query.isSetFilterJson()) {
            query.setFilterJson(existFilter.toString());
        } else {
            AndFilterBuilder filter = FilterBuilders.andFilter();
            filter.add(existFilter);
            filter.add(FilterBuilders.wrapperFilter(query.getFilterJson()));
            query.setFilterJson(filter.toString());
        }

        logger.debug("*** after add ssr filter = " + query.getFilterJson());
    }

    @Override
    public boolean ping() {
        GeospatialExtractorService.Client geoClient = null;
        EzElastic.Client docClient = null;
        try {
            logger.debug("getting document dataset");
            docClient = getDocumentClient();
            logger.debug("getting geo service");
            geoClient = getGeospatialClient();
            boolean result;
            logger.debug("calling ping on doc client");
            result = docClient.ping();
            logger.debug("calling ping on geo client");
            result = result && geoClient.ping();
            return result;
        } catch (TException e) {
            logger.error("SSR ping failed : {}", e.getMessage());
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (docClient != null) {
                pool.returnToPool(docClient);
            }
            if (geoClient != null) {
                pool.returnToPool(geoClient);
            }
        }
        return false;
    }

    private List<FacetValue> getTermFacets(FacetResult facetResult) {
        List<FacetValue> values = new ArrayList<>();

        for (TermsFacetEntry entry : facetResult.getTermsFacetResult().getEntries()) {
            RawValue rawValue = new RawValue();
            rawValue.setStringValue(entry.getTerm());
            values.add(new FacetValue().setLabel(entry.getTerm()).setValue(rawValue).setCount(String.valueOf(entry.getCount())));
        }
        return values;
    }

    private List<FacetValue> getVisibilityFacets(FacetResult facetResult) {
        List<FacetValue> values = new ArrayList<>();
        // TODO
        return values;
    }

    private List<FacetValue> getDateFacets(FacetResult facetResult) {
        List<FacetValue> values = new ArrayList<>();

        for (RangeFacetEntry entry : facetResult.getRangeFacetResult().getEntries()) {
            RawValue rawValue = new RawValue();
            rawValue.setDoubleValue(Double.valueOf(entry.getRangeFrom()));
            values.add(new FacetValue().setLabel(getTimeWindow(Double.valueOf(entry.getRangeFrom()))).setValue(rawValue).setCount(String.valueOf(entry.getCount())));
        }

        return values;
    }

    private String getTimeWindow(double ms) {
        if (ms <= last365daysMS) {
            return "Last Year";
        }

        if (ms <= last90daysMS) {
            return "Last 90 Days";
        }

        if (ms <= last30daysMS) {
            return "Last 30 Days";
        }

        if (ms <= last7daysMS) {
            return "Last Week";
        }

        if (ms <= last72hoursMS) {
            return "Last 72 Hours";
        }

        if (ms <= last48hoursMS) {
            return "Last 48 Hours";
        }

        return "Last 24 Hours";
    }

    private Facet getDateRangeFacets(String field, String label) {
        GregorianCalendar calendar = new GregorianCalendar();

        Facet dateFacet = new Facet();
        RangeFacet dateRangeFacet = new RangeFacet();
        BaseFacetValue dateField = new BaseFacetValue();
        dateField.setFacetField(field);
        dateRangeFacet.setField(dateField);

        FacetRange last24 = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        last24hoursMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        last24.setRangeFrom(String.valueOf(last24hoursMS));
        dateRangeFacet.addToRanges(last24);

        FacetRange last48 = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        last48hoursMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        last48.setRangeFrom(String.valueOf(last48hoursMS));
        dateRangeFacet.addToRanges(last48);

        FacetRange last72 = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        last72hoursMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        last72.setRangeFrom(String.valueOf(last72hoursMS));
        dateRangeFacet.addToRanges(last72);

        FacetRange lastWeek = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -4);
        last7daysMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        lastWeek.setRangeFrom(String.valueOf(last7daysMS));
        dateRangeFacet.addToRanges(lastWeek);

        FacetRange last30Days = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -23);
        last30daysMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        last30Days.setRangeFrom(String.valueOf(last30daysMS));
        dateRangeFacet.addToRanges(last30Days);

        FacetRange last90Days = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -60);
        last90daysMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        last90Days.setRangeFrom(String.valueOf(last90daysMS));
        dateRangeFacet.addToRanges(last90Days);

        FacetRange lastYear = new FacetRange(RangeType.DATE);
        calendar.add(Calendar.DAY_OF_YEAR, -275);
        last365daysMS = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();
        lastYear.setRangeFrom(String.valueOf(last365daysMS));
        dateRangeFacet.addToRanges(lastYear);

        FacetRequest dateRequest = new FacetRequest();
        dateRequest.setRangeFacet(dateRangeFacet);

        dateFacet.setLabel(label);
        dateFacet.setFacet(dateRequest);

        return dateFacet;
    }

    private List<Facet> buildSSRFacets() {
        List<Facet> facets = new ArrayList<>();

        /* Date Facet */
        facets.add(getDateRangeFacets(SSRUtils.SSR_DATE_FIELD, DATE_FACET_KEY));

        /* Ingest Date Facet */
        facets.add(getDateRangeFacets(SSRUtils.SSR_TIME_OF_INGEST, INGEST_FACET_KEY));

        /* Geo Facet via Metacarta */
        if (isGeoEnabled) {
            TermsFacet countryFacet = new TermsFacet();
            countryFacet.setFields(Arrays.asList(SSRUtils.SSR_COUNTRY_FIELD));

            FacetRequest countryFacetRequest = new FacetRequest();
            countryFacetRequest.setTermsFacet(countryFacet);

            Facet ssrCountryFacet = new Facet();
            ssrCountryFacet.setLabel(GEO_COUNTRY_FACET_KEY);
            ssrCountryFacet.setFacet(countryFacetRequest);
            facets.add(ssrCountryFacet);

            TermsFacet provinceFacet = new TermsFacet();
            provinceFacet.setFields(Arrays.asList(SSRUtils.SSR_PROVINCE_FIELD));

            FacetRequest provinceFacetRequest = new FacetRequest();
            provinceFacetRequest.setTermsFacet(provinceFacet);

            Facet ssrProvinceFacet = new Facet();
            ssrProvinceFacet.setLabel(GEO_PROVINCE_FACET_KEY);
            ssrProvinceFacet.setFacet(provinceFacetRequest);
            facets.add(ssrProvinceFacet);
        }
        /* End Geo Facet */

        /* Type Facet */
        TermsFacet typeFacet = new TermsFacet();
        typeFacet.setFields(Arrays.asList(SSRUtils.SSR_TYPE_FIELD));

        FacetRequest typeFacetRequest = new FacetRequest();
        typeFacetRequest.setTermsFacet(typeFacet);

        Facet ssrTypeFacet = new Facet();
        ssrTypeFacet.setLabel(TYPE_FACET_KEY);
        ssrTypeFacet.setFacet(typeFacetRequest);
        facets.add(ssrTypeFacet);
        /* End Type Facet */

        /* Security Facet */
        TermsFacet securityFacet = new TermsFacet();
        securityFacet.setFields(Arrays.asList("ezbake_auths"));

        FacetRequest securityFacetRequest = new FacetRequest();
        securityFacetRequest.setTermsFacet(securityFacet);

        Facet ssrAuthFacet = new Facet();
        ssrAuthFacet.setLabel(VISIBILITY_FACET_KEY);
        ssrAuthFacet.setFacet(securityFacetRequest);
        facets.add(ssrAuthFacet);
         /* End Security Facet */

        return facets;
    }

    private EzElastic.Client getDocumentClient() throws TException {
        return pool.getClient(getConfigurationProperties().getProperty(EZELASTIC_APPLICATION_NAME_KEY),
                getConfigurationProperties().getProperty(EZELASTIC_SERVICE_NAME_KEY),
                EzElastic.Client.class);
    }

    private GeospatialExtractorService.Client getGeospatialClient() throws TException {
        return pool.getClient(GeospatialExtractorConstants.SERVICE_NAME, GeospatialExtractorService.Client.class);
    }

    private Document generateDocument(SSR ssr, String json) {
        Document document = new Document();
        document.set_jsonObject(json);
        document.set_id(ssr.getUri());
        // Type should be application + thrift object type
        document.set_type(getTypeFromUri(ssr.getUri()));
        document.setVisibility(ssr.getVisibility());
        return document;
    }

    private String getCombinedJSON(SSR ssr, String jsonDocument) throws TException {

        Map<String, Object> ssrJson = new HashMap<>();
        Map<String, Double> coordMap = new HashMap<>();
        coordMap.put(SSRUtils.ELASTIC_LATITUDE_DEFAULT, ssr.getCoordinate() != null ? ssr.getCoordinate().getLatitude() : 0.0);
        coordMap.put(SSRUtils.ELASTIC_LONGITUDE_DEFAULT, ssr.getCoordinate() != null ? ssr.getCoordinate().getLongitude() : 0.0);
        SimpleDateFormat resultFormatter = new SimpleDateFormat("ddHHmm'Z' MMM yy");
        ssrJson.put(SSRUtils.SSR_DATE_FIELD, ssr.getResultDate() != null ? resultFormatter.format(new Date(TimeUtil.convertFromThriftDateTime(ssr.getResultDate()))) : null);
        ssrJson.put(SSRUtils.SSR_COORDINATE_FIELD, coordMap);
        ssrJson.put(SSRUtils.SSR_TYPE_FIELD, getTypeFromUri(ssr.getUri()));
        ssrJson.put(SSRUtils.SSR_FIELD, ssr);
        ssrJson.put(SSRUtils.SSR_METADATA_FIELD, ssr.getMetaData() != null ? ssr.getMetaData().getTags() : null);
        SimpleDateFormat ingestFormatter = new SimpleDateFormat(SSR_DEFAULT_DATE_FORMAT);
        ssrJson.put(SSRUtils.SSR_TIME_OF_INGEST, ssr.getTimeOfIngest() != null ? ingestFormatter.format(new Date(TimeUtil.convertFromThriftDateTime(ssr.getTimeOfIngest()))) : null);

        if (ssr.getCoordinate() != null && isGeoEnabled) {
            GeospatialExtractorService.Client geoClient = getGeospatialClient();
            try {
                TLocationFinderResult geoLocation = geoClient
                        .findLocation(new TCentroid(ssr.getCoordinate().getLatitude(),
                                ssr.getCoordinate().getLongitude()), null);
                if (!geoLocation.getLocations().isEmpty()) {
                    // Find the location with the most administrative paths
                    List<String> administrativePaths = getMostAccurateLocation(geoLocation.getLocations());
                    if (!administrativePaths.isEmpty()) {
                        ssrJson.put(SSRUtils.SSR_COUNTRY_FIELD, administrativePaths.get(administrativePaths.size() - 1));
                        if (administrativePaths.size() > 1) {
                            ssrJson.put(SSRUtils.SSR_PROVINCE_FIELD, administrativePaths.get(administrativePaths.size() - 2));
                        }
                        logger.info("SSR being indexed based on geospatial locations : {}",
                                StringUtils.join(administrativePaths, ", "));
                    }
                }
            } finally {
                pool.returnToPool(geoClient);
            }
        }

        String ssrStripped = gson.toJson(ssrJson)
                .replaceFirst(Pattern.quote("{"), StringUtils.EMPTY);
        return jsonDocument.substring(0, jsonDocument.lastIndexOf("}")) + "," + ssrStripped;
    }

    private List<String> getMostAccurateLocation(List<TLocation> locations) {
        List<String> results = new ArrayList<>();
        for (TLocation location : locations) {
            if (location.getPaths().getAdministrativeSize() > results.size()) {
                results = location.getPaths().getAdministrative();
            }
        }
        return results;
    }

    protected String getTypeFromUri(String uri) {
        String type = uri.replace("://", ":");
        return type.substring(0, type.indexOf("/"));
    }

    protected static String getSSRTypeMap(String type) {
        return "{\n" +
                "    \"" + type + "\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"" + SSRUtils.SSR_COORDINATE_FIELD + "\" : {\n" +
                "                \"type\" : \"geo_point\",\n" +
                "                \"lat_lon\" : true\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_DATE_FIELD + "\" : {\n" +
                "                \"type\" : \"date\",\n" +
                "                \"format\" : \"ddHHmm'Z' MMM yy\",\n" +
                "                \"store\" : true\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_FIELD + "\" : {\n" +
                "                \"type\" : \"object\",\n" +
                "                \"store\" : true\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_TYPE_FIELD + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_COUNTRY_FIELD + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_PROVINCE_FIELD + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_METADATA_FIELD + "\" : {\n" +
                "                \"type\" : \"object\",\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"not_analyzed\"\n" +
                "            },\n" +
                "            \"" + SSRUtils.SSR_TIME_OF_INGEST + "\" : {\n" +
                "                \"type\" : \"date\",\n" +
                "                \"format\" : \"yyyyMMdd'T' HHmmss.SSSZ\",\n" +
                "                \"store\" : true\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
    }

    // purge implementation

    /**
     * Deletes documents with given set of ids (uris, more specifically) from the elastic database.
     *
     * @param uriList   set of URIs to delete from SSR
     * @param userToken the token of the user or service initiating this request
     */
    protected void bulkDelete(Set<String> uriList, EzSecurityToken userToken) throws TException {
        EzElastic.Client documentClient = getDocumentClient();
        try {
            documentClient.bulkDelete(uriList, userToken);
        } finally {
            pool.returnToPool(documentClient);
        }
    }

    protected class SSRDeleterRunnable implements Runnable {
        private Set<Long> idsToPurge;
        private EzSecurityToken userToken;
        private long purgeId;

        public SSRDeleterRunnable(Set<Long> idsToPurge, EzSecurityToken userToken, long purgeId) {
            this.idsToPurge = idsToPurge;
            this.userToken = userToken;
            this.purgeId = purgeId;
        }

        @Override
        public void run() {
            Visibility visibility = new Visibility();
            // TODO revisit the visibility level to use
            visibility.setFormalVisibility(userToken.getAuthorizationLevel());
            try {
                PurgeState state = purgeStatus(userToken, purgeId);
                try {

                    if (!(state.getCancelStatus() == CancelStatus.CANCELED && state.getPurgeStatus() == PurgeStatus.ERROR
                            && state.getPurgeStatus() == PurgeStatus.FINISHED_COMPLETE)) {
                        Set<String> uriList = getPurgeUris(idsToPurge, userToken);
                        bulkDelete(uriList, security.fetchDerivedTokenForApp(userToken, securityId));

                        // Purge completed, update the status object
                        state.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
                        state.setTimeStamp(TimeUtil.convertToThriftDateTime(System.currentTimeMillis()));
                        state.setPurged(idsToPurge);
                        state.setNotPurged(Sets.<Long>newHashSet());
                    }
                } catch (Exception e) {
                    logger.error(
                            "The delete of the URIs from the ssr index failed. The requesting purgeId is '"
                                    + purgeId + "'.", e);
                    state.setPurgeStatus(PurgeStatus.ERROR);
                }
                insertPurgeStatus(state, visibility, userToken);
            } catch (EzSecurityTokenException e) {
                logger.error("Could not retrieve chained security token for delete operation", e);
            } catch (TException e) {
                logger.error("Purge could not complete successfully", e);
            }
        }
    }

    @Override
    public PurgeState beginVirusPurge(String purgeCallbackService, long purgeId,
                                      Set<Long> idsToPurge, EzSecurityToken userToken) throws TException {
        return this.beginPurge(purgeCallbackService, purgeId, idsToPurge, userToken);
    }

    /**
     * Always returns a {@link CancelStatus#CANNOT_CANCEL} status and does
     * not cancel previously started purges from ezElastic.
     */
    @Override
    public PurgeState cancelPurge(EzSecurityToken userToken, long purgeId) throws TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "cancelPurge");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(userToken, AuditEventType.FileObjectDelete, auditArgs);

        validateEzCentralPurgeSecurityId(userToken);

        PurgeState state = purgeStatus(userToken, purgeId); // TODO
        state.setCancelStatus(CancelStatus.CANNOT_CANCEL);
        state.setTimeStamp(TimeUtil.convertToThriftDateTime(System.currentTimeMillis()));
        insertPurgeStatus(state, new Visibility().setFormalVisibility(userToken.getAuthorizationLevel()), userToken);

        return state;
    }

    @Override
    public PurgeState beginPurge(String purgeCallbackService, long purgeId,
                                 Set<Long> idsToPurge, EzSecurityToken userToken) throws TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "beginPurge");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(userToken, AuditEventType.FileObjectDelete, auditArgs);

        validateEzCentralPurgeSecurityId(userToken);
        boolean emptyPurgeIds = idsToPurge == null || idsToPurge.isEmpty();

        Visibility visibility = new Visibility();
        // TODO revisit the visibility level to use
        visibility.setFormalVisibility(userToken.getAuthorizationLevel());

        PurgeState state = new PurgeState();
        state.setPurgeId(purgeId);
        state.setPurgeStatus(PurgeStatus.STARTING);
        state.setTimeStamp(TimeUtil.convertToThriftDateTime(System.currentTimeMillis()));
        state.setPurged(Sets.<Long>newHashSet());
        state.setNotPurged(Sets.<Long>newHashSet());
        state.setSuggestedPollPeriod(10000);
        if (emptyPurgeIds) {
            logger.info("No ids were given for purge. Marking the purge as finished.");
            state.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
        }

        insertPurgeStatus(state, visibility, userToken);

        // Checking emptyPurgeIds twice because we want the start up of the deleter thread
        // to be the absolute last thing we do before returning
        if (!emptyPurgeIds) {
            new Thread(new SSRDeleterRunnable(idsToPurge, userToken, purgeId)).start();
        }
        return state;
    }

    @Override
    public PurgeState purgeStatus(EzSecurityToken userToken, long purgeId) throws TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "purgeStatus");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(userToken, AuditEventType.FileObjectAccess, auditArgs);

        validateEzCentralPurgeSecurityId(userToken);

        PurgeState state = new PurgeState();
        state.setPurgeId(purgeId);
        state.setPurgeStatus(PurgeStatus.UNKNOWN_ID);
        state.setTimeStamp(TimeUtil.convertToThriftDateTime(System.currentTimeMillis()));
        Set<Long> emptySet = Sets.newHashSet();
        state.setPurged(emptySet);
        state.setNotPurged(emptySet);
        state.setSuggestedPollPeriod(10000);

        String queryStr = QueryBuilders.matchQuery("purgeId", purgeId).toString();
        Query query = new Query();
        query.setSearchString(queryStr);
        query.setType(SSRUtils.PURGE_TYPE_FIELD);

        ezbake.data.elastic.thrift.SearchResult purgeResults;
        EzElastic.Client documentClient = getDocumentClient();
        try {
            purgeResults = documentClient.query(query, security.fetchDerivedTokenForApp(userToken, securityId));
        } catch (MalformedQueryException e) {
            logger.error("Purge query was malformed.", e);
            throw new TException(e);
        } finally {
            pool.returnToPool(documentClient);
        }

        if (purgeResults.getMatchingDocuments().size() > 0) {
            Document match = purgeResults.getMatchingDocuments().get(0);
            String jsonObjectAsString = match.get_jsonObject();
            if (jsonObjectAsString == null) {
                logger.error("Document had no json object");
            }
            JsonElement jsonElement = jsonParser.parse(jsonObjectAsString);
            state = gson.fromJson(jsonElement, PurgeState.class);
        }

        return state;
    }

    /**
     * Retrieves the document URIs corresponding to given IDs from the
     * Provenance service.
     *
     * @param idsToPurge set of IDs to purge whose URIs we are seeking
     * @param userToken  security token
     * @return set of document URIs
     * @throws TException if any error occurs
     */
    private Set<String> getPurgeUris(Set<Long> idsToPurge,
                                     EzSecurityToken userToken) throws TException {

        ProvenanceService.Client client = null;
        try {
            client = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME,
                    ProvenanceService.Client.class);
            ArrayList<Long> idsToPurgeList = new ArrayList<>();
            idsToPurgeList.addAll(idsToPurge);
            EzSecurityTokenWrapper chained = security.fetchDerivedTokenForApp(userToken, pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME));
            PositionsToUris uriPositions = client.getDocumentUriFromId(chained, idsToPurgeList);

            Map<Long, String> map = uriPositions.getMapping();
            Collection<String> uris = map.values();

            return uris == null ? new HashSet<String>() : new HashSet<>(uris);
        } finally {
            pool.returnToPool(client);
        }
    }

    /**
     * Inserts a purge status into the elastic database via the ezElastic
     * service.
     *
     * @param purgeState containing the purge id, status, time, etc.
     * @param visibility visibility of the purge.
     * @param userToken  security token.
     * @throws TException if any error occurs
     */
    void insertPurgeStatus(PurgeState purgeState, Visibility visibility,
                           EzSecurityToken userToken) throws TException {

        EzElastic.Client documentClient = getDocumentClient();

        if (!typeCache.containsKey(SSRUtils.PURGE_TYPE_FIELD)) {
            // If it already exists and just isn't in the cache there is no harm
            logger.info("Setting up initial mapping for purge type ({})", SSRUtils.PURGE_TYPE_FIELD);
            documentClient.setTypeMapping("purge:type", getPurgeTypeMap(), security.fetchDerivedTokenForApp(userToken, securityId));
            typeCache.put(SSRUtils.PURGE_TYPE_FIELD, true);
        }

        String json = gson.toJson(purgeState);

        Document document = new Document();
        document.set_jsonObject(json);
        document.set_id(String.valueOf(purgeState.getPurgeId()));
        document.set_type(SSRUtils.PURGE_TYPE_FIELD);
        document.setVisibility(visibility);

        try {
            List<IndexResponse> result = Lists.newArrayList();
            IndexResponse response = documentClient.put(document, security.fetchDerivedTokenForApp(userToken, securityId));
            if (!response.isSuccess()) {
                logger.error("Put failed for purge status with id {}", response.get_id());
                result.add(response);
            }
        } catch (DocumentIndexingException e) {
            logger.error("Failed to index purge status", e);
            throw new TException("Error indexing purge status - document index exception", e);
        } finally {
            pool.returnToPool(documentClient);
        }
    }

    /**
     * Creates an elasticsearch mapping specifically for the purge state persistence,
     * distinct from the ssr mapping. Used to separate purge state data
     * from ssr search queries.
     *
     * @return purge type mapping string for elasticsearch.
     */
    private String getPurgeTypeMap() {
        return "{\n" +
                "    \"" + SSRUtils.PURGE_TYPE_FIELD + "\" : {\n" +
                "    \"_all\" : {\"enabled\" : false}, \n" +
                "        \"properties\" : {\n" +
                "            \"" + SSRUtils.PURGE_ID_FIELD + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"include_in_all\" : false,\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"no\"\n" +
                "            },\n" +
                "            \"" + SSRUtils.PURGE_STATE_FIELD + "\" : {\n" +
                "                \"type\" : \"string\",\n" +
                "                \"include_in_all\" : false,\n" +
                "                \"store\" : true,\n" +
                "                \"index\" : \"no\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
    }

    /**
     * Creates an elasticsearch mapping specifically for the main inbox for
     * saved percolator queries
     *
     * @return main inbox type mapping string for elasticsearch.
     */
    private String getMainInboxMapping() {
        return "{\n" +
                "    \"" + SSRUtils.PERCOLATOR_MAIN_INBOX_TYPE_FIELD + "\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"" + SSRUtils.PERCOLATOR_MAIN_INBOX_IDS + "\" : {\n" +
                "                \"type\" : \"string\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
    }

    // get the ssr_default type mapping
    protected static String getSSRDefaultTypeMap(String type) throws IOException {
        final XContentBuilder mappingBuilder =
                jsonBuilder().startObject()
                        .startObject(type)
                            .startObject("properties")
                                .startObject(SSRUtils.SSR_DATE_FIELD)
                                    .field("type", "date")
                                    .field("format", "ddHHmm'Z' MMM yy")
                                .endObject()
                                .startObject(SSRUtils.SSR_TIME_OF_INGEST)
                                    .field("type", "date")
                                    .field("format", SSR_DEFAULT_DATE_FORMAT)
                                .endObject()
                            .endObject()
                        .endObject();

        logger.info("ssr_default mapping = " + mappingBuilder.string());
        return mappingBuilder.string();
    }

    /**
     * Creates an elasticsearch mapping specifically for the main inbox for
     * saved percolator queries
     *
     * @return main inbox type mapping string for elasticsearch.
     */
    private String getIndividualPercolatorInboxMapping() throws IOException {
        final XContentBuilder mappingBuilder =
                jsonBuilder().startObject()
                        .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD)
                            .startObject("properties")
                                .startObject(SSRUtils.PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED)
                                    .field("type", "date")
                                    .field("format", SSR_DEFAULT_DATE_FORMAT)
                                .endObject()
                                .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_NAME)
                                    .field("type", "string")
                                .endObject()
                                .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_SEARCH_TEXT)
                                    .field("type", "string")
                                .endObject()
                                .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS)
                                    .startObject("properties")
                                        .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_ID)
                                            .field("type", "string")
                                        .endObject()
                                        .startObject(SSRUtils.PERCOLATOR_INDIVIDUAL_INBOX_DOC_TIMEOFINGEST)
                                            .field("type", "date")
                                            .field("format", SSR_DEFAULT_DATE_FORMAT)
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject();

        logger.info("percolator inbox mapping = " + mappingBuilder.string());

        return mappingBuilder.string();
    }

    /**
     * Validates that the app security from the userToken matches up the
     * EzCentralPurgeService security id.
     *
     * @param userToken The purge service's token
     * @throws TException thrown if the security Id does not match up.
     */
    private void validateEzCentralPurgeSecurityId(EzSecurityToken userToken) throws TException {
        security.validateReceivedToken(userToken);
        String purgeSecurityId = pool.getSecurityId(EzCentralPurgeConstants.SERVICE_NAME);

        EzSecurityTokenWrapper tokenWrapper = new EzSecurityTokenWrapper(userToken);
        String appSecurityId = tokenWrapper.getSecurityId();

        if ((purgeSecurityId != null) && (!purgeSecurityId.equals(appSecurityId))) {
            logger.error("CentralPurge security id mismatch! expected = " + purgeSecurityId + " actual = " + appSecurityId);
            throw new TException("The app security id does not match up with the"
                    + " EzCentralPurgeService security id");
        }
    }

    @Override
    public void shutdown() {
        pool.close();
        try {
            if (security != null) {
                security.close();
            }
        } catch (IOException e) {
            logger.warn("Could not properly close security client", e);
        }
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

    private void logError(Exception e, AuditEvent evt, String loggerMessage) {
        evt.failed();
        e.printStackTrace();
        evt.arg(e.getClass().getName(), e);
        logger.error(loggerMessage);
    }

    public static String getEncodedUserPrincipal(EzSecurityToken userToken) throws UnsupportedEncodingException {
        return URLEncoder.encode(userToken.getTokenPrincipal().getPrincipal().replaceAll("\\s", ""), "UTF-8");
    }

    private EzElastic.Client returnAndNullClient(EzElastic.Client client) {
        if (client != null)
            pool.returnToPool(client);
        return null;
    }

    /**
     * Given a token, will validate it, and then return a new token created for SSR
     * @param userToken the user's passed in token
     * @return A token created specifically for SSR
     * @throws EzSecurityTokenException if the token isn't valid
     */
    private EzSecurityToken validateAndFetchDerived(EzSecurityToken userToken) throws EzSecurityTokenException {
        security.validateReceivedToken(userToken);
        return security.fetchDerivedTokenForApp(userToken, securityId);
    }
}
