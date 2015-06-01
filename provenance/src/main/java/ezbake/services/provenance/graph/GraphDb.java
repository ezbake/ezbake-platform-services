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

package ezbake.services.provenance.graph;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.common.graph.TitanGraphConfiguration;
import ezbake.services.provenance.idgenerator.IdGeneratorException;
import ezbake.services.provenance.idgenerator.IdProvider;
import ezbake.services.provenance.graph.frames.*;
import ezbake.services.provenance.graph.frames.AgeOffRule;
import ezbake.services.provenance.idgenerator.ZookeeperIdProvider;
import ezbake.services.provenance.thrift.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static ezbake.services.provenance.idgenerator.IdProvider.ID_GENERATOR_TYPE;

public class GraphDb {
    private static Logger logger = LoggerFactory.getLogger(GraphDb.class);
    private static final String SEARCH_INDEX = "search";

    private final TitanGraph graph;
    private final FramedGraphFactory framedGraphFactory;
    private final IdProvider idGenerator;

    public GraphDb(final Properties properties) throws Exception {

        logger.info("Initializing graph db...");

        // use ezconfig settings to set storage index search properties
        String elasticClusterName = properties.getProperty(EzBakePropertyConstants.ELASTICSEARCH_CLUSTER_NAME);
        String elasticHostname = properties.getProperty(EzBakePropertyConstants.ELASTICSEARCH_HOST);
        if (StringUtils.isNotEmpty(elasticClusterName)) {
            // storage.index.search.cluster-name
            String clusterKey = joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.INDEX_NAMESPACE, SEARCH_INDEX, ElasticSearchIndex.CLUSTER_NAME_KEY);
            properties.put(clusterKey, elasticClusterName);
        }
        if (StringUtils.isNotEmpty(elasticHostname)) {
            // storage.index.search.hostname
            String hostKey = joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.INDEX_NAMESPACE, SEARCH_INDEX, GraphDatabaseConfiguration.HOSTNAME_KEY);
            properties.put(hostKey, elasticHostname);
        }

        TitanGraphConfiguration graphConfig = new TitanGraphConfiguration(properties);
        graphConfig.setTitanAccumuloProperties();

        Iterator<String> iter = graphConfig.getKeys();
        while (iter.hasNext()) {
            String key = iter.next();
            logger.debug("graphconfig: " + key + "=" + graphConfig.getString(key));
        }

        this.graph = TitanFactory.open(graphConfig);
        framedGraphFactory = new FramedGraphFactory(new JavaHandlerModule());
        this.idGenerator = new ZookeeperIdProvider(properties);

        defineSchema();

        /* This addresses an error we ran into when running ezcentos.
         * The id_generators would start returning ids that already exist
         * after shutting down ezcentos. This sets the current value of
         * the id generator to the highest current id for each type.
        */
        for (ID_GENERATOR_TYPE type : ID_GENERATOR_TYPE.values()) {
            long id = getHighestVertexId(type);
            long currentId = this.idGenerator.getCurrentValue(type);
            if (id > currentId) {
                this.idGenerator.setCurrentValue(type, id);
                logger.info(String.format("set current %s id = %d", type, id));
            } else {
                logger.info(String.format("current %s highest vertex = %d, id = %d", type, id, currentId));
            }
        }
    }

    public static String getElasticIndexName(final Properties properties) {
        String indexNameKey = joinProperties(GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.INDEX_NAMESPACE, SEARCH_INDEX, ElasticSearchIndex.INDEX_NAME_KEY);
        return properties.getProperty(indexNameKey, "provenance");
    }

    private static String joinProperties(String... properties) {
        return Joiner.on(".").skipNulls().join(properties);
    }

    private FramedGraph<TitanGraph> getFramedGraph() {
        return this.framedGraphFactory.create(this.graph);
    }

    // Returns the highest id value of a certain type
    private long getHighestVertexId(ID_GENERATOR_TYPE type) {
        long id = 0;
        String idField;
        switch (type) {
            case DocumentType:
                idField = Document.DocumentIdEs;
                break;
            case AgeOffRule:
                idField = AgeOffRule.RuleIdEs;
                break;
            case PurgeEvent:
                idField = PurgeEvent.PurgeIdEs;
                break;
            default:
                logger.error("Invalid type to get highest id");
                return 0;
        }

        try {
            Iterator<Vertex> lastVertex = graph.query()
                    .has(idField, Cmp.GREATER_THAN, 0L)
                    .orderBy(idField, Order.DESC)
                    .limit(1).vertices().iterator();
            if (lastVertex.hasNext()) {
                id = lastVertex.next().getProperty(idField);
            } else {
                logger.info("no vertex found for type " + type);
            }

            // AgeOffEvent share the same id with PurgeEvent. use the bigger one
            if (type == ID_GENERATOR_TYPE.PurgeEvent) {
                FramedGraph<TitanGraph> framedGraph = getFramedGraph();
                Iterator<AgeOffEvent> it = framedGraph.query().has(BaseVertex.Type, AgeOffEvent.TYPE).limit(1).vertices(AgeOffEvent.class).iterator();
                if (it.hasNext()) {
                    long eventId = it.next().getEventMaxId();
                    logger.info("AgeOffEvent max id = " + eventId);
                    id = eventId > id ? eventId : id;
                }
            }
            this.graph.commit();
        } catch (TitanException ex) {
            this.graph.rollback();
            logger.error(String.format("failed to get highest vertex %s id", type), ex);
            throw ex;
        }
        return id;
    }

    private synchronized void defineSchema() {
        logger.info("setup graph db schema...");

        if (this.graph == null) {
            return;
        }

        // setup kyes/indexes
        // vertex Type
        if (graph.getType(BaseVertex.Type) == null) {
            graph.makeKey(BaseVertex.Type).dataType(String.class).indexed(Vertex.class).make();
        }
        // vertex Application
        if (graph.getType(BaseVertex.Application) == null) {
            graph.makeKey(BaseVertex.Application).dataType(String.class).make();
        }
        // vertex User
        if (graph.getType(BaseVertex.User) == null) {
            graph.makeKey(BaseVertex.User).dataType(String.class).make();
        }
        // vertex TimeStamp
        if (graph.getType(BaseVertex.TimeStamp) == null) {
            graph.makeKey(BaseVertex.TimeStamp).dataType(Date.class).make();
        }
        // vertex Document URI
        if (graph.getType(Document.URI) == null) {
            graph.makeKey(Document.URI).dataType(String.class).indexed(Vertex.class).make();
        }
        // vertex Document DocumentId
        boolean documentIdExist = false;
        if (graph.getType(Document.DocumentId) == null) {
            graph.makeKey(Document.DocumentId).dataType(Long.class).indexed(Vertex.class).make();
        } else {
            documentIdExist = true;
        }
        // // vertex Document DocumentIdEs -- for elastic search index to speed up orderby query
        if (graph.getType(Document.DocumentIdEs) == null) {
            graph.makeKey(Document.DocumentIdEs).dataType(Long.class).indexed(SEARCH_INDEX, Vertex.class).make();
            // for upgrade
            if (documentIdExist) {
                importSearchIndex(Document.TYPE, Document.DocumentIdEs, Document.DocumentId);
            }
        }
        // vertex Document Aged
        if (graph.getType(Document.Aged) == null) {
            graph.makeKey(Document.Aged).dataType(Boolean.class).make();
        }
        // vertex Document InheritanceInfoList
        if (graph.getType(Document.InheritanceInfoList) == null) {
            graph.makeKey(Document.InheritanceInfoList).dataType(InheritanceInfo[].class).make();
        }
        // vertex AgeOffRule RuleId
        boolean ruleIdExist = false;
        if (graph.getType(AgeOffRule.RuleId) == null) {
            graph.makeKey(AgeOffRule.RuleId).dataType(Long.class).indexed(Vertex.class).unique().make();
        } else {
            ruleIdExist = true;
        }
        // vertex AgeOffRule RuleIdEs -- for elastic search index to speed up orderby query
        if (graph.getType(AgeOffRule.RuleIdEs) == null) {
            graph.makeKey(AgeOffRule.RuleIdEs).dataType(Long.class).indexed(SEARCH_INDEX, Vertex.class).make();
            // for upgrade
            if (ruleIdExist) {
                importSearchIndex(AgeOffRule.TYPE, AgeOffRule.RuleIdEs, AgeOffRule.RuleId);
            }
        }
        // vertex AgeOffRule Name
        if (graph.getType(AgeOffRule.Name) == null) {
            graph.makeKey(AgeOffRule.Name).dataType(String.class).indexed(Vertex.class).unique().make();
        }
        // vertex AgeOffRule Duration
        if (graph.getType(AgeOffRule.Duration) == null) {
            graph.makeKey(AgeOffRule.Duration).dataType(Long.class).make();
        }
        // vertex AgeOffRule MaximumExecutionPeriod
        if (graph.getType(AgeOffRule.MaximumExecutionPeriod) == null) {
            graph.makeKey(AgeOffRule.MaximumExecutionPeriod).dataType(Integer.class).make();
        }

        // vertex PurgeEvent PurgeId
        boolean purgeIdExist = false;
        if (graph.getType(PurgeEvent.PurgeId) == null) {
            graph.makeKey(PurgeEvent.PurgeId).dataType(Long.class).indexed(Vertex.class).unique().make();
        } else {
            purgeIdExist = true;
        }
        // vertex PurgeEvent PurgeIdEs -- for elastic search index to speed up orderby query
        if (graph.getType(PurgeEvent.PurgeIdEs) == null) {
            graph.makeKey(PurgeEvent.PurgeIdEs).dataType(Long.class).indexed(SEARCH_INDEX, Vertex.class).make();
            // for upgrade
            if (purgeIdExist) {
                importSearchIndex(PurgeEvent.TYPE, PurgeEvent.PurgeIdEs, PurgeEvent.PurgeId);
            }
        }

        // vertex PurgeEvent Name
        if (graph.getType(PurgeEvent.Name) == null) {
            graph.makeKey(PurgeEvent.Name).dataType(String.class).make();
        }
        // vertex PurgeEvent Description
        if (graph.getType(PurgeEvent.Description) == null) {
            graph.makeKey(PurgeEvent.Description).dataType(String.class).make();
        }
        // vertex PurgeEvent DocumentUris
        if (graph.getType(PurgeEvent.DocumentUris) == null) {
            graph.makeKey(PurgeEvent.DocumentUris).dataType(String[].class).make();
        }
        // vertex PurgeEvent DocumentUrisNotFound
        if (graph.getType(PurgeEvent.DocumentUrisNotFound) == null) {
            graph.makeKey(PurgeEvent.DocumentUrisNotFound).dataType(String[].class).make();
        }
        // vertex PurgeEvent PurgeDocumentIds
        if (graph.getType(PurgeEvent.PurgeDocumentIds) == null) {
            graph.makeKey(PurgeEvent.PurgeDocumentIds).dataType(Long[].class).make();
        }
        // vertex PurgeEvent CompletelyPurgedDocumentIds
        if (graph.getType(PurgeEvent.CompletelyPurgedDocumentIds) == null) {
            graph.makeKey(PurgeEvent.CompletelyPurgedDocumentIds).dataType(Long[].class).make();
        }
        // vertex PurgeEvent Resolved
        if (graph.getType(PurgeEvent.Resolved) == null) {
            graph.makeKey(PurgeEvent.Resolved).dataType(Boolean.class).make();
        }

        // vertex AgeOffEvent EventMaxId
        if (graph.getType(AgeOffEvent.EventMaxId) == null) {
            graph.makeKey(AgeOffEvent.EventMaxId).dataType(Long.class).make();
        }

        // edge AgeOff
        if (graph.getType(AgeOff.LABEL) == null) {
            graph.makeLabel(AgeOff.LABEL).make();
        }
        // edge AgeOff Rule(id)
        if (graph.getType(AgeOff.Rule) == null) {
            graph.makeKey(AgeOff.Rule).dataType(Long.class).indexed(Edge.class).make();
        }
        // edge AgeOff AgeOffRelevantDateTime
        if (graph.getType(AgeOff.AgeOffRelevantDateTime) == null) {
            graph.makeKey(AgeOff.AgeOffRelevantDateTime).dataType(Date.class).make();
        }

        // edge DerivedFrom
        if (graph.getType(DerivedFrom.LABEL) == null) {
            graph.makeLabel(DerivedFrom.LABEL).make();
        }
        graph.commit();
    }

    private void importSearchIndex(String type, String esKey, String idKey) {
        logger.info("importing search index for " + type);
        int limit = 100;
        GremlinPipeline pipe = new GremlinPipeline();
        pipe.start(this.graph.query().has(BaseVertex.Type, type).vertices());

        do {
            Iterator<Vertex> it = pipe.next(limit).iterator();
            while (it.hasNext()) {
                Vertex vertex = it.next();
                vertex.setProperty(esKey, vertex.getProperty(idKey));
            }
            this.graph.commit();
        } while (pipe.hasNext());
    }

    public void shutdown() {
        logger.info("shutdown graph db");
        this.graph.shutdown();
        logger.info("shutdown idGenerator");
        this.idGenerator.shutdown();
    }

    private void validateGraphDb() throws TException {
        if (this.graph == null || this.framedGraphFactory == null) {
            throw new TException("graph db not connected");
        }
    }

    // add AgeOffRule Vertex
    public long addAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, String name, long duration, int maxPeriod) throws ProvenanceAgeOffRuleNameExistsException, org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // verify the name is unique
            Iterator<AgeOffRule> it = framedGraph.query().has(AgeOffRule.Name, name).vertices(AgeOffRule.class).iterator();
            if (it.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNameExistsException("This name of the age off rule already exists: " + name);
            }

            long id = idGenerator.getNextId(ID_GENERATOR_TYPE.AgeOffRule);
            AgeOffRule ageOffRule = framedGraph.addVertex(null, AgeOffRule.class);
            ageOffRule.updateProperties(securityToken, name, id, duration, maxPeriod);

            this.graph.commit();

            return id;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        } catch (IdGeneratorException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get AgeOffRule vertex by name
    public ezbake.services.provenance.thrift.AgeOffRule getAgeOffRule(String name) throws ProvenanceAgeOffRuleNotFoundException, TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Iterator<AgeOffRule> it = framedGraph.query().has(AgeOffRule.Name, name).vertices(AgeOffRule.class).iterator();
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("Age off rule not found with name: " + name);
            }

            AgeOffRule rule = it.next();
            ezbake.base.thrift.DateTime dateTime = Utils.convertDate2DateTime(rule.getTimeStamp());

            this.graph.commit();

            return new ezbake.services.provenance.thrift.AgeOffRule(rule.getName(), rule.getRuleId(), rule.getDuration(), rule.getMaximumExecutionPeriod(), rule.getApplication(), rule.getUser(), dateTime);
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get AgeOffRule Vertex by ruleId
    public ezbake.services.provenance.thrift.AgeOffRule getAgeOffRule(long ruleId) throws ProvenanceAgeOffRuleNotFoundException, TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Iterator<AgeOffRule> it = framedGraph.query().has(AgeOffRule.RuleId, ruleId).vertices(AgeOffRule.class).iterator();
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("Age off rule not found with id: " + ruleId);
            }

            AgeOffRule rule = it.next();
            ezbake.base.thrift.DateTime dateTime = Utils.convertDate2DateTime(rule.getTimeStamp());

            this.graph.commit();

            return new ezbake.services.provenance.thrift.AgeOffRule(rule.getName(), rule.getRuleId(), rule.getDuration(), rule.getMaximumExecutionPeriod(), rule.getApplication(), rule.getUser(), dateTime);
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get AgeOffRule vertices with page parameter page + limit
    public List<ezbake.services.provenance.thrift.AgeOffRule> getAllAgeOffRules(int limit, int page) throws TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();
            List<ezbake.services.provenance.thrift.AgeOffRule> results = new ArrayList<ezbake.services.provenance.thrift.AgeOffRule>();

            GremlinPipeline pipe = new GremlinPipeline();
            pipe.start(this.graph.query().has(BaseVertex.Type, AgeOffRule.TYPE).orderBy(AgeOffRule.Name, Order.ASC).vertices());
            if (limit > 0 && page > 0) {
                pipe.range(limit * (page - 1), limit * page - 1);
            }
            Iterator<Vertex> it = pipe.iterator();
            while (it.hasNext()) {
                AgeOffRule rule = framedGraph.frame(it.next(), AgeOffRule.class);
                ezbake.base.thrift.DateTime dateTime = Utils.convertDate2DateTime(rule.getTimeStamp());
                results.add(new ezbake.services.provenance.thrift.AgeOffRule(rule.getName(), rule.getRuleId(), rule.getDuration(), rule.getMaximumExecutionPeriod(), rule.getApplication(), rule.getUser(), dateTime));
            }

            this.graph.commit();

            return results;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get the count of AgeOffRule vertex
    public int countAgeOffRules() throws TException {
        validateGraphDb();

        try {
            GremlinPipeline pipe = new GremlinPipeline();
            int count = (int) pipe.start(this.graph.query().has(BaseVertex.Type, AgeOffRule.TYPE).vertices()).count();

            this.graph.commit();

            return count;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // update AgeOffRule Vertex
    public void updateAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, String name, long duration, String adminApp) throws
            ProvenanceAgeOffRuleNotFoundException, EzSecurityTokenException, TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Iterator<AgeOffRule> it = framedGraph.query().has(AgeOffRule.Name, name).vertices(AgeOffRule.class).iterator();
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("Age off rule not found with name: " + name);
            }

            ezbake.services.provenance.graph.frames.AgeOffRule ageOffRule = it.next();

            if (!Utils.isAuthenticatedToUpdate(securityToken, ageOffRule.getApplication(), adminApp)) {
                this.graph.rollback();
                throw new EzSecurityTokenException("Not authorized to update age off rule");
            }
            ageOffRule.updateProperties(securityToken, name, ageOffRule.getRuleId(), duration, ageOffRule.getMaximumExecutionPeriod());

            this.graph.commit();

        } catch (TitanException ex) {
            logger.error("updateAgeOffRule TitanException: ", ex);
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // add Document Vertex
    public long addDocument(ezbake.base.thrift.EzSecurityToken securityToken, String uri, List<InheritanceInfo> parents, List<AgeOffMapping> ageOffRules) throws
            ProvenanceDocumentExistsException, ProvenanceAgeOffRuleNotFoundException, ProvenanceParentDocumentNotFoundException, ProvenanceCircularInheritanceNotAllowedException, org.apache.thrift.TException {
        validateGraphDb();

        try {
            long id = idGenerator.getNextId(ID_GENERATOR_TYPE.DocumentType);
            AddDocumentHelper helper = new AddDocumentHelper(securityToken, uri);

            try {
                Document doc = helper.createDocumentVertex(this.graph, getFramedGraph(), id);
                helper.addDocumentInheritance(getFramedGraph(), doc, parents);
                helper.addAgeOffRules(getFramedGraph(), doc, ageOffRules, false);
            } catch (ProvenanceDocumentExistsException ex) {
                this.graph.rollback();
                throw ex;
            } catch (ProvenanceCircularInheritanceNotAllowedException ex) {
                this.graph.rollback();
                throw ex;
            } catch (ProvenanceParentDocumentNotFoundException ex) {
                this.graph.rollback();
                throw ex;
            } catch (ProvenanceAgeOffRuleNotFoundException ex) {
                this.graph.rollback();
                throw ex;
            }

            this.graph.commit();
            return id;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        } catch (IdGeneratorException ex) {
            throw new TException(ex);
        }
    }

    // add Documents in bulk
    public Map<String, AddDocumentResult> addDocuments(ezbake.base.thrift.EzSecurityToken securityToken, Set<AddDocumentEntry> documents, Set<AgeOffMapping> ageOffRulesSet) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceAgeOffRuleNotFoundException, org.apache.thrift.TException {
        validateGraphDb();

        StopWatch watch = new StopWatch();
        watch.start();

        Map<String, AddDocumentResult> results = new HashMap<>();
        AddDocumentHelper helper = new AddDocumentHelper(securityToken, "BULK");
        List<AgeOffMapping> ageOffRules = ageOffRulesSet == null ? null : new ArrayList<>(ageOffRulesSet);
        Map<AgeOffMapping, AgeOffRule> rules = null;

        try {
            rules = helper.addAgeOffRules(getFramedGraph(), null, ageOffRules, true);
        } catch (ProvenanceAgeOffRuleNotFoundException ex) {
            this.graph.rollback();
            throw ex;
        }

        this.graph.commit();

        // uri to its entry. build a map for easy retrieval
        Map<String, AddDocumentEntry> documentsMap = new HashMap<>();
        for (AddDocumentEntry document : documents) {
            String uri = document.getUri();
            if (documentsMap.containsKey(uri)) {
                documentsMap.get(uri).getParents().addAll(document.getParents());
            } else {
                documentsMap.put(document.getUri(), document);
            }
        }

        List<String> validUris = helper.getValidDocumentEntriesInOrder(documentsMap);

        try {
            long nextNId = this.idGenerator.getNextNId(ID_GENERATOR_TYPE.DocumentType, validUris.size());
            long currentId = nextNId - validUris.size() + 1;
            for (String uri : validUris) {
                AddDocumentEntry entry = documentsMap.get(uri);
                AddDocumentResult result = addDocument(securityToken, uri, currentId++, entry.getParents(), rules);
                results.put(uri, result);
//                logger.info(String.format("adding bulk document %s result: %s", entry.getUri(), result.getStatus()));
                documentsMap.remove(uri);
            }

            // all remaining uris were involved in circular inheritance
            for (AddDocumentEntry entry : documentsMap.values()) {
                AddDocumentResult result = new AddDocumentResult(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED);
                results.put(entry.getUri(), result);
//                logger.info(String.format("adding bulk document %s result: %s", entry.getUri(), AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED));
            }

            documentsMap.clear();

            try {
                this.graph.commit();
            } catch (TitanException ex) {
                this.graph.rollback();

                for (AddDocumentResult result : results.values()) {
                    if (result.status == AddDocumentStatus.SUCCESS) {
                        result.setStatus(AddDocumentStatus.UNKNOWN_ERROR);
                    }
                }
                logger.error("addDocuments exception: ", ex);
            }
            watch.stop();
            logger.info(String.format(" *** add %d documents took %d ms", validUris.size(), watch.getNanoTime() / 1000000));

        } catch (IdGeneratorException ex) {
            throw new TException(ex);
        }
        return results;
    }

    // add document from bulk
    private AddDocumentResult addDocument(ezbake.base.thrift.EzSecurityToken securityToken, String uri, long id, Set<InheritanceInfo> parentsSet, Map<AgeOffMapping, AgeOffRule> rules)
            throws TException {
        AddDocumentResult result = new AddDocumentResult(AddDocumentStatus.SUCCESS);

        AddDocumentHelper helper = new AddDocumentHelper(securityToken, uri);

        // create the Document vertex
        Document doc = null;

        try {
            doc = helper.createDocumentVertex(this.graph, getFramedGraph(), id);
        } catch (ProvenanceDocumentExistsException ex) {
            result.setStatus(AddDocumentStatus.ALREADY_EXISTS);
            return result;
        }

        try {
            List<InheritanceInfo> parents = parentsSet == null ? null : new ArrayList<>(parentsSet);
            helper.addDocumentInheritance(getFramedGraph(), doc, parents);
        } catch (ProvenanceParentDocumentNotFoundException ex) {
            result.setStatus(AddDocumentStatus.PARENT_NOT_FOUND);
            result.setParentsNotFound(ex.getParentUris());
            return result;
        } catch (ProvenanceCircularInheritanceNotAllowedException ex) {
            result.setStatus(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED);
            return result;
        }

        for (Map.Entry<AgeOffMapping, AgeOffRule> entry : rules.entrySet()) {
            helper.addAgeOffEdge(getFramedGraph(), entry.getValue(), doc, entry.getKey());
        }

        result.setDocumentId(id);

        return result;
    }

    // get the DocumentIds that will be aged against the ruleId and effectiveTime
    public AgeOffInitiationResult ageOff(ezbake.base.thrift.EzSecurityToken securityToken, long ruleId, ezbake.base.thrift.DateTime effectiveTime) throws
            ProvenanceAgeOffRuleNotFoundException, org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Set<Long> docIds = new HashSet<Long>();

            // find AgeOffRule vertex
            Iterator<AgeOffRule> ruleIt = framedGraph.query().has(AgeOffRule.RuleId, ruleId).vertices(AgeOffRule.class).iterator();
            if (!ruleIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("Age off rule not found with id: " + ruleId);
            }

            long duration = ruleIt.next().getDuration();
            long effectTime;
            if (effectiveTime != null) {
                effectTime = Utils.convertDateTime2Date(effectiveTime).getTime();
            } else {
                effectTime = Utils.getCurrentDate().getTime();
            }

            Date compare = new Date(effectTime - duration);
            int limit = 100;               // number of records to retrieve per page
            GremlinPipeline pipe = new GremlinPipeline();
            // query all Document vertex that has incoming AgeOff edge with ruleId and AgeOffRelevantDateTime less than (effectiveTime - duration)
            pipe.start(this.graph.query().has(AgeOff.Rule, ruleId).edges()).has(AgeOff.AgeOffRelevantDateTime, Tokens.T.lt, compare).inV().has(BaseVertex.Type, Document.TYPE);

            do {
                Iterator<Vertex> it = pipe.next(limit).iterator();
                while (it.hasNext()) {
                    Document doc = framedGraph.frame(it.next(), Document.class);
                    docIds.add(doc.getDocumentId());
                }
            } while (pipe.hasNext());

            this.graph.commit();

            long eventId = this.idGenerator.getNextId(ID_GENERATOR_TYPE.PurgeEvent);

            try {
                // record the AgeOffEvent.EventMaxId, so when service restarts, we won't lose information and generate duplicate id.
                Iterator<AgeOffEvent> it = framedGraph.query().has(BaseVertex.Type, AgeOffEvent.TYPE).limit(1).vertices(AgeOffEvent.class).iterator();
                if (it.hasNext()) {
                    AgeOffEvent event = it.next();
                    if (eventId > event.getEventMaxId()) {
                        event.setEventMaxId(eventId);
                    }
                } else {
                    AgeOffEvent event = framedGraph.addVertex(null, AgeOffEvent.class);
                    event.setType(AgeOffEvent.TYPE);
                    event.setEventMaxId(eventId);
                }
                this.graph.commit();
            } catch (TitanException ex) {
                this.graph.rollback();
                logger.error("Set AgeOffEvent EventMaxId exception: ", ex);
            }

            return new AgeOffInitiationResult(eventId, docIds);

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        } catch (IdGeneratorException ex) {
            throw new TException(ex);
        }
    }

    // mark document vertex as aged
    public void markAsAged(List<Long> agedDocumentIds) throws
            ProvenanceDocumentNotFoundException, org.apache.thrift.TException {
        validateGraphDb();

        try {
            if (agedDocumentIds == null) {
                return;
            }
            if (agedDocumentIds.size() == 0) {
                return;
            }
            Collections.sort(agedDocumentIds);


            // compare the biggest doc Id with the id generator value
            long maxDocId = this.idGenerator.getCurrentValue(ID_GENERATOR_TYPE.DocumentType);
            if (agedDocumentIds.get(agedDocumentIds.size() - 1) > maxDocId) {
                throw new ProvenanceDocumentNotFoundException("Document Id too big");
            }

            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            for (Long docId : agedDocumentIds) {
                Iterator<Document> it = framedGraph.query().has(Document.DocumentId, docId).vertices(Document.class).iterator();
                if (!it.hasNext()) {
                    logger.warn(String.format("Document %d does not exist when markAsAged. Ignore this document.", docId));
                    continue;
                }
                // set aged property
                Document doc = it.next();
                doc.setAged(true);

                // remove all incoming AgeOff edges
                Iterator<AgeOff> edgeIt = doc.getInAgeOffEdges();
                while (edgeIt.hasNext()) {
                    this.graph.removeEdge(edgeIt.next().asEdge());
                }
            }

            this.graph.commit();

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        } catch (IdGeneratorException ex) {
            throw new TException(ex);
        }
    }

    // get Document Vertex properties. When id is supplied, use it, otherwirse, use uri
    public DocumentInfo getDocumentInfo(long id, String uri) throws
            ProvenanceDocumentNotFoundException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Iterator<Document> it;
            if (id > 0) {
                it = framedGraph.query().has(Document.DocumentId, id).vertices(Document.class).iterator();
            } else {
                it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
            }
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("The document cannot be found");
            }
            Document doc = it.next();

            List<Map<Long, String>> parents = new ArrayList<Map<Long, String>>();
            List<Map<Long, String>> children = new ArrayList<Map<Long, String>>();
            List<DocumentAgeOffInfo> ageOffInfo = new ArrayList<DocumentAgeOffInfo>();

            // get all parents  -- derivedFrom edge in vertex
            Iterator<Document> parentIt = doc.getParentDocuments();
            while (parentIt.hasNext()) {
                Document parent = parentIt.next();
                Map<Long, String> p = new HashMap<Long, String>();
                p.put(parent.getDocumentId(), parent.getUri());
                parents.add(p);
            }

            // get all children -- derivedFrom edge out vertex
            Iterator<Document> childIt = doc.getChildDocuments();
            while (childIt.hasNext()) {
                Document child = childIt.next();
                Map<Long, String> c = new HashMap<Long, String>();
                c.put(child.getDocumentId(), child.getUri());
                children.add(c);
            }

            // get all ageOffInfo -- get AgeOff edge in first
            Iterator<AgeOff> ageOffIt = doc.getInAgeOffEdges();
            while (ageOffIt.hasNext()) {
                AgeOff ageOff = ageOffIt.next();
                // get vertex out, either AgeOff or Document
                Vertex ageOffOut = ageOff.asEdge().getVertex(Direction.OUT);

                boolean inherited = false;
                long inheritedFromId = 0;
                String inheritedFromUri = "";
                int maximumExecutionTime = 0;
                // vertex is Document, set inherited properties
                if (ageOffOut.getProperty(BaseVertex.Type).equals(Document.TYPE)) {
                    Document document = framedGraph.frame(ageOffOut, Document.class);
                    inherited = true;
                    inheritedFromId = document.getDocumentId();
                    inheritedFromUri = document.getUri();

                    // query the AgeOffRule to get MaximumExecutionPeriod
                    Iterator<AgeOffRule> ruleIt = framedGraph.query().has(AgeOffRule.RuleId, ageOff.getRuleId()).vertices(AgeOffRule.class).iterator();
                    if (ruleIt.hasNext()) {
                        maximumExecutionTime = ruleIt.next().getMaximumExecutionPeriod();
                    }
                } else if (ageOffOut.getProperty(BaseVertex.Type).equals(AgeOffRule.TYPE)) {
                    AgeOffRule rule = framedGraph.frame(ageOffOut, AgeOffRule.class);
                    maximumExecutionTime = rule.getMaximumExecutionPeriod();
                }
                DocumentAgeOffInfo info = new DocumentAgeOffInfo(ageOff.getRuleId(), Utils.convertDate2DateTime(ageOff.getAgeOffRelevantDateTime()), maximumExecutionTime,
                        Utils.convertDate2DateTime(ageOff.getTimeStamp()), ageOff.getApplication(), ageOff.getUser(), inherited);
                if (inherited) {
                    info.setInheritedFromId(inheritedFromId);
                    info.setInheritedFromUri(inheritedFromUri);
                }
                ageOffInfo.add(info);
            }

            this.graph.commit();

            return new DocumentInfo(doc.getUri(), doc.getDocumentId(), doc.getApplication(), Utils.convertDate2DateTime(doc.getTimeStamp()), doc.getUser(),
                    parents, children, ageOffInfo, doc.getAged());

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // helper function to get ancestor documentId
    private Set<Long> getAncestors(Document document) {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        Set<Long> documentIds = new HashSet<Long>();
        // add self first
        documentIds.add(document.getDocumentId());
        // loop through all ancestores
        Iterator<Vertex> iter = new GremlinPipeline<Vertex, Vertex>(document.asVertex())
                .as("documents")
                .inE(DerivedFrom.LABEL)
                .outV()
                .loop("documents", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return true;
                            }
                        }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return true;
                            }
                        }
                )
                .dedup();

        while (iter.hasNext()) {
            Document doc = framedGraph.frame(iter.next(), Document.class);
            documentIds.add(doc.getDocumentId());
        }

        return documentIds;
    }

    // get all Document ancestors fo the given uris
    public DerivedResult getAncestors(List<String> uris) throws
            org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Set<Long> documentIds = new HashSet<Long>();
            DerivedResult result = new DerivedResult();
            result.setUrisNotFound(new ArrayList<String>());

            for (String uri : uris) {
                Iterator<Document> it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
                if (it.hasNext()) {
                    documentIds.addAll(getAncestors(it.next()));
                } else {
                    result.addToUrisNotFound(uri);
                }
            }
            result.setDerivedDocs(documentIds);

            // check if to set immediateChildren
            if (uris.size() == 1) {
                setImmediateChildren(result, uris.get(0));
            }

            this.graph.commit();

            return result;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // help to set the DerivedResult.ImmediateChildren
    private void setImmediateChildren(DerivedResult result, String uri) {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        result.setImmediateChildren(new ArrayList<Long>());
        Iterator<Document> it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
        if (it.hasNext()) {
            Document document = it.next();
            Iterator<Document> children = document.getChildDocuments();
            while (children.hasNext()) {
                result.addToImmediateChildren(children.next().getDocumentId());
            }
        }
    }

    // helper function to get all descendant documentId
    private Set<Long> getDescendants(Document document) {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        Set<Long> documentIds = new HashSet<Long>();

        // add self first
        documentIds.add(document.getDocumentId());

        // loop through all descendants
        Iterator<Vertex> iter = new GremlinPipeline<Vertex, Vertex>(document.asVertex())
                .as("documents")
                .outE(DerivedFrom.LABEL)
                .inV()
                .loop("documents", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return true;
                            }
                        }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                            @Override
                            public Boolean compute(LoopPipe.LoopBundle<Vertex> vertexLoopBundle) {
                                return true;
                            }
                        }
                )
                .dedup();

        while (iter.hasNext()) {
            Document doc = framedGraph.frame(iter.next(), Document.class);
            documentIds.add(doc.getDocumentId());
        }

        return documentIds;
    }

    // get all descendant documentIds of the given uris
    public DerivedResult getDescendants(List<String> uris) throws
            org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Set<Long> documentIds = new HashSet<Long>();
            DerivedResult result = new DerivedResult();
            result.setUrisNotFound(new ArrayList<String>());

            for (String uri : uris) {
                Iterator<Document> it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
                if (it.hasNext()) {
                    documentIds.addAll(getDescendants(it.next()));
                } else {
                    result.addToUrisNotFound(uri);
                }
            }

            result.setDerivedDocs(documentIds);

            // check if to set immediateChildren
            if (uris.size() == 1) {
                setImmediateChildren(result, uris.get(0));
            }

            this.graph.commit();

            return result;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // prepare a Document for later purge
    public PurgeInitiationResult markForPurge(ezbake.base.thrift.EzSecurityToken securityToken, List<String> uris, String name, String description) throws
            org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            Set<Long> documentIds = new HashSet<Long>();
            List<String> docNotFound = new ArrayList<String>();

            // get all descendants
            for (String uri : uris) {
                Iterator<Document> it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
                if (it.hasNext()) {
                    documentIds.addAll(getDescendants(it.next()));
                } else {
                    docNotFound.add(uri);
                }
            }

            // create PurgeEvent vertex
            long id = idGenerator.getNextId(ID_GENERATOR_TYPE.PurgeEvent);
            PurgeEvent purgeEvent = framedGraph.addVertex(null, PurgeEvent.class);
            purgeEvent.updateProperties(securityToken, id, name, description, uris, docNotFound, documentIds, new HashSet<Long>(), false);

            this.graph.commit();

            return new PurgeInitiationResult(documentIds, docNotFound, id);

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        } catch (IdGeneratorException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // map DocumentId in list to URI
    public PositionsToUris getUriFromId(List<Long> positionsList) throws
            org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            PositionsToUris result = new PositionsToUris();
            result.setMapping(new HashMap<Long, String>());
            result.setUnfoundPositionList(new ArrayList<Long>());

            if (positionsList != null) {
                // retrieve the uri maps to id
                for (long docId : positionsList) {
                    Iterator<Document> it = framedGraph.query().has(Document.DocumentId, docId).vertices(Document.class).iterator();
                    if (it.hasNext()) {
                        result.getMapping().put(docId, it.next().getUri());
                    } else {
                        result.addToUnfoundPositionList(docId);
                    }
                }
            }

            this.graph.commit();

            return result;

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // convert ids to ConvertedUris
    public ConversionResult getConvertedUrisFromIds(Set<Long> ids) throws
            org.apache.thrift.TException {

        validateGraphDb();

        try {
            ConversionResult result = new ConversionResult();
            result.setConvertedUris(new ArrayList<Long>());

            // check if vertex exists
            for (long docId : ids) {
                Iterator<Vertex> it = this.graph.query().has(Document.DocumentId, docId).vertices().iterator();
                if (it.hasNext()) {
                    result.addToConvertedUris(docId);
                } else {
                    result.addToIdsNotFound(docId);
                }
            }

            this.graph.commit();

            return result;

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // convert uris to ConvertedUris
    public ConversionResult getConvertedUrisFromUris(Set<String> uris) throws
            org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            ConversionResult result = new ConversionResult();
            result.setConvertedUris(new ArrayList<Long>());

            // check if vertex exists
            for (String uri : uris) {
                Iterator<Document> it = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator();
                if (it.hasNext()) {
                    result.addToConvertedUris(it.next().getDocumentId());
                } else {
                    result.addToUrisNotFound(uri);
                }
            }

            this.graph.commit();

            return result;

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get PurgeEvent vertex matching purgeId
    public PurgeInfo getPurgeInfo(long purgeId) throws
            ProvenancePurgeIdNotFoundException, org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // check if vertex exists
            Iterator<PurgeEvent> it = framedGraph.query().has(PurgeEvent.PurgeId, purgeId).vertices(PurgeEvent.class).iterator();
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenancePurgeIdNotFoundException(String.format("Purge Event with id %d not found", purgeId));
            }
            PurgeEvent event = it.next();

            List<String> uris = event.getDocumentUris();
            if (uris == null) {
                uris = new ArrayList<>();
            }
            List<String> urisNotFound = event.getDocumentUrisNotFound();
            if (urisNotFound == null) {
                urisNotFound = new ArrayList<>();
            }
            String user = event.getUser();
            if (user == null) {
                user = "NOT SET";
            }
            PurgeInfo result = new PurgeInfo(event.getPurgeId(), Utils.convertDate2DateTime(event.getTimeStamp()), uris, urisNotFound,
                    event.getPurgeDocumentIds(), event.getCompletelyPurgedDocumentIds(), user, event.getResolved());

            result.setName(event.getName());
            result.setDescription(event.getDescription());

            this.graph.commit();

            return result;
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // get PurgeId of all PurgeEvent vertex
    public List<Long> getAllPurgeIds() throws org.apache.thrift.TException {
        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            List<Long> ids = new ArrayList<Long>();

            Iterator<PurgeEvent> it = framedGraph.query().has(BaseVertex.Type, PurgeEvent.TYPE).vertices(PurgeEvent.class).iterator();
            while (it.hasNext()) {
                ids.add(it.next().getPurgeId());
            }

            this.graph.commit();

            return ids;

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // update PurgeEvent vertex
    public void updatePurge(ezbake.base.thrift.EzSecurityToken securityToken, long purgeId, Set<Long> completelyPurged, String note, boolean resolved) throws
            ProvenancePurgeIdNotFoundException, ProvenanceDocumentNotInPurgeException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // check if vertex exists
            Iterator<PurgeEvent> it = framedGraph.query().has(PurgeEvent.PurgeId, purgeId).vertices(PurgeEvent.class).iterator();
            if (!it.hasNext()) {
                this.graph.rollback();
                throw new ProvenancePurgeIdNotFoundException(String.format("Purge Event with id %d not found", purgeId));
            }

            PurgeEvent event = it.next();

            if (!event.getPurgeDocumentIds().containsAll(completelyPurged)) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotInPurgeException("Document not found in PurgeEvent vertex's PurgeDocumentIds");
            }

            // combine the existing completed set with the new one
            Set<Long> complelted = event.getCompletelyPurgedDocumentIds();
            complelted.addAll(completelyPurged);
            event.setCompletelyPurgedDocumentIds(complelted);
            event.setResolved(resolved);

            // format description as
            // description \n\nNote: <timestamp> <applicationId> <userPrincipal>\n note
            if (StringUtils.isNotEmpty(note)) {
                StringBuilder sb = new StringBuilder();
                String description = event.getDescription();
                if (StringUtils.isNotEmpty(description)) {
                    sb.append(description);
                    sb.append(System.lineSeparator());
                    sb.append(System.lineSeparator());
                }
                sb.append("Note: ");
                sb.append(Utils.getCurrentDate().toString());
                sb.append(" ");
                sb.append(Utils.getApplication(securityToken));
                sb.append(" ");
                sb.append(Utils.getUser(securityToken));
                sb.append(System.lineSeparator());
                sb.append(note);

                event.setDescription(sb.toString());
            }
            this.graph.commit();

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // remove document ageOff inheritance from parent document
    public void removeDocAgeOffRuleInheritance(long documentId, String documentUri, long parentId, String parentUri) throws
            ProvenanceDocumentNotFoundException, ProvenanceAlreadyAgedException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // get document
            Iterator<Document> docIt = null;
            if (documentId > 0) {
                docIt = framedGraph.query().has(Document.DocumentId, documentId).vertices(Document.class).iterator();
            } else {
                docIt = framedGraph.query().has(Document.URI, documentUri).vertices(Document.class).iterator();
            }
            if (!docIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Document not found");
            }
            Document document = docIt.next();
            if (document.getAged()) {
                this.graph.rollback();
                throw new ProvenanceAlreadyAgedException("Document already aged");
            }

            // get parent
            Iterator<Document> parentIt = null;
            if (documentId > 0) {
                parentIt = framedGraph.query().has(Document.DocumentId, parentId).vertices(Document.class).iterator();
            } else {
                parentIt = framedGraph.query().has(Document.URI, parentUri).vertices(Document.class).iterator();
            }
            if (!parentIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Document not found");
            }
            Document parent = parentIt.next();

            // is there inheritance between the document and parent
            if (document.getInheritanceInfoOfParent(parent.getUri()) == null) {
                this.graph.rollback();
                return;
            }

            // get AgeOff edges from this parent
            Iterator<AgeOff> ageOffIt = document.getInAgeOffEdgesFromParent(parent.getUri());
            while (ageOffIt.hasNext()) {
                // reset inheritanceInfo and update vertex
                document.resetInheritanceInfo(parent.getUri());

                // remove the edge
                AgeOff ageOff = ageOffIt.next();
                this.graph.removeEdge(ageOff.asEdge());

                // examine child
                removeAgeOffEdgeExamineChild(document, ageOff);
            }

            this.graph.commit();
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // helper to recursively remove AgeOff edge or adjust RelevantDateTime
    private void removeAgeOffEdgeExamineChild(Document document, AgeOff theTarget) throws TException {

        // figure out if this is the only edge with ruleId or the oldest AgeOfReleventDate
        Date oldestRelevantDate = document.getInAgeOffOldestRelevantDateHasRule(theTarget.getRuleId());
        boolean isOnlyEdge = oldestRelevantDate == null;       // don't have any edge now
        boolean hasOldestRelevantDate = oldestRelevantDate != null && oldestRelevantDate.after(theTarget.getAgeOffRelevantDateTime());

        // no need to go further
        if (!isOnlyEdge && !hasOldestRelevantDate) {
            return;
        }
        // find the out ageOff edge with same ruleId
        Iterator<AgeOff> outIt = document.getOutAgeOffEdgesHasRule(theTarget.getRuleId());
        while (outIt.hasNext()) {
            AgeOff ageOff = outIt.next();
            Document child = ageOff.getInDocument();    // the child document to examine
            InheritanceInfo inheritanceInfo = child.getInheritanceInfoOfParent(document.getUri());

            if (inheritanceInfo.inheritParentAgeOff && inheritanceInfo.trackParentAgeOff) {
                if (isOnlyEdge) {              // the only edge
                    // delete the edge
                    this.graph.removeEdge(ageOff.asEdge());
                } else if (hasOldestRelevantDate) {   // the oldest
                    if (!inheritanceInfo.isSetAgeOffRelevantDateTime()) {
                        // update the edge to the now oldestRelevantDate
                        ageOff.setAgeOffRelevantDateTime(oldestRelevantDate);
                    }
                }
                // recursively examine the descendants
                removeAgeOffEdgeExamineChild(child, ageOff);
            }
        }
    }

    // remove the AgeOff edge from AgeOffRule identified by ageOffRuldId
    public void removeDocExplicitAgeOffRule(long documentId, String documentUri, long ageOffRuleId) throws
            ProvenanceDocumentNotFoundException, ProvenanceAgeOffRuleNotFoundException, ProvenanceAlreadyAgedException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // get document
            Iterator<Document> docIt = null;
            if (documentId > 0) {
                docIt = framedGraph.query().has(Document.DocumentId, documentId).vertices(Document.class).iterator();
            } else {
                docIt = framedGraph.query().has(Document.URI, documentUri).vertices(Document.class).iterator();
            }
            if (!docIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Document not found");
            }
            Document document = docIt.next();
            if (document.getAged()) {
                this.graph.rollback();
                throw new ProvenanceAlreadyAgedException("Document already aged");
            }

            // get ageOffRule
            Iterator<Vertex> ruleIt = this.graph.query().has(AgeOffRule.RuleId, ageOffRuleId).vertices().iterator();
            if (!ruleIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("AgeOffRule not found");
            }

            // get AgeOff edges from this AgeOffRule
            Iterator<AgeOff> ageOffIt = document.getInAgeOffEdgesFromAgeOffRule(ageOffRuleId);
            while (ageOffIt.hasNext()) {
                // remove the edge
                AgeOff ageOff = ageOffIt.next();
                this.graph.removeEdge(ageOff.asEdge());

                // examine child
                removeAgeOffEdgeExamineChild(document, ageOff);
            }

            this.graph.commit();
        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // add ageOff rule to document
    public void addDocExplicitAgeOffRule(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, AgeOffMapping mapping) throws
            ProvenanceDocumentNotFoundException, ProvenanceAgeOffRuleNotFoundException, ProvenanceAlreadyAgedException, ProvenanceAgeOffExistsException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // get document
            Iterator<Document> docIt = null;
            if (documentId > 0) {
                docIt = framedGraph.query().has(Document.DocumentId, documentId).vertices(Document.class).iterator();
            } else {
                docIt = framedGraph.query().has(Document.URI, documentUri).vertices(Document.class).iterator();
            }
            if (!docIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Document not found");
            }
            Document document = docIt.next();
            if (document.getAged()) {
                this.graph.rollback();
                throw new ProvenanceAlreadyAgedException("Document already aged");
            }

            // get ageOffRule
            Iterator<AgeOffRule> ruleIt = framedGraph.query().has(AgeOffRule.RuleId, mapping.getRuleId()).vertices(AgeOffRule.class).iterator();
            if (!ruleIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffRuleNotFoundException("AgeOffRule not found");
            }

            // make sure there is no edge from the AgeOffRule already
            Iterator<AgeOff> ageOffIt = document.getInAgeOffEdgesFromAgeOffRule(mapping.getRuleId());
            if (ageOffIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceAgeOffExistsException("There is already AgeOff edge from the AgeOffRule to Document");
            }

            // get the oldestRelevantDate before we add the new edge
            Date oldestRelevantDate = document.getInAgeOffOldestRelevantDateHasRule(mapping.ruleId);

            // add the AgeOff edge
            AgeOffRule rule = ruleIt.next();
            AgeOff ageOff = framedGraph.addEdge(null, rule.asVertex(), document.asVertex(), AgeOff.LABEL, AgeOff.class);
            // use mapping relevantDate if set. otherwise, use current time.
            ageOff.updateProperties(securityToken, mapping.getRuleId(), Utils.convertDateTime2Date(mapping.getAgeOffRelevantDateTime()));

            // figure out if this is the only edge with ruleId or the oldest AgeOfReleventDate
            boolean isOnlyEdge = oldestRelevantDate == null;       // don't have any edge yet
            boolean hasOldestRelevantDate = oldestRelevantDate != null && oldestRelevantDate.after(ageOff.getAgeOffRelevantDateTime());

            // examine child if either true
            if (isOnlyEdge || hasOldestRelevantDate) {
                addAgeOffEdgeExamineChild(securityToken, document, mapping.ruleId);
            }

            this.graph.commit();

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }

    // helper to recursively add AgeOff edge or adjust RelevantDateTime
    private void addAgeOffEdgeExamineChild(ezbake.base.thrift.EzSecurityToken securityToken, Document document, long ruleId) throws TException {
        FramedGraph<TitanGraph> framedGraph = getFramedGraph();

        Date oldestRelevantDate = document.getInAgeOffOldestRelevantDateHasRule(ruleId);

        // find all children Document vertex
        Iterator<Document> children = document.getChildDocuments();
        while (children.hasNext()) {
            Document child = children.next();
            InheritanceInfo info = child.getInheritanceInfoOfParent(document.getUri());

            if (info.inheritParentAgeOff && info.trackParentAgeOff) {
                // check if this child already has AgeOff edge with ruleId from this parent
                Iterator<AgeOff> ageOffIt = child.getInAgeOffEdgesFromParentHasRule(document.getUri(), ruleId);
                if (ageOffIt.hasNext()) {
                    // ensure the relevant time
                    ageOffIt.next().setAgeOffRelevantDateTime(info.isSetAgeOffRelevantDateTime() ? Utils.convertDateTime2Date(info.getAgeOffRelevantDateTime()) : oldestRelevantDate);
                } else {
                    // create new edge
                    AgeOff ageOff = framedGraph.addEdge(null, document.asVertex(), child.asVertex(), AgeOff.LABEL, AgeOff.class);
                    // use mapping relevantDate if set. otherwise, use current time.
                    ageOff.updateProperties(securityToken, ruleId, info.isSetAgeOffRelevantDateTime() ? Utils.convertDateTime2Date(info.getAgeOffRelevantDateTime()) : oldestRelevantDate);
                }

                // recursively examine descendant
                addAgeOffEdgeExamineChild(securityToken, child, ruleId);
            }
        }
    }

    // add new inheritanceInfo to document
    public void addDocumentInheritanceInfo(ezbake.base.thrift.EzSecurityToken securityToken, long documentId, String documentUri, InheritanceInfo inheritanceInfo) throws
            ezbake.base.thrift.EzSecurityTokenException, ProvenanceDocumentNotFoundException, ProvenanceCircularInheritanceNotAllowedException, ProvenanceAlreadyAgedException, ProvenanceAgeOffInheritanceExistsException, org.apache.thrift.TException {

        validateGraphDb();

        try {
            FramedGraph<TitanGraph> framedGraph = getFramedGraph();

            // get document
            Iterator<Document> docIt = null;
            if (documentId > 0) {
                docIt = framedGraph.query().has(Document.DocumentId, documentId).vertices(Document.class).iterator();
            } else {
                docIt = framedGraph.query().has(Document.URI, documentUri).vertices(Document.class).iterator();
            }
            if (!docIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Document not found");
            }
            Document document = docIt.next();
            if (document.getAged()) {
                throw new ProvenanceAlreadyAgedException("Document already aged");
            }

            // get parent
            Iterator<Document> parentIt = framedGraph.query().has(Document.URI, inheritanceInfo.parentUri).vertices(Document.class).iterator();
            if (!parentIt.hasNext()) {
                this.graph.rollback();
                throw new ProvenanceDocumentNotFoundException("Parent document not found");
            }
            Document parent = parentIt.next();

            // make sure there is no InheritanceInfo from parent already
            if (document.getInheritanceInfoOfParent(inheritanceInfo.parentUri) != null) {
                this.graph.rollback();
                throw new ProvenanceAgeOffInheritanceExistsException("Document already has inheritanceInfo from this parent");
            }

            // make sure this will not create a circular inheritance
            if (getDescendants(document).contains(parent.getDocumentId())) {
                this.graph.rollback();
                throw new ProvenanceCircularInheritanceNotAllowedException("Add this parent will cause circular inheritance");
            }

            // the original <RuleId, RelevantDate> map
            HashMap<Long, Date> originMap = document.getInAgeOffEdgeRuleOldestRelevantDateMap();

            // add the inheritanceInfo first
            document.addInheritanceInfo(inheritanceInfo.deepCopy());
            // create DeriveFrom the AgeOff edges from parent to child
            AddDocumentHelper helperAdd = new AddDocumentHelper(securityToken, documentUri);
            helperAdd.addInheritanceEdges(getFramedGraph(), parent, document, inheritanceInfo);

            HashMap<Long, Date> newMap = document.getInAgeOffEdgeRuleOldestRelevantDateMap();

            // compare the maps to decide if need to examine child
            for (Long ruleId : newMap.keySet()) {
                boolean isOnlyEdge = !originMap.containsKey(ruleId);
                boolean hasOldestRelevantDate = originMap.containsKey(ruleId) && originMap.get(ruleId).after(newMap.get(ruleId));

                // need to examine child if is the only edge or introduced new
                if (isOnlyEdge || hasOldestRelevantDate) {
                    addAgeOffEdgeExamineChild(securityToken, document, ruleId);
                }
            }

            this.graph.commit();

        } catch (TitanException ex) {
            this.graph.rollback();
            throw new TException(ex);
        }
    }
}



