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

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.data.common.graph.TitanGraphConfiguration;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.security.thrift.ezsecurityConstants;
import ezbake.services.provenance.graph.Utils;
import ezbake.services.provenance.graph.frames.*;
import ezbake.services.provenance.thrift.*;
import ezbake.services.provenance.thrift.AgeOffRule;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.*;
import org.junit.runners.MethodSorters;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import ezbake.security.impl.ua.FileUAService;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProvenanceServiceTest {
    private static ThriftClientPool pool;
    private static ThriftServerPool servers;

    private static EzSecurityToken securityToken = ezbake.security.test.MockEzSecurityToken.getMockAppToken("Test", "Principle");
    private static EzSecurityToken adminSecurityToken = ezbake.security.test.MockEzSecurityToken.getBlankToken("EzCentralPurgeService", "EzCentralPurgeService", 6000);

    private static TitanGraph graph = null;
    private static FramedGraph framedGraph = null;

    private static final String APP_NAME = "testapp";
    private static final String SERVICE_NAME = "provenanceservice";

    private static final String AGE_OFF_RULE_NAME = "age off rule #";
    private static final int AGE_OFF_RULE_COUNT = 10;
    private static long[] ageOffRuleIds = new long[AGE_OFF_RULE_COUNT];

    private static final String DOCUMENT_URI = "provenance://testdocument/";
    private static final int DOCUMENT_COUNT = 10;
    private static long[] documentIds = new long[DOCUMENT_COUNT * 3];
    private static long[] purgeIds = new long[DOCUMENT_COUNT * 3];

    @BeforeClass
    public static void init() throws Exception {
        EzConfiguration configuration = new EzConfiguration(new ClasspathConfigurationLoader());
        Properties properties = configuration.getProperties();
        setupGraphDb(properties);

        properties.setProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, APP_NAME);
        properties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");

        // for security
        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, ProvenanceServiceTest.class.getResource("/pki/server").getFile());
        properties.setProperty(FileUAService.USERS_FILENAME, ProvenanceServiceTest.class.getResource("/users.json").getFile());

        //Using the ThriftServerPool for testing. Just give it ezConfiguration and a starting port number
        servers = new ThriftServerPool(properties, 14000);

        // mock security service
        String securityId = properties.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);
        servers.startCommonService(new ezbake.security.service.processor.EzSecurityHandler(), ezsecurityConstants.SERVICE_NAME, securityId);

        //Now start the service supplying the service implementation and the service name
        properties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, ProvenanceServiceTest.class.getResource("/pki/client").getFile());
        servers.startApplicationService(new ProvenanceServiceImpl(), SERVICE_NAME, APP_NAME, securityId);

        //Still create our client pool like we would for real
        pool = new ThriftClientPool(properties);
    }

    private static void setupGraphDb(Properties properties) {
        TitanGraphConfiguration graphConfig = new TitanGraphConfiguration(properties);
        graphConfig.setTitanAccumuloProperties();

        graph = TitanFactory.open(graphConfig);
        framedGraph = new FramedGraphFactory(new JavaHandlerModule()).create(graph);
    }

    @AfterClass
    public static void cleanup() throws IOException {
        //Shutdown our client connections and the services
        if (pool != null) {
            pool.close();
        }
        if (servers != null) {
            servers.shutdown();
        }
        if(graph != null) {
            graph.shutdown();
            TitanCleanup.clear(graph);
        }
    }

    private String getAgeOffRuleName(int index) {
        return AGE_OFF_RULE_NAME + String.format("%03d", index);
    }

    @Test
    public void test1AddAgeOffRules() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 1; i <= AGE_OFF_RULE_COUNT; i++) {
                String name = getAgeOffRuleName(i);
                long duration = 24 * 60 * 60 * i;
                int maxPeriod = 60;
                long id = client.addAgeOffRule(securityToken, name, duration, maxPeriod);
                ageOffRuleIds[i - 1] = id;

                Iterator<ezbake.services.provenance.graph.frames.AgeOffRule> it = framedGraph.query().has(ezbake.services.provenance.graph.frames.AgeOffRule.Name, name).vertices(ezbake.services.provenance.graph.frames.AgeOffRule.class).iterator();

                assertTrue(it.hasNext());
                ezbake.services.provenance.graph.frames.AgeOffRule rule = it.next();
                assertNotEquals(rule, null);
                assertNotEquals(rule.getTimeStamp(), null);
                assertEquals(rule.getDuration(), duration);
                assertEquals(rule.getMaximumExecutionPeriod(), maxPeriod);
                assertEquals(rule.getRuleId(), id);
                assertEquals(rule.getType(), ezbake.services.provenance.graph.frames.AgeOffRule.TYPE);
                verifyVertexCommonProperty(rule, ezbake.services.provenance.graph.frames.AgeOffRule.TYPE);

                graph.commit();
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNameExistsException.class)
    public void test1AddAgeOffRulesNameExists() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = getAgeOffRuleName(1);
            long duration = 24 * 60 * 60;
            long id = client.addAgeOffRule(securityToken, name, duration, 60);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceIllegalAgeOffRuleNameException.class)
    public void test1AddAgeOffRulesNameEmpty() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "";
            long duration = 24 * 60 * 60;
            long id = client.addAgeOffRule(securityToken, name, duration, 60);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceIllegalAgeOffDurationSecondsException.class)
    public void test1AddAgeOffRulesDurationInvalid() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "test";
            long duration = 0;
            long id = client.addAgeOffRule(securityToken, name, duration, 60);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test2GetAgeOffRule() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 1; i <= AGE_OFF_RULE_COUNT; i++) {
                String name = getAgeOffRuleName(i);
                long duration = 24 * 60 * 60 * i;
                ezbake.services.provenance.thrift.AgeOffRule rule = client.getAgeOffRule(securityToken, name);

                assertNotEquals(rule, null);
                assertTrue(rule.getId() >= 0);
                assertEquals(rule.getName(), name);
                assertNotEquals(rule.getTimeStamp(), null);
                assertEquals(rule.getRetentionDurationSeconds(), duration);
                assertEquals(60, rule.getMaximumExecutionPeriod());
                assertEquals(rule.getApplication(), Utils.getApplication(securityToken));
                assertEquals(rule.getUser(), Utils.getUser(securityToken));
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test2GetAgeOffRuleById() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 1; i <= AGE_OFF_RULE_COUNT; i++) {
                String name = getAgeOffRuleName(i);
                long duration = 24 * 60 * 60 * i;
                long id = ageOffRuleIds[i - 1];
                ezbake.services.provenance.thrift.AgeOffRule rule = client.getAgeOffRuleById(securityToken, id);

                assertNotEquals(rule, null);
                assertTrue(rule.getId() >= 0);
                assertEquals(rule.getName(), name);
                assertNotEquals(rule.getTimeStamp(), null);
                assertEquals(rule.getRetentionDurationSeconds(), duration);
                assertEquals(rule.getApplication(), Utils.getApplication(securityToken));
                assertEquals(rule.getUser(), Utils.getUser(securityToken));
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test2GetAgeOffRuleNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "no such a name";
            ezbake.services.provenance.thrift.AgeOffRule rule = client.getAgeOffRule(securityToken, name);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test2GetAgeOffRuleByIdNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            int id = 987654321;
            ezbake.services.provenance.thrift.AgeOffRule rule = client.getAgeOffRuleById(securityToken, id);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test3UpdateAgeOffRule() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 1; i <= AGE_OFF_RULE_COUNT; i++) {
                String name = getAgeOffRuleName(i);
                long duration = 24 * 60 * 60 * i * 2;

                client.updateAgeOffRule(securityToken, name, duration);

                Iterator<ezbake.services.provenance.graph.frames.AgeOffRule> it = framedGraph.query().has(ezbake.services.provenance.graph.frames.AgeOffRule.Name, name).vertices(ezbake.services.provenance.graph.frames.AgeOffRule.class).iterator();

                assertTrue(it.hasNext());
                ezbake.services.provenance.graph.frames.AgeOffRule rule = it.next();

                assertNotEquals(rule, null);
                assertTrue(rule.getRuleId() >= 0);
                assertEquals(rule.getName(), name);
                assertNotEquals(rule.getTimeStamp(), null);
                assertEquals(rule.getDuration(), duration);
                verifyVertexCommonProperty(rule, ezbake.services.provenance.graph.frames.AgeOffRule.TYPE);
                graph.commit();
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceIllegalAgeOffDurationSecondsException.class)
    public void test3UpdateAgeOffRulesDurationInvalid() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "test";
            long duration = 0;
            client.updateAgeOffRule(securityToken, name, duration);
        } finally {
            pool.returnToPool(client);
        }
    }


    @Test(expected = ProvenanceIllegalAgeOffRuleNameException.class)
    public void test3UpdateAgeOffRulesNameEmpty() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "";
            long duration = 24 * 60 * 60;
            client.updateAgeOffRule(securityToken, name, duration);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test3UpdateAgeOffRulesNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String name = "name not exist";
            long duration = 24 * 60 * 60;
            client.updateAgeOffRule(securityToken, name, duration);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test2CountAgeOffRules() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            int count = client.countAgeOffRules(securityToken);
            assertEquals(count, AGE_OFF_RULE_COUNT);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test2GetAllAgeOffRules() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            // no paging
            List<AgeOffRule> rules = client.getAllAgeOffRules(securityToken, 0, 0);
            assertEquals(rules.size(), AGE_OFF_RULE_COUNT);

            // paging
            int limit = 3;
            int page = 1;

            do {
                int pageStartIndex = (page - 1) * limit;
                int expectedCount = (AGE_OFF_RULE_COUNT - pageStartIndex) > limit ? limit : (AGE_OFF_RULE_COUNT - pageStartIndex);
                rules = client.getAllAgeOffRules(securityToken, limit, page);
                assertEquals(rules.size(), expectedCount > 0 ? expectedCount : 0);

                for (int i = 0; i < rules.size(); i++) {
                    AgeOffRule rule = (AgeOffRule) rules.get(i);
                    assertEquals(rule.getId(), ageOffRuleIds[pageStartIndex + i]);
                }
                page++;
            } while (rules.size() > 0);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test2AddDocument() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date date = Utils.getCurrentDate();
            Date date2 = new Date(Utils.getCurrentDate().getTime() - 7 * 24 * 60 * 60 * 1000);

            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                String uri = DOCUMENT_URI + "parent/" + i;
                AgeOffMapping ageOffMapping = new AgeOffMapping();
                long ruleId = ageOffRuleIds[i % 10];
                ageOffMapping.setRuleId(ruleId);
                ageOffMapping.setAgeOffRelevantDateTime(Utils.convertDate2DateTime(date));

                List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
                rules.add(ageOffMapping);

                long id = client.addDocument(securityToken, uri, null, rules);
                documentIds[i] = id;

                // Document vertex
                Document document = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator().next();
                assertNotEquals(document, null);
                assertEquals(uri, document.getUri());
                assertEquals(id, document.getDocumentId());
                assertEquals(false, document.getAged());
                verifyVertexCommonProperty(document, Document.TYPE);

                // AgeOff edge
                Iterator<Edge> ageOffIt = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
                assertTrue(ageOffIt.hasNext());
                AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
                if (ageOff.getRuleId() == ruleId) {
                    assertEquals(ageOff.getAgeOffRelevantDateTime(), date);
                } else {
                    assertEquals(ageOff.getAgeOffRelevantDateTime(), date2);
                }
                verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);

                graph.commit();
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentExistsException.class)
    public void test3AddDocumentExists() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "parent/1";
            client.addDocument(securityToken, uri, null, null);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test3AddDocumentWithParents() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                String uri = DOCUMENT_URI + "child/" + i;
                String parentUri = DOCUMENT_URI + "parent/" + i;
                InheritanceInfo inherit = new InheritanceInfo(parentUri, true, true);

                Date relaventDatetime = new Date(Utils.getCurrentDate().getTime() - 24 * 60 * 60 * 1000);
                if (i < 5) {
                    inherit.setAgeOffRelevantDateTime(Utils.convertDate2DateTime(relaventDatetime));
                }
                List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
                parents.add(inherit);
                parents.add(inherit);

                Date date = Utils.getCurrentDate();
                AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[i % 10], Utils.convertDate2DateTime(date));
                List<AgeOffMapping> ageOffMappingList = new ArrayList<AgeOffMapping>();
                ageOffMappingList.add(ageOffMapping);
                ageOffMappingList.add(ageOffMapping);

                long docId = client.addDocument(securityToken, uri, parents, ageOffMappingList);
                documentIds[DOCUMENT_COUNT + i] = docId;

                // Document vertex
                Document document = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator().next();
                assertNotEquals(document, null);
                assertEquals(uri, document.getUri());
                assertEquals(docId, document.getDocumentId());
                assertFalse(document.getAged());
                assertEquals(1, document.getInheritanceInfoList().size());
                assertEquals(inherit, document.getInheritanceInfoList().get(0));
                verifyVertexCommonProperty(document, document.TYPE);

                // DerivedFrom edge
                Iterator<Edge> it = document.asVertex().getEdges(Direction.IN, DerivedFrom.LABEL).iterator();
                Edge edge = it.next();
                assertEquals(edge.getVertex(Direction.OUT).getProperty(Document.URI), parentUri);
                assertEquals(edge.getLabel(), DerivedFrom.LABEL);
                DerivedFrom derivedFrom = (DerivedFrom) framedGraph.frame(edge, DerivedFrom.class);
                verifyEdgeCommonProperty(derivedFrom, DerivedFrom.LABEL);

                // AgeOff edge
                it = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
                while (it.hasNext()) {
                    edge = it.next();
                    Vertex edgeOut = edge.getVertex(Direction.OUT);
                    if (edgeOut.getProperty(BaseVertex.Type).equals(Document.TYPE)) {
                        // from parent doc
                        AgeOff ageOffParent = (AgeOff) framedGraph.frame(edgeOut.getEdges(Direction.IN, AgeOff.LABEL).iterator().next(), AgeOff.class);
                        AgeOff ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
                        assertEquals(ageOff.getRuleId(), ageOffParent.getRuleId());
                        Date expectedDate = (i < 5) ? relaventDatetime : ageOffParent.getAgeOffRelevantDateTime();
                        assertEquals(expectedDate, ageOff.getAgeOffRelevantDateTime());
                        verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
                    } else if (edgeOut.getProperty(BaseVertex.Type).equals(ezbake.services.provenance.graph.frames.AgeOffRule.TYPE)) {
                        // form ageOffRule
                        ezbake.services.provenance.graph.frames.AgeOffRule ageOffParent = (ezbake.services.provenance.graph.frames.AgeOffRule) framedGraph.frame(edgeOut, ezbake.services.provenance.graph.frames.AgeOffRule.class);
                        AgeOff ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
                        assertEquals(ageOff.getRuleId(), ageOffParent.getRuleId());
                        assertEquals(ageOff.getAgeOffRelevantDateTime(), date);
                        verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
                    }
                }

                graph.commit();
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentWithParents2() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                String uri = DOCUMENT_URI + "g-child/" + i;
                String parentUri = DOCUMENT_URI + "child/" + i;
                InheritanceInfo inherit = new InheritanceInfo(parentUri, true, true);

                Date relaventDatetime = new Date(Utils.getCurrentDate().getTime() - 2 * 24 * 60 * 60 * 1000);
                if (i >= 5) {
                    inherit.setAgeOffRelevantDateTime(Utils.convertDate2DateTime(relaventDatetime));
                }
                List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
                parents.add(inherit);
                parents.add(inherit);

                Date date = Utils.getCurrentDate();
                AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[i % 10], Utils.convertDate2DateTime(date));
                List<AgeOffMapping> ageOffMappingList = new ArrayList<AgeOffMapping>();
                ageOffMappingList.add(ageOffMapping);
                ageOffMappingList.add(ageOffMapping);

                long docId = client.addDocument(securityToken, uri, parents, ageOffMappingList);
                documentIds[DOCUMENT_COUNT * 2 + i] = docId;

                // Document vertex
                Document document = framedGraph.query().has(Document.URI, uri).vertices(Document.class).iterator().next();
                assertNotEquals(document, null);
                assertEquals(uri, document.getUri());
                assertEquals(docId, document.getDocumentId());
                assertEquals(1, document.getInheritanceInfoList().size());
                assertEquals(inherit, document.getInheritanceInfoList().get(0));
                verifyVertexCommonProperty(document, document.TYPE);

                // DerivedFrom edge
                Iterator<Edge> it = document.asVertex().getEdges(Direction.IN, DerivedFrom.LABEL).iterator();
                Edge edge = it.next();
                assertEquals(edge.getVertex(Direction.OUT).getProperty(Document.URI), parentUri);
                assertEquals(edge.getLabel(), DerivedFrom.LABEL);
                DerivedFrom derivedFrom = (DerivedFrom) framedGraph.frame(edge, DerivedFrom.class);
                verifyEdgeCommonProperty(derivedFrom, DerivedFrom.LABEL);

                // AgeOff edge
                it = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
                while (it.hasNext()) {
                    edge = it.next();
                    Vertex edgeOut = edge.getVertex(Direction.OUT);
                    if (edgeOut.getProperty(BaseVertex.Type).equals(Document.TYPE)) {
                        // from parent doc
                        AgeOff ageOffParent = (AgeOff) framedGraph.frame(edgeOut.getEdges(Direction.IN, AgeOff.LABEL).iterator().next(), AgeOff.class);
                        AgeOff ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
                        assertEquals(ageOff.getRuleId(), ageOffParent.getRuleId());
                        Date expectedDate = (i >= 5) ? relaventDatetime : getMiniRelaventDateTime(edgeOut, ageOff.getRuleId());
                        assertEquals(expectedDate, ageOff.getAgeOffRelevantDateTime());
                        verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
                    } else if (edgeOut.getProperty(BaseVertex.Type).equals(ezbake.services.provenance.graph.frames.AgeOffRule.TYPE)) {
                        // form ageOffRule
                        ezbake.services.provenance.graph.frames.AgeOffRule ageOffParent = (ezbake.services.provenance.graph.frames.AgeOffRule) framedGraph.frame(edgeOut, ezbake.services.provenance.graph.frames.AgeOffRule.class);
                        AgeOff ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
                        assertEquals(ageOff.getRuleId(), ageOffParent.getRuleId());
                        assertEquals(ageOff.getAgeOffRelevantDateTime(), date);
                        verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
                    }
                }

                graph.commit();
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceCircularInheritanceNotAllowedException.class)
    public void test4AddDocumentWithSelfParents() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {

            String uri = DOCUMENT_URI + "self-parent/";
            InheritanceInfo inherit = new InheritanceInfo(uri, true, true);

            Date relaventDatetime = new Date(Utils.getCurrentDate().getTime() - 2 * 24 * 60 * 60 * 1000);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);

            Date date = Utils.getCurrentDate();
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[0], Utils.convertDate2DateTime(date));
            List<AgeOffMapping> ageOffMappingList = new ArrayList<AgeOffMapping>();
            ageOffMappingList.add(ageOffMapping);
            long docId = client.addDocument(securityToken, uri, parents, ageOffMappingList);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetAddDocumentMaxSize() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            int size = client.getAddDocumentsMaxSize(securityToken);
            System.out.println("batch size = " + size);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceExceedsMaxBatchSizeException.class)
    public void test4AddDocumentsExceedsLimit() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            int size = 11111;
            String uri = DOCUMENT_URI + "bulk/";
            Set<AddDocumentEntry> documents = new HashSet<>();
            for (int i = 0; i < size; i++) {
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                documents.add(entry);
            }
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, null);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsInOrder() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            int size = 20;
            String uri = DOCUMENT_URI + "bulk/";
            for (int i = 0; i < size; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                if (i > 0) {
                    String parentUri = uri + (i - 1);
                    String parentUri2 = uri + (i - 2);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                    if (i > 1) {
                        inheritanceInfos.add(new InheritanceInfo(parentUri2, true, true));
                    }
                }
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            StopWatch watch = new StopWatch();
            watch.start();

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            watch.stop();
            System.out.println(String.format("add %d documents take %d ms", size, watch.getNanoTime() / 1000000));
            assertEquals(size, results.size());

            for (int i = 0; i < size; i++) {
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
            }
            for (int i = 2; i < 10; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
                assertEquals(2, document.getParentsSize());
                assertEquals(3, document.getAgeOffInfoSize());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsReverseOrder() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[2], Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk1/";

            int size = 20;
            for (int i = size; i > 0; i--) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                if (i > 1) {
                    String parentUri = uri + (i - 1);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                }
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }


            StopWatch watch = new StopWatch();
            watch.start();

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            watch.stop();
            System.out.println(String.format("add %d documents take %d ms", size, watch.getNanoTime() / 1000000));
            assertEquals(size, results.size());

            for (int i = 1; i <= size; i++) {
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
            }
            for (int i = 2; i < 10; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
                assertEquals(1, document.getParentsSize());
                assertEquals(2, document.getAgeOffInfoSize());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocuments2Trees() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk2/";
            for (int i = 0; i < 5; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                if (i > 0) {
                    String parentUri = uri + (i - 1);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                }
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 9; i >= 5; i--) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                if (i > 5) {
                    String parentUri = uri + (i - 1);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                }
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(10, results.size());

            for (int i = 2; i < 5; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
                assertEquals(1, document.getParentsSize());
                assertEquals(2, document.getAgeOffInfoSize());
            }
            for (int i = 9; i > 5; i--) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
                assertEquals(1, document.getParentsSize());
                assertEquals(2, document.getAgeOffInfoSize());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5AddDocumentsExistingRoot() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk2/";
            for (int i = 10; i < 15; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 1);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(5, results.size());

            for (int i = 10; i < 15; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());

                assertEquals(1, document.getParentsSize());
                assertEquals(2, document.getAgeOffInfoSize());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5AddDocumentsExistingRoot2Branchs() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk2/";
            for (int i = 20; i < 25; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 1);
                if (i == 20) {
                    parentUri = uri + 9;
                }
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }
            for (int i = 25; i < 30; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 5);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(10, results.size());
            for (int i = 20; i < 30; i++) {
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
            }

            for (int i = 10; i < 15; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);

                assertEquals(1, document.getParentsSize());
                assertEquals(2, document.getAgeOffInfoSize());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentNotFoundException.class)
    public void test4AddDocumentsCircle() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk3/";
            for (int i = 0; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + ((i + 1) % 3);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(3, results.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED, results.get(uri + i).getStatus());
            }

            DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + 1);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsCircleWithHead() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk3/";
            for (int i = 0; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                if (i > 0) {
                    String parentUri = uri + (i - 1);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                }
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + 2;
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + 3);
            entry.setParents(inheritanceInfos);
            documents.add(entry);

            inheritanceInfos = new HashSet<>();
            parentUri = uri + 3;
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            entry = new AddDocumentEntry(uri + 4);
            entry.setParents(inheritanceInfos);
            documents.add(entry);

            inheritanceInfos = new HashSet<>();
            parentUri = uri + 4;
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            entry = new AddDocumentEntry(uri + 2);
            entry.setParents(inheritanceInfos);
            documents.add(entry);

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(5, results.size());

            for (int i = 0; i < 2; i++) {
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
            }
            for (int i = 2; i < 5; i++) {
                assertEquals(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED, results.get(uri + i).getStatus());
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentNotFoundException.class)
    public void test3AddDocumentsCircleWithTail() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk4/";
            for (int i = 0; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + ((i + 1) % 3);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 3; i < 5; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + 3;
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(5, results.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED, results.get(uri + i).getStatus());
            }

            DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + 1);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsMixedTrees() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            // first tree has circle with tail
            String uri = DOCUMENT_URI + "bulk5/";
            for (int i = 0; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + ((i + 1) % 3);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 3; i < 5; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + 3;
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            // second tree is valid
            for (int i = 10; i < 15; i++) {
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);

                if (i > 10) {
                    Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                    String parentUri = uri + (i - 1);
                    inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                    entry.setParents(inheritanceInfos);
                }

                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(10, results.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED, results.get(uri + i).getStatus());
            }
            for (int i = 10; i < 15; i++) {
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
                DocumentInfo document = client.getDocumentInfo(securityToken, results.get(uri + i).getDocumentId(), null);
                assertEquals(uri + i, document.getUri());
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsNoRelation() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            Set<AddDocumentEntry> documents = new HashSet<>();
            String uri = DOCUMENT_URI + "bulk6/";
            for (int i = 0; i < 5; i++) {
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, null);
            assertEquals(5, results.size());
            for (int i = 0; i < 5; i++) {
                AddDocumentResult result = results.get(uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, result.getStatus());
                DocumentInfo document = client.getDocumentInfo(securityToken, result.getDocumentId(), null);
                assertEquals(result.getDocumentId(), document.getDocumentId());
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsNotTrees() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {

            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            Set<AgeOffMapping> ageOffRules = new HashSet<>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk7/";

            String base = uri + "0";
            AddDocumentEntry entry = new AddDocumentEntry(base);
            documents.add(entry);

            for (int i = 1; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = base;
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 5; i < 7; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 2);
                inheritanceInfos.add(new InheritanceInfo(base, true, true));
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 7; i < 9; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 2);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            for (int i = 3; i < 5; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 2);
                inheritanceInfos.add(new InheritanceInfo(base, true, true));
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, ageOffRules);
            assertEquals(9, results.size());
            for (int i = 0; i < 9; i++) {
                AddDocumentResult result = results.get(uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, result.getStatus());
                DocumentInfo document = client.getDocumentInfo(securityToken, result.getDocumentId(), null);
                assertEquals(result.getDocumentId(), document.getDocumentId());

                if (i == 0) {
                    assertEquals(6, document.getChildrenSize());
                }
                if (i > 2 && i < 7) {
                    assertEquals(2, document.getParentsSize());
                    assertEquals(3, document.getAgeOffInfoSize());
                }
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsSelfCircle() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {

            AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
            List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
            ageOffRules.add(ageOffMapping);

            Set<AddDocumentEntry> documents = new HashSet<>();

            String uri = DOCUMENT_URI + "bulk8/";

            String base = uri + "0";
            AddDocumentEntry entry = new AddDocumentEntry(base);
            documents.add(entry);

            for (int i = 1; i < 3; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = base;
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                inheritanceInfos.add(new InheritanceInfo(uri + i, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }


            for (int i = 3; i < 5; i++) {
                Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
                String parentUri = uri + (i - 2);
                inheritanceInfos.add(new InheritanceInfo(base, true, true));
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
                entry = new AddDocumentEntry(uri + i);
                entry.setParents(inheritanceInfos);

                documents.add(entry);
            }

            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, null);
            assertEquals(5, results.size());
            for (int i = 1; i < 5; i++) {
                AddDocumentResult result = results.get(uri + i);
                assertEquals(AddDocumentStatus.CIRCULAR_INHERITANCE_NOT_ALLOWED, result.getStatus());
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetDocumentInfo() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                DocumentInfo info = client.getDocumentInfo(securityToken, 0, DOCUMENT_URI + "child/" + i);
                assertNotEquals(info.getDocumentId(), 0);
                assertEquals(1, info.getParentsSize());
                assertTrue(info.getParents().get(0).containsValue(DOCUMENT_URI + "parent/" + i));
                assertEquals(1, info.getChildrenSize());
                assertTrue(info.getChildren().get(0).containsValue(DOCUMENT_URI + "g-child/" + i));
                assertEquals(2, info.getAgeOffInfo().size());
                for (DocumentAgeOffInfo docInfo : info.getAgeOffInfo()) {
                    if (docInfo.isInherited()) {
                        assertEquals(DOCUMENT_URI + "parent/" + i, docInfo.getInheritedFromUri());
                    }
                    assertEquals(ageOffRuleIds[i % AGE_OFF_RULE_COUNT], docInfo.getRuleId());
                    assertEquals(60, docInfo.getMaximumExecutionPeriod());
                }
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetAncestors() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<String> uris = new ArrayList<String>();
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "g-child/" + i);
            }
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "no-such-uri/" + i);
            }
            DerivedResult result = client.getDocumentAncestors(securityToken, uris);
            assertEquals(5, result.getUrisNotFoundSize());
            assertEquals(15, result.getDerivedDocsSize());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetAncestorsOnlyUri() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<String> uris = new ArrayList<String>();
            uris.add(DOCUMENT_URI + "child/1");
            DerivedResult result = client.getDocumentAncestors(securityToken, uris);
            assertEquals(0, result.getUrisNotFoundSize());
            assertEquals(2, result.getDerivedDocsSize());
            assertEquals(1, result.getImmediateChildrenSize());
            assertEquals(Long.valueOf(documentIds[DOCUMENT_COUNT * 2 + 1]), result.getImmediateChildren().get(0));
        } finally {
            pool.returnToPool(client);
        }
    }


    @Test
    public void test4GetDescendants() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<String> uris = new ArrayList<String>();
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "child/" + i);
            }
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "no-such-uri/" + i);
            }
            DerivedResult result = client.getDocumentDescendants(securityToken, uris);
            assertEquals(5, result.getUrisNotFoundSize());
            assertEquals(10, result.getDerivedDocsSize());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetDescendantsOnlyUri() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<String> uris = new ArrayList<String>();
            uris.add(DOCUMENT_URI + "child/1");
            DerivedResult result = client.getDocumentDescendants(securityToken, uris);
            assertEquals(0, result.getUrisNotFoundSize());
            assertEquals(2, result.getDerivedDocsSize());
            assertEquals(1, result.getImmediateChildrenSize());
            assertEquals(Long.valueOf(documentIds[DOCUMENT_COUNT * 2 + 1]), result.getImmediateChildren().get(0));
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4MarkForPurge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<String> uris = new ArrayList<String>();
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "child/" + i);
            }
            for (int i = 1; i <= 5; i++) {
                uris.add(DOCUMENT_URI + "no-such-uri/" + i);
            }
            String name = "purge name";
            String description = "purge description";
            PurgeInitiationResult result = client.markDocumentForPurge(adminSecurityToken, uris, name, description);
            long id = result.getPurgeId();
            purgeIds[0] = id;
            PurgeEvent event = framedGraph.query().has(PurgeEvent.PurgeId, id).vertices(PurgeEvent.class).iterator().next();
            assertEquals(name, event.getName());
            assertEquals(description, event.getDescription());
            assertEquals(uris, event.getDocumentUris());
            assertEquals(result.getUrisNotFound(), event.getDocumentUrisNotFound());
            assertEquals(event.getPurgeDocumentIds(), result.getToBePurged());
            assertEquals(5, result.getUrisNotFoundSize());
            assertEquals(10, result.getToBePurgedSize());
        } finally {
            pool.returnToPool(client);
        }

    }

    @Test
    public void test5GetPurgeInfo() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            PurgeInfo result = client.getPurgeInfo(adminSecurityToken, purgeIds[0]);
            PurgeEvent event = framedGraph.query().has(PurgeEvent.PurgeId, purgeIds[0]).vertices(PurgeEvent.class).iterator().next();
            assertEquals(purgeIds[0], result.getId());
            assertNotNull(result.getTimeStamp());
            assertEquals(event.getDocumentUris(), result.getDocumentUris());
            assertEquals(event.getDocumentUrisNotFound(), result.getDocumentUrisNotFound());
            assertEquals(event.getPurgeDocumentIds(), result.getPurgeDocumentIds());
            assertEquals(event.getPurgeDocumentIds(), result.getPurgeDocumentIds());
            assertEquals(event.getCompletelyPurgedDocumentIds(), result.getCompletelyPurgedDocumentIds());
            assertEquals(event.getName(), result.getName());
            assertEquals(event.getDescription(), result.getDescription());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5GetAllPurgeIds() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<Long> result = client.getAllPurgeIds(adminSecurityToken);
            assertEquals(1, result.size());
            assertEquals((Long) purgeIds[0], (Long) result.get(0));
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5UpdatePurge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> completePurge = new HashSet<>();
            for (int i = 1; i <= 5; i++) {
                completePurge.add(documentIds[DOCUMENT_COUNT + i]);
            }
            boolean resolved = true;
            client.updatePurge(adminSecurityToken, purgeIds[0], completePurge, "test note", resolved);
            PurgeEvent event = framedGraph.query().has(PurgeEvent.PurgeId, purgeIds[0]).vertices(PurgeEvent.class).iterator().next();
            assertEquals(completePurge, event.getCompletelyPurgedDocumentIds());
            assertTrue(event.getDescription().endsWith("test note"));
            assertEquals(resolved, event.getResolved());

            System.out.println(event.getDescription());
            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentNotInPurgeException.class)
    public void test5UpdatePurgeDocumentNotInPurge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> completePurge = new HashSet<>();
            for (int i = 1; i <= 5; i++) {
                completePurge.add(documentIds[DOCUMENT_COUNT + i]);
            }
            completePurge.add(999999999999L);
            client.updatePurge(adminSecurityToken, purgeIds[0], completePurge, null, false);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetUriFromBitPosition() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<Long> docIds = new ArrayList<Long>();
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                docIds.add(documentIds[i]);
            }
            List<Long> fakeIds = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                fakeIds.add(999999999999L + i);
            }
            List<Long> allIds = new ArrayList<>(docIds);
            allIds.addAll(fakeIds);

            PositionsToUris result = client.getDocumentUriFromId(securityToken, allIds);
            assertEquals(DOCUMENT_COUNT, result.getMappingSize());
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                assertEquals(DOCUMENT_URI + "parent/" + i, result.getMapping().get(documentIds[i]));
            }
            assertEquals(fakeIds.size(), result.getUnfoundPositionListSize());
            assertEquals(fakeIds, result.getUnfoundPositionList());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentsPerformance() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {

            Set<AddDocumentEntry> documents = new HashSet<>();

            int size = 100;
            String uri = DOCUMENT_URI + "performance-bulk/";
            for (int i = 0; i < size; i++) {
                AddDocumentEntry entry = new AddDocumentEntry(uri + i);
                documents.add(entry);
            }
            for (int i = 0; i < 10; i++) {
                AddDocumentEntry entry = new AddDocumentEntry(DOCUMENT_URI + "bulk/" + i);
                documents.add(entry);
            }

            StopWatch watch = new StopWatch();
            watch.start();
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, null);
            watch.stop();
            System.out.println(String.format("add %d documents take %d ms", size, watch.getNanoTime()/1000000));

            assertEquals(size + 10, results.size());

            for (int i = 1; i < 50; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());

                client.getDocumentInfo(securityToken, 0, uri + i);
            }

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetConvertedUrisFromIds() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> docIds = new HashSet<>();
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                docIds.add(documentIds[i]);
            }

            Set<Long> fakeIds = new HashSet<Long>();
            for (int i = 0; i < 5; i++) {
                fakeIds.add(999999999999L + i);
            }

            Set<Long> allIds = new HashSet<>(docIds);
            allIds.addAll(fakeIds);

            ConversionResult result = client.getDocumentConvertedUrisFromIds(securityToken, allIds);
            assertEquals(fakeIds.size(), result.getIdsNotFoundSize());
            assertTrue(CollectionUtils.isEqualCollection(docIds, result.getConvertedUris()));
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetConvertedUrisFromUris() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<String> uris = new HashSet<String>();
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                uris.add(DOCUMENT_URI + "parent/" + i);
            }
            for (int i = 0; i < 5; i++) {
                uris.add("test://uri_not_exists/" + i);
            }

            ConversionResult result = client.getDocumentConvertedUrisFromUris(securityToken, uris);
            assertEquals(5, result.getUrisNotFound().size());

            List<Long> expectedIds = new ArrayList<Long>();
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                expectedIds.add(documentIds[i]);
            }
            assertTrue(CollectionUtils.isEqualCollection(expectedIds, result.getConvertedUris()));
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = EzSecurityTokenException.class)
    public void test5AgeOffInvalidSecurity() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            long ruleId = ageOffRuleIds[0];
            Date effectiveTime = Utils.getCurrentDate();
            client.startAgeOffEvent(securityToken, ruleId, Utils.convertDate2DateTime(effectiveTime));
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test5AgeOffRuleNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            long ruleId = 999999999999L;
            Date effectiveTime = Utils.getCurrentDate();
            client.startAgeOffEvent(adminSecurityToken, ruleId, Utils.convertDate2DateTime(effectiveTime));
        } finally {
            pool.returnToPool(client);
        }
    }

    private static Set<Long> ageoffDocIds = new HashSet<Long>();

    @Test
    public void test5AgeOffEmptyBv() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            long ruleId = ageOffRuleIds[0];
            Date effectiveTime = new Date(0);
            AgeOffInitiationResult result = client.startAgeOffEvent(adminSecurityToken, ruleId, Utils.convertDate2DateTime(effectiveTime));
            List<Long> docIds = new ArrayList<>(result.getAgeOffDocumentIds());
            List<Long> expect = new ArrayList<>();
            assertEquals(expect, docIds);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5AgeOff() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            for (int i = 1; i <= AGE_OFF_RULE_COUNT; i++) {
                long ruleId = ageOffRuleIds[i - 1];
                Date effectiveTime = Utils.getCurrentDate();// new Date(new Date().getTime() + 3 * 24 * 60 *60 * 1000);
                AgeOffInitiationResult result = client.startAgeOffEvent(adminSecurityToken, ruleId, Utils.convertDate2DateTime(effectiveTime));
                List<Long> docIds = new ArrayList<>(result.getAgeOffDocumentIds());
                ezbake.services.provenance.graph.frames.AgeOffRule rule = framedGraph.query().has(ezbake.services.provenance.graph.frames.AgeOffRule.RuleId, ruleId).vertices(ezbake.services.provenance.graph.frames.AgeOffRule.class).iterator().next();
                long duration = rule.getDuration();
                Set<Long> expectedDocIds = new HashSet<Long>();

                Iterator<AgeOff> ageOffIterator = framedGraph.query().has(AgeOff.Rule, ruleId).edges(AgeOff.class).iterator();
                while (ageOffIterator.hasNext()) {
                    AgeOff ageOff = ageOffIterator.next();
                    long docId = ageOff.asEdge().getVertex(Direction.IN).getProperty(Document.DocumentId);
                    Date compare = new Date(ageOff.getAgeOffRelevantDateTime().getTime() + duration);
                    if (compare.before(effectiveTime)) {
                        expectedDocIds.add(docId);
                    }
                }
                List<Long> expect = new ArrayList<Long>(expectedDocIds);
                Collections.sort(expect);
                Collections.sort(docIds);

                assertEquals(expect, docIds);

                // used by following test case
                if (i == 1) {
                    ageoffDocIds = new HashSet<Long>(docIds);
                }
            }
            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentNotFoundException.class)
    public void test5MarkAsAgedIdTooLarge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> ids = new HashSet<>();
            ids.add(999999999999L);
            client.markDocumentAsAged(adminSecurityToken, ids);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5MarkAsAged() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            client.markDocumentAsAged(adminSecurityToken, ageoffDocIds);

            assertNotEquals(ageoffDocIds.size(), 0);

            for (Long id : ageoffDocIds) {
                Document doc = (Document) framedGraph.query().has(Document.DocumentId, id).vertices(Document.class).iterator().next();
                assertTrue(doc.getAged());

                Iterator<Edge> it = doc.asVertex().query().labels(AgeOff.LABEL).edges().iterator();
                assertFalse(it.hasNext());
            }
            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceDocumentNotFoundException.class)
    public void test5MarkAsAgedDocumentNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> ids = new HashSet<Long>();
            ids.add(999999999999L);
            client.markDocumentAsAged(adminSecurityToken, ids);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveDocAgeOffRuleInheritanceOldestRelevantDate() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String child = DOCUMENT_URI + "child/1";
            String parent = DOCUMENT_URI + "parent/1";
            client.removeDocumentAgeOffRuleInheritance(securityToken, 0, child, 0, parent);
            Document document = framedGraph.query().has(Document.URI, child).vertices(Document.class).iterator().next();

            verifyResetInheritanceInfo(document, parent);

            // the ageOff edge should be removed
            Iterator<Edge> it = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
            while (it.hasNext()) {
                AgeOff ageOff = (AgeOff) framedGraph.frame(it.next(), AgeOff.class);
                Vertex pV = ageOff.asEdge().getVertex(Direction.OUT);
                assertNotEquals(pV.getProperty(Document.URI), parent);
            }

            // verify descendant
            String gChild = DOCUMENT_URI + "g-child/1";
            it = document.asVertex().getEdges(Direction.OUT, AgeOff.LABEL).iterator();
            // should still have an AgeOff edge out to the g-child
            assertTrue(it.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(it.next(), AgeOff.class);
            Vertex pV = ageOff.asEdge().getVertex(Direction.IN);
            assertEquals(pV.getProperty(Document.URI), gChild);
            // the relevantDate should have been updated
            assertEquals(document.getInAgeOffOldestRelevantDateHasRule(ageOffRuleIds[1]), ageOff.getAgeOffRelevantDateTime());

            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveDocAgeOffRuleInheritanceOnlyEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            // setup first
            int number = DOCUMENT_COUNT + 1;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String g2child = DOCUMENT_URI + "g2-child/" + number;

            // add parent
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            AgeOffMapping ageOffMapping2 = new AgeOffMapping(ageOffRuleIds[2], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
            rules.add(ageOffMapping);
            rules.add(ageOffMapping2);
            client.addDocument(securityToken, parent, null, rules);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // one more layer
            inherit = new InheritanceInfo(gchild, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, g2child, parents, null);

            client.removeDocumentAgeOffRuleInheritance(securityToken, 0, child, 0, parent);
            verifyRemovedAgeOffInheritance(child, parent);
            verifyRemovedAgeOffInheritance(gchild, child);
            verifyRemovedAgeOffInheritance(g2child, gchild);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveDocAgeOffRuleInheritanceMultiParents() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            // setup first
            int number = DOCUMENT_COUNT + 2;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String parent2 = DOCUMENT_URI + "parent/" + (++number);

            // add parent
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            AgeOffMapping ageOffMapping2 = new AgeOffMapping(ageOffRuleIds[2], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
            rules.add(ageOffMapping);
            rules.add(ageOffMapping2);
            client.addDocument(securityToken, parent, null, rules);

            // add parent2
            client.addDocument(securityToken, parent2, null, rules);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            InheritanceInfo inherit2 = new InheritanceInfo(parent2, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            parents.add(inherit2);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            client.removeDocumentAgeOffRuleInheritance(securityToken, 0, child, 0, parent);
            Document document = framedGraph.query().has(Document.URI, child).vertices(Document.class).iterator().next();
            verifyResetInheritanceInfo(document, parent);

            // the ageOff edge should be removed
            Iterator<Edge> it = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(it.hasNext());
            while (it.hasNext()) {
                AgeOff ageOff = (AgeOff) framedGraph.frame(it.next(), AgeOff.class);
                Vertex pV = ageOff.asEdge().getVertex(Direction.OUT);
                assertNotEquals(pV.getProperty(Document.URI), parent);
            }

            // descendant
            it = document.asVertex().getEdges(Direction.OUT, AgeOff.LABEL).iterator();
            // should still have an AgeOff edge out to the g-child
            assertTrue(it.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(it.next(), AgeOff.class);
            Vertex pV = ageOff.asEdge().getVertex(Direction.IN);
            assertEquals(pV.getProperty(Document.URI), gchild);
            // the relevantDate should have been updated
            assertEquals(document.getInAgeOffOldestRelevantDateHasRule(ageOffRuleIds[1]), ageOff.getAgeOffRelevantDateTime());

            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveExplicitAgeOffRuleNotOnly() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "parent/4";
            long ruleId = ageOffRuleIds[4];

            client.removeDocumentExplicitAgeOffRule(securityToken, 0, uri, ruleId);

            // verify no AgeOff edge to document
            Vertex document = graph.query().has(Document.URI, uri).vertices().iterator().next();
            Iterator<Edge> ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertFalse(ageOffIt.hasNext());

            // child should still have edge
            uri = DOCUMENT_URI + "child/4";
            Vertex child = graph.query().has(Document.URI, uri).vertices().iterator().next();
            ageOffIt = child.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());

            // g-child should still have edge
            uri = DOCUMENT_URI + "g-child/4";
            Vertex gchild = graph.query().has(Document.URI, uri).vertices().iterator().next();
            ageOffIt = gchild.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            // should have the oldest relevant date
            AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            Document doc = (Document) framedGraph.frame(child, Document.class);
            assertEquals(ageOff.getAgeOffRelevantDateTime(), doc.getInAgeOffOldestRelevantDateHasRule(ruleId));

            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveExplicitAgeOffRuleOnlyEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            // setup first
            int number = DOCUMENT_COUNT + 4;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String g2child = DOCUMENT_URI + "g2-child/" + (++number);

            // add parent
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            AgeOffMapping ageOffMapping2 = new AgeOffMapping(ageOffRuleIds[2], Utils.convertDate2DateTime(Utils.getCurrentDate()));
            List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
            rules.add(ageOffMapping);
            rules.add(ageOffMapping2);
            client.addDocument(securityToken, parent, null, rules);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // one more layer
            inherit = new InheritanceInfo(gchild, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, g2child, parents, null);

            client.removeDocumentExplicitAgeOffRule(securityToken, 0, parent, ageOffRuleIds[1]);
            client.removeDocumentExplicitAgeOffRule(securityToken, 0, parent, ageOffRuleIds[2]);
            verifyRemovedAgeOffInheritance(child, parent);
            verifyRemovedAgeOffInheritance(gchild, child);
            verifyRemovedAgeOffInheritance(g2child, gchild);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test6AddDocExplicitAgeOffRuleOnlyEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date date = new Date(Utils.getCurrentDate().getTime() - 10 * 24 * 60 * 60 * 1000);
            long ruleId = ageOffRuleIds[1];
            AgeOffMapping mapping = new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date));

            // setup first
            int number = DOCUMENT_COUNT + 7;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;

            // add parent
            client.addDocument(securityToken, parent, null, null);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // add this rule
            client.addDocumentExplicitAgeOffRule(securityToken, 0, parent, mapping);

            // verify AgeOff edge to document
            Vertex document = graph.query().has(Document.URI, parent).vertices().iterator().next();
            Iterator<Edge> ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // child should have edge too
            document = graph.query().has(Document.URI, child).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g-child should still have edge
            document = graph.query().has(Document.URI, gchild).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test6AddDocExplicitAgeOffRuleOldestEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date date = new Date(Utils.getCurrentDate().getTime() - 10 * 24 * 60 * 60 * 1000);
            long ruleId = ageOffRuleIds[1];
            AgeOffMapping mapping = new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date));

            // setup first
            int number = DOCUMENT_COUNT + 8;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String g2child = DOCUMENT_URI + "g2-child/" + number;
            String gchild2 = DOCUMENT_URI + "g-child/" + (++number);

            List<AgeOffMapping> mappingList = new ArrayList<AgeOffMapping>();
            mappingList.add(new AgeOffMapping(ruleId, Utils.convertDate2DateTime(Utils.getCurrentDate())));

            // add parent
            client.addDocument(securityToken, parent, null, mappingList);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // add another grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild2, parents, null);


            // add grand-grand Child
            inherit = new InheritanceInfo(gchild, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, g2child, parents, null);

            // add the rule explicitly to child
            client.addDocumentExplicitAgeOffRule(securityToken, 0, child, mapping);

            // verify AgeOff edge to document
            Vertex document = graph.query().has(Document.URI, child).vertices().iterator().next();
            Iterator<Edge> ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertTrue(ageOffIt.hasNext());

            // g-child should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, gchild).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g-child-2 should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, gchild2).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g2-child should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, g2child).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            graph.commit();
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test6AddDocExplicitAgeOffRuleNotOldestEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date date = new Date(Utils.getCurrentDate().getTime() - 10 * 24 * 60 * 60 * 1000);
            Date newDate = Utils.getCurrentDate();

            long ruleId = ageOffRuleIds[1];
            AgeOffMapping mapping = new AgeOffMapping(ruleId, Utils.convertDate2DateTime(newDate));

            // setup first
            int number = DOCUMENT_COUNT + 10;
            String parent = DOCUMENT_URI + "parent/" + number;
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String g2child = DOCUMENT_URI + "g2-child/" + number;
            String gchild2 = DOCUMENT_URI + "g-child/" + (++number);

            List<AgeOffMapping> mappingList = new ArrayList<AgeOffMapping>();
            mappingList.add(new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date)));

            // add parent
            client.addDocument(securityToken, parent, null, mappingList);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // add another grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild2, parents, null);


            // add grand-grand Child
            inherit = new InheritanceInfo(gchild, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, g2child, parents, null);

            // add the rule explicitly to child
            client.addDocumentExplicitAgeOffRule(securityToken, 0, child, mapping);

            // verify AgeOff edge to document
            Vertex document = graph.query().has(Document.URI, child).vertices().iterator().next();
            Iterator<Edge> ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertTrue(ageOffIt.hasNext());

            // g-child should still have one edge with origin relevantdate
            document = graph.query().has(Document.URI, gchild).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g-child-2 should still have one edge with origin relevantdate
            document = graph.query().has(Document.URI, gchild2).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g2-child should still have one edge with origin relevantdate
            document = graph.query().has(Document.URI, g2child).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            graph.commit();

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffInheritanceExistsException.class)
    public void test7AddDocAgeOffRuleInheritanceInheritanceExists() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);
        try {
            String parent = DOCUMENT_URI + "parent/7";
            String child = DOCUMENT_URI + "child/7";
            InheritanceInfo info = new InheritanceInfo(parent, true, true);
            client.addDocumentInheritanceInfo(securityToken, 0, child, info);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test7AddDocAgeOffRuleInheritanceOnlyEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            int index = DOCUMENT_COUNT + 12;
            long ruleId = ageOffRuleIds[0];

            String parent = DOCUMENT_URI + "parent/" + index;
            String child = DOCUMENT_URI + "child/" + index;
            String gchild = DOCUMENT_URI + "g-child/" + index;

            //set up
            List<AgeOffMapping> mappingList = new ArrayList<AgeOffMapping>();
            Date date = Utils.getCurrentDate();
            mappingList.add(new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date)));
            // add parent
            client.addDocument(securityToken, parent, null, mappingList);
            // add child
            client.addDocument(securityToken, child, null, null);
            // add grandChild
            InheritanceInfo inherit = new InheritanceInfo(child, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // add inheritance now
            InheritanceInfo info = new InheritanceInfo(parent, true, true);
            client.addDocumentInheritanceInfo(securityToken, 0, child, info);

            Document doc = framedGraph.query().has(Document.URI, child).vertices(Document.class).iterator().next();
            // verify inheritance
            assertEquals(1, doc.getInheritanceInfoList().size());
            assertEquals(parent, doc.getInheritanceInfoList().get(0).getParentUri());

            // verify derivedFrom edge
            Edge derivedFrom = doc.asVertex().getEdges(Direction.IN, DerivedFrom.LABEL).iterator().next();
            verifyEdgeCommonProperty((DerivedFrom) framedGraph.frame(derivedFrom, DerivedFrom.class), DerivedFrom.LABEL);

            // verify ageOff edge
            Edge edge = doc.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator().next();
            AgeOff ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
            verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // descendant
            doc = framedGraph.query().has(Document.URI, gchild).vertices(Document.class).iterator().next();
            edge = doc.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator().next();
            ageOff = (AgeOff) framedGraph.frame(edge, AgeOff.class);
            verifyEdgeCommonProperty(ageOff, AgeOff.LABEL);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test6AddDocAgeOffRuleInheritanceOldestEdge() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date oldDate = Utils.getCurrentDate();
            Date date = new Date(Utils.getCurrentDate().getTime() - 10 * 24 * 60 * 60 * 1000);
            long ruleId = ageOffRuleIds[1];
            AgeOffMapping mapping = new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date));

            // setup first
            int number = DOCUMENT_COUNT + 14;
            String parent = DOCUMENT_URI + "parent/" + number;
            String parent2 = DOCUMENT_URI + "parent/" + (number + 1);
            String child = DOCUMENT_URI + "child/" + number;
            String gchild = DOCUMENT_URI + "g-child/" + number;
            String g2child = DOCUMENT_URI + "g2-child/" + number;
            String gchild2 = DOCUMENT_URI + "g-child/" + (++number);

            // add parent
            List<AgeOffMapping> mappingList = new ArrayList<AgeOffMapping>();
            mappingList.add(new AgeOffMapping(ruleId, Utils.convertDate2DateTime(oldDate)));
            client.addDocument(securityToken, parent, null, mappingList);

            // add parent2
            mappingList = new ArrayList<AgeOffMapping>();
            mappingList.add(new AgeOffMapping(ruleId, Utils.convertDate2DateTime(date)));
            client.addDocument(securityToken, parent2, null, mappingList);

            // add child
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);
            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, child, parents, null);

            // add grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild, parents, null);

            // add another grandChild
            inherit = new InheritanceInfo(child, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, gchild2, parents, null);


            // add grand-grand Child
            inherit = new InheritanceInfo(gchild, true, true);
            parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit);
            client.addDocument(securityToken, g2child, parents, null);

            InheritanceInfo info = new InheritanceInfo(parent2, true, true);
            // add the rule explicitly to child
            client.addDocumentInheritanceInfo(securityToken, 0, child, info);

            // verify document
            Vertex document = graph.query().has(Document.URI, child).vertices().iterator().next();
            Document doc = (Document) framedGraph.frame(document, Document.class);
            // 2 inheritance
            assertEquals(2, doc.getInheritanceInfoList().size());
            // 2 derivedFrom edge
            Iterator<Edge> ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            AgeOff ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());

            // g-child should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, gchild).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g-child-2 should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, gchild2).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

            // g2-child should still have one edge with updated relevantdate
            document = graph.query().has(Document.URI, g2child).vertices().iterator().next();
            ageOffIt = document.getEdges(Direction.IN, AgeOff.LABEL).iterator();
            assertTrue(ageOffIt.hasNext());
            ageOff = (AgeOff) framedGraph.frame(ageOffIt.next(), AgeOff.class);
            assertEquals(ruleId, ageOff.getRuleId());
            assertFalse(ageOffIt.hasNext());
            assertEquals(date, ageOff.getAgeOffRelevantDateTime());

        } finally {
            pool.returnToPool(client);
        }
    }

    private void verifyResetInheritanceInfo(Document document, String parent) {
        List<InheritanceInfo> inheritanceInfos = document.getInheritanceInfoList();
        // inheritanceInfo should be reset
        for (InheritanceInfo info : inheritanceInfos) {
            if (parent.equals(info.parentUri)) {
                assertFalse(info.inheritParentAgeOff);
                assertFalse(info.trackParentAgeOff);
            }
        }
    }

    private void verifyRemovedAgeOffInheritance(String child, String parent) {
        Document document = framedGraph.query().has(Document.URI, child).vertices(Document.class).iterator().next();
        // the ageOff edge should be removed
        Iterator<Edge> it = document.asVertex().getEdges(Direction.IN, AgeOff.LABEL).iterator();
        assertFalse(it.hasNext());
    }

    private Date getMiniRelaventDateTime(Vertex parentDoc, long ruleId) {
        Iterator<Edge> it = parentDoc.query().labels(AgeOff.LABEL).edges().iterator();
        Date date = null;
        while (it.hasNext()) {
            AgeOff ageOff = (AgeOff) framedGraph.frame(it.next(), AgeOff.class);
            if (ageOff.getRuleId() == ruleId) {
                if (date == null) {
                    date = ageOff.getAgeOffRelevantDateTime();
                } else if (ageOff.getAgeOffRelevantDateTime().before(date)) {
                    date = ageOff.getAgeOffRelevantDateTime();
                }
            }
        }
        return date;
    }

    private void verifyVertexCommonProperty(BaseVertex vertex, String type) {
        assertEquals(vertex.getApplication(), Utils.getApplication(securityToken));
        assertEquals(vertex.getUser(), Utils.getUser(securityToken));
        assertNotEquals(vertex.getTimeStamp(), null);
        assertEquals(vertex.getType(), type);
    }

    private void verifyEdgeCommonProperty(BaseEdge edge, String label) {
        assertEquals(edge.getApplication(), Utils.getApplication(securityToken));
        assertEquals(edge.getUser(), Utils.getUser(securityToken));
        assertNotEquals(edge.getTimeStamp(), null);
        assertEquals(edge.asEdge().getLabel(), label);
    }

    @Test
    public void testDateTimeConverter() {
        Date date = Utils.getCurrentDate();

        DateTime dateTime = Utils.convertDate2DateTime(date);
        Date date2 = Utils.convertDateTime2Date(dateTime);

        assertEquals(date.getTime(), date2.getTime());
    }

    //    @Test
    public void test9DumpVertices() throws Exception {
        for (Vertex v : graph.getVertices()) {
            String type = v.getProperty(BaseVertex.Type);
            System.out.print("vertex = " + type);

            if (type.equals(Document.TYPE)) {
                System.out.print(" URI = " + v.getProperty(Document.URI));
                System.out.println(" DocumentId = " + v.getProperty(Document.DocumentId));
            } else if (type.equals(ezbake.services.provenance.graph.frames.AgeOffRule.TYPE)) {
                System.out.print(" Name = " + v.getProperty(ezbake.services.provenance.graph.frames.AgeOffRule.Name));
                System.out.println(" RuleId = " + v.getProperty(ezbake.services.provenance.graph.frames.AgeOffRule.RuleId));
            } else if (type.equals(PurgeEvent.TYPE)) {
                System.out.print(" Name = " + v.getProperty(PurgeEvent.Name));
                System.out.println(" PurgeId = " + v.getProperty(PurgeEvent.PurgeId));
            }
        }
    }

    //    @Test
    public void test9DumpEdges() throws Exception {
        for (Edge edge : graph.getEdges()) {
            String label = edge.getLabel();
            if (label.equals(DerivedFrom.LABEL)) {
//                System.out.print(label);
//                System.out.print("  In URI = " + edge.getVertex(Direction.IN).getProperty(Document.URI));
//                System.out.println("  OUT URI = " + edge.getVertex(Direction.OUT).getProperty(Document.URI));
            } else if (label.equals(AgeOff.LABEL)) {
                System.out.print(label);
                System.out.print(" ruleId = " + edge.getProperty(AgeOff.Rule));
                System.out.println("  AgeOffRelevantDateTime = " + edge.getProperty(AgeOff.AgeOffRelevantDateTime));
                String type = edge.getVertex(Direction.OUT).getProperty(BaseVertex.Type);
                if (type.equals(ezbake.services.provenance.graph.frames.AgeOffRule.TYPE)) {
                    System.out.print("  OUT ruleId = " + edge.getVertex(Direction.OUT).getProperty(ezbake.services.provenance.graph.frames.AgeOffRule.RuleId));
                } else if (type.equals(Document.TYPE)) {
                    System.out.print("  OUT documentId = " + edge.getVertex(Direction.OUT).getProperty(Document.URI));
                }
                System.out.println("  IN documentId = " + edge.getVertex(Direction.IN).getProperty(Document.URI));
            }
        }
    }

    //    @Test
    public void test9DumpKeys() throws Exception {
        for (TitanKey key : graph.getTypes(TitanKey.class)) {
            System.out.println("Key = " + key.toString());
        }
        for (TitanLabel key : graph.getTypes(TitanLabel.class)) {
            System.out.println("Label = " + key.toString());
        }
        for (String key : graph.getIndexedKeys(Vertex.class)) {
            System.out.println("Index vertex = " + key);
        }
        for (String key : graph.getIndexedKeys(Edge.class)) {
            System.out.println("Index edge = " + key);
        }
    }

}
