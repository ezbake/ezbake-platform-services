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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.services.centralPurge.thrift.ezCentralPurgeServiceConstants;
import ezbake.services.provenance.graph.Utils;
import ezbake.services.provenance.thrift.*;
import org.apache.commons.collections.CollectionUtils;
import org.codehaus.plexus.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.apache.commons.lang3.time.StopWatch;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProvenanceServiceTestReal {
    private static ThriftClientPool pool;

    private static EzSecurityToken securityToken = ThriftTestUtils.generateTestSecurityToken("U");
    private static EzSecurityToken adminSecurityToken = ezbake.security.test.MockEzSecurityToken.getMockAppToken("EzCentralPurgeService", "Principle");

    private static final String APP_NAME = "common_services";
    private static final String SERVICE_NAME = "EzProvenanceService";

    private static final String AGE_OFF_RULE_NAME = "age off rule #5";
    private static final int AGE_OFF_RULE_COUNT = 10;
    private static long[] ageOffRuleIds = new long[AGE_OFF_RULE_COUNT];

    private static final String DOCUMENT_URI = "provenance://testdocument/5/";
    private static final int DOCUMENT_COUNT = 10;
    private static long[] documentIds = new long[DOCUMENT_COUNT * 3];
    private static long[] purgeIds = new long[DOCUMENT_COUNT * 3];

    @BeforeClass
    public static void init() throws Exception {
        EzConfiguration configuration = new EzConfiguration(new DirectoryConfigurationLoader(), new ClasspathConfigurationLoader());
        Properties properties = configuration.getProperties();
        properties.setProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, APP_NAME);
        //We need Zookeeper too
        properties.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "192.168.50.105:2181");
        //Still create our client pool like we would for real
        pool = new ThriftClientPool(properties);

        // set purge security id if service running
        String purgeApp = pool.getSecurityId(ezCentralPurgeServiceConstants.SERVICE_NAME);
        if (StringUtils.isNotEmpty(purgeApp)) {
            ezbake.security.client.EzSecurityTokenWrapper wrapper = new ezbake.security.client.EzSecurityTokenWrapper(adminSecurityToken);
            wrapper.setSecurityId(purgeApp);
            adminSecurityToken = wrapper;
        }
    }

    @AfterClass
    public static void cleanup() {
        //Shutdown our client connections and the services
        pool.close();
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
                assertTrue(id > 0);
                ageOffRuleIds[i - 1] = id;
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

    @Test
    public void test2CountAgeOffRules() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            int count = client.countAgeOffRules(securityToken);
            assertTrue(count >= AGE_OFF_RULE_COUNT);
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
            assertTrue(rules.size() >= AGE_OFF_RULE_COUNT);

            int totalCount = rules.size();

            // paging
            int limit = 100;
            int page = 1;

            do {
                int pageStartIndex = (page - 1) * limit;
                int expectedCount = (totalCount - pageStartIndex) > limit ? limit : (totalCount - pageStartIndex);
                rules = client.getAllAgeOffRules(securityToken, limit, page);
                assertEquals(rules.size(), expectedCount > 0 ? expectedCount : 0);

                page++;
            } while (rules.size() > 0);

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
            int id = 999999999;
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
                AgeOffRule rule = client.getAgeOffRule(securityToken, name);
                assertEquals(duration, rule.getRetentionDurationSeconds());
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
    public void test2AddDocument() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Date date = Utils.getCurrentDate();
            Date date2 = new Date(Utils.getCurrentDate().getTime() - 7 * 24 * 60 * 60 * 1000);

            StopWatch watch = new StopWatch();
            long timing = 0;

            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                String uri = DOCUMENT_URI + "parent/" + i;
                AgeOffMapping ageOffMapping = new AgeOffMapping();
                long ruleId = ageOffRuleIds[i % 10];
                ageOffMapping.setRuleId(ruleId);
                ageOffMapping.setAgeOffRelevantDateTime(Utils.convertDate2DateTime(date));

                List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
                rules.add(ageOffMapping);

                watch.start();
                long docId = client.addDocument(securityToken, uri, null, rules);
                timing += watch.getNanoTime();
                watch.reset();

                assertTrue(docId > 0);
                documentIds[i] = docId;

                DocumentInfo info = client.getDocumentInfo(securityToken, docId, null);
                assertEquals(docId, info.getDocumentId());
                assertEquals(0, info.getParentsSize());
                assertEquals(1, info.getAgeOffInfoSize());
                assertEquals(ruleId, info.getAgeOffInfo().get(0).getRuleId());
            }

            System.out.println("addDocument took " + timing / DOCUMENT_COUNT / 1000000);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test3AddDocumentRuleNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "another_doc";

            AgeOffMapping ageOffMapping = new AgeOffMapping(999999999999L, Utils.convertDate2DateTime(new Date()));
            List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
            rules.add(ageOffMapping);

            client.addDocument(securityToken, uri, null, rules);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceAgeOffRuleNotFoundException.class)
    public void test3AddDocumentRuleNotFound2() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "another_doc";

            AgeOffMapping ageOffMapping = new AgeOffMapping(999999999999L, Utils.convertDate2DateTime(new Date()));
            List<AgeOffMapping> rules = new ArrayList<AgeOffMapping>();
            rules.add(ageOffMapping);

            client.addDocument(securityToken, uri, null, rules);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test(expected = ProvenanceParentDocumentNotFoundException.class)
    public void test3AddDocumentParentNotFound() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "another_doc";
            String parentUri1 = DOCUMENT_URI + "not_such_doc1";
            String parentUri2 = DOCUMENT_URI + "not_such_doc2";
            InheritanceInfo inherit1 = new InheritanceInfo(parentUri1, true, true);
            InheritanceInfo inherit2 = new InheritanceInfo(parentUri2, true, true);

            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit1);
            parents.add(inherit2);

            client.addDocument(securityToken, uri, parents, null);
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test3AddDocumentParentNotFound2() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "another_doc";
            String parentUri1 = DOCUMENT_URI + "not_such_doc1";
            String parentUri2 = DOCUMENT_URI + "not_such_doc2";
            InheritanceInfo inherit1 = new InheritanceInfo(parentUri1, true, true);
            InheritanceInfo inherit2 = new InheritanceInfo(parentUri2, true, true);

            List<InheritanceInfo> parents = new ArrayList<InheritanceInfo>();
            parents.add(inherit1);
            parents.add(inherit2);

            client.addDocument(securityToken, uri, parents, null);
        } catch (ProvenanceParentDocumentNotFoundException ex) {
            assertEquals(2, ex.getParentUrisSize());
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
            StopWatch watch = new StopWatch();
            long timing = 0;

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

                watch.start();
                long docId = client.addDocument(securityToken, uri, parents, ageOffMappingList);
                timing += watch.getNanoTime();
                watch.reset();

                assertTrue(docId > 0);
                documentIds[DOCUMENT_COUNT + i] = docId;


                DocumentInfo info = client.getDocumentInfo(securityToken, docId, null);
                assertEquals(docId, info.getDocumentId());
                assertEquals(1, info.getParentsSize());
                assertEquals(2, info.getAgeOffInfoSize());
            }

            System.out.println("addDocument took " + timing / DOCUMENT_COUNT / 1000000);

        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4AddDocumentWithParents2() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            StopWatch watch = new StopWatch();
            long timing = 0;

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

                watch.start();
                long docId = client.addDocument(securityToken, uri, parents, ageOffMappingList);
                timing += watch.getNanoTime();
                watch.reset();

                assertTrue(docId > 0);
                documentIds[DOCUMENT_COUNT * 2 + i] = docId;
            }

            System.out.println("addDocument took " + timing / DOCUMENT_COUNT / 1000000);

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

            StopWatch watch = new StopWatch();
            watch.start();
            Map<String, AddDocumentResult> results = client.addDocuments(securityToken, documents, null);
            watch.stop();
            System.out.println(String.format("add %d documents take %d ms", size, watch.getNanoTime() / 1000000));

            assertEquals(size, results.size());

            for (int i = 1; i < 50; i++) {
                DocumentInfo document = client.getDocumentInfo(securityToken, 0, uri + i);
                assertEquals(AddDocumentStatus.SUCCESS, results.get(uri + i).getStatus());
            }

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

            int size = 200;
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

            int size = 200;
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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
            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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

            AgeOffMapping ageOffMapping = new AgeOffMapping(ageOffRuleIds[1], Utils.convertDate2DateTime(new Date()));
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

    @Test(expected = ProvenanceCircularInheritanceNotAllowedException.class)
    public void test5AddDocAgeOffRuleInheritanceCircularInheritance() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "parent/0";
            String parent = DOCUMENT_URI + "g-child/0";
            InheritanceInfo inherit = new InheritanceInfo(parent, true, true);

            client.addDocumentInheritanceInfo(securityToken, 0, uri, inherit);
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
            assertEquals(15, result.getDerivedDocs().size());
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
            assertTrue(id > 0);
            purgeIds[0] = id;
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
            assertEquals(10, result.getDocumentUris().size());
            assertEquals(5, result.getDocumentUrisNotFound().size());

            assertEquals(purgeIds[0], result.getId());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5GetAllPurgeIds() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<Long> result = client.getAllPurgeIds(adminSecurityToken);
            assertTrue(result.size() > 0);
            assertTrue(result.contains(purgeIds[0]));
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
            PurgeInfo info = client.getPurgeInfo(adminSecurityToken, purgeIds[0]);
            assertEquals(resolved, info.isResolved());

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
                // used by following test case
                if (i == 1) {
                    ageoffDocIds = new HashSet<Long>(docIds);
                }
            }
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

            // ageOff the same rule should return empty docIds now
            long ruleId = ageOffRuleIds[0];
            Date effectiveTime = Utils.getCurrentDate();
            AgeOffInitiationResult result = client.startAgeOffEvent(adminSecurityToken, ruleId, Utils.convertDate2DateTime(effectiveTime));
            assertEquals(0, result.getAgeOffDocumentIds().size());
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
    public void test4GetUriFromBitPosition2() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            List<Long> docIds = new ArrayList<Long>();
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                docIds.add(documentIds[i]);
            }

            PositionsToUris result = client.getDocumentUriFromId(securityToken, docIds);
            assertEquals(DOCUMENT_COUNT, result.getMappingSize());
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                assertEquals(DOCUMENT_URI + "parent/" + i, result.getMapping().get(documentIds[i]));
            }
            assertEquals(0, result.getUnfoundPositionListSize());
            assertEquals(new ArrayList<Long>(), result.getUnfoundPositionList());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test4GetConvertedUrisFromIds() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            Set<Long> docIds = new HashSet<Long>();
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

    @Test
    public void test5RemoveDocAgeOffRuleInheritanceOldestRelevantDate() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String child = DOCUMENT_URI + "child/1";
            String parent = DOCUMENT_URI + "parent/1";
            client.removeDocumentAgeOffRuleInheritance(securityToken, 0, child, 0, parent);

            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(1, info.getAgeOffInfo().size());
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

            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(0, info.getAgeOffInfo().size());
        } finally {
            pool.returnToPool(client);
        }
    }

    @Test
    public void test5RemoveExplicitAgeOffRuleNotOnly() throws Exception {
        ProvenanceService.Client client = pool.getClient(APP_NAME, SERVICE_NAME, ProvenanceService.Client.class);

        try {
            String uri = DOCUMENT_URI + "parent/4";
            String child = DOCUMENT_URI + "child/4";
            long ruleId = ageOffRuleIds[4];

            client.removeDocumentExplicitAgeOffRule(securityToken, 0, uri, ruleId);

            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(1, info.getAgeOffInfo().size());

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

            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(0, info.getAgeOffInfo().size());

            info = client.getDocumentInfo(securityToken, 0, gchild);
            assertEquals(0, info.getAgeOffInfo().size());

            info = client.getDocumentInfo(securityToken, 0, g2child);
            assertEquals(0, info.getAgeOffInfo().size());

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
            DocumentInfo info = client.getDocumentInfo(securityToken, 0, parent);
            DocumentAgeOffInfo ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // child should have edge too
            info = client.getDocumentInfo(securityToken, 0, child);
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g-child should still have edge
            info = client.getDocumentInfo(securityToken, 0, gchild);
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

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
            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(2, info.getAgeOffInfo().size());
            DocumentAgeOffInfo ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());

            // g-child should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g-child-2 should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild2);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g2-child should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, g2child);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

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
            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            assertEquals(2, info.getAgeOffInfo().size());
            DocumentAgeOffInfo ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());

            // g-child should still have one edge with origin relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g-child-2 should still have one edge with origin relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild2);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g2-child should still have one edge with origin relevantdate
            info = client.getDocumentInfo(securityToken, 0, g2child);
            assertEquals(1, info.getAgeOffInfo().size());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

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

            inherit = new InheritanceInfo(parent2, true, true);
            // add the rule explicitly to child
            client.addDocumentInheritanceInfo(securityToken, 0, child, inherit);

            // verify document
            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            // 2 inheritance
            assertEquals(2, info.getParentsSize());
            // 2 ageOff
            assertEquals(2, info.getAgeOffInfoSize());
            DocumentAgeOffInfo ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            ageOff = info.getAgeOffInfo().get(1);
            assertEquals(ruleId, ageOff.getRuleId());

            // g-child should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild);
            assertEquals(1, info.getAgeOffInfoSize());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g-child-2 should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, gchild2);
            assertEquals(1, info.getAgeOffInfoSize());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // g2-child should still have one edge with updated relevantdate
            info = client.getDocumentInfo(securityToken, 0, g2child);
            assertEquals(1, info.getAgeOffInfoSize());
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

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
            inherit = new InheritanceInfo(parent, true, true);
            client.addDocumentInheritanceInfo(securityToken, 0, child, inherit);

            DocumentInfo info = client.getDocumentInfo(securityToken, 0, child);
            // verify inheritance
            assertEquals(1, info.getParentsSize());
            assertTrue(info.getParents().get(0).containsValue(parent));
            // verify ageOff edge
            assertEquals(1, info.getAgeOffInfoSize());
            DocumentAgeOffInfo ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));

            // descendant
            info = client.getDocumentInfo(securityToken, 0, gchild);
            ageOff = info.getAgeOffInfo().get(0);
            assertEquals(ruleId, ageOff.getRuleId());
            assertEquals(date, Utils.convertDateTime2Date(ageOff.getAgeOffRelevantDateTime()));
        } finally {
            pool.returnToPool(client);
        }
    }

}
