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

import ezbake.services.provenance.graph.AddDocumentHelper;
import ezbake.services.provenance.graph.Utils;
import ezbake.services.provenance.thrift.AddDocumentEntry;
import ezbake.services.provenance.thrift.AgeOffMapping;
import ezbake.services.provenance.thrift.InheritanceInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import java.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProvenanceAddDocumentsTest {

    @BeforeClass
    public static void setup() {

    }

    @AfterClass
    public static void cleanup() {

    }

    @Test
    public void test1AddDocumentsInOrder() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk/level/";
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

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);
        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(5, nodes.size());
    }


    @Test
    public void test4AddDocumentsReverseOrder() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk2/level/";
        for (int i = 5; i > 0; i--) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            if (i > 1) {
                String parentUri = uri + (i - 1);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            }
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);
        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(5, nodes.size());
    }

    @Test
    public void test1AddDocuments2Trees() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk1/level/";
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

        for (int i = 10; i > 5; i--) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            if (i > 6) {
                String parentUri = uri + (i - 1);
                inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            }
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);
        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(10, nodes.size());
    }


    @Test
    public void test2AddDocumentsExistingRoot() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk/level/";
        for (int i = 10; i < 15; i++) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + (i - 1);
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(5, nodes.size());
    }

    @Test
    public void test2AddDocumentsExistingRoot2Branchs() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk/level/";
        for (int i = 10; i < 15; i++) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + (i - 1);
            if (i == 10) {
                parentUri = uri + 9;
            }
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }
        for (int i = 15; i < 20; i++) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + (i - 5);
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String > nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(10, nodes.size());
    }

    @Test
    public void test3AddDocumentsCircle() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk3/level/";
        for (int i = 0; i < 3; i++) {
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + ((i + 1) % 3);
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);
        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String > nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(0, nodes.size());
    }

    @Test
    public void test3AddDocumentsCircleWithHead() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk3/level/";
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

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(2, nodes.size());
    }

    @Test
    public void test0AddDocumentsNoRelation() {
        Set<AddDocumentEntry> documents = new HashSet<>();
        String uri = "provenance://testdocuments/bulk/level/";
        for (int i = 0; i < 5; i++) {
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);
        assertEquals(5, nodes.size());
    }

    @Test
    public void test3AddDocumentsCircleWithTail() {
        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        String uri = "provenance://testdocuments/bulk3/level/";
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

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(0, nodes.size());
    }

    @Test
    public void test3AddDocumentsMixedTrees() {

        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        // first tree has circle with tail
        String uri = "provenance://testdocuments/bulk3/level/";
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
            Set<InheritanceInfo> inheritanceInfos = new HashSet<>();
            String parentUri = uri + (i - 1);
            if (i == 10) {
                parentUri = uri + 9;
            }
            inheritanceInfos.add(new InheritanceInfo(parentUri, true, true));
            AddDocumentEntry entry = new AddDocumentEntry(uri + i);
            entry.setParents(inheritanceInfos);

            documents.add(entry);
        }

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(5, nodes.size());
    }

    @Test
    public void test5AddDocumentsNotTrees() {

        AgeOffMapping ageOffMapping = new AgeOffMapping(1, Utils.convertDate2DateTime(new Date()));
        List<AgeOffMapping> ageOffRules = new ArrayList<AgeOffMapping>();
        ageOffRules.add(ageOffMapping);

        Set<AddDocumentEntry> documents = new HashSet<>();

        // first tree has circle with tail
        String uri = "provenance://testdocuments/bulk5/level/";

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

        Map<String, AddDocumentEntry> documentsMap = buildMap(documents);

        AddDocumentHelper helper = new AddDocumentHelper(null, null);
        List<String> nodes = helper.getValidDocumentEntriesInOrder(documentsMap);

        assertEquals(9, nodes.size());
    }

    private Map<String, AddDocumentEntry> buildMap(Set<AddDocumentEntry> documents) {
        Map<String, AddDocumentEntry> documentsMap = new HashMap<>();
        for (AddDocumentEntry document : documents) {
            String uri = document.getUri();
            if(documentsMap.containsKey(uri)) {
                documentsMap.get(uri).getParents().addAll(document.getParents());
            } else {
                documentsMap.put(document.getUri(), document);
            }
        }
        return documentsMap;
    }
}
