/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.persist;

import ezbake.persist.exception.EzPKeyError;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 8:37 AM
 */
public class TestFilePersist {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    EzPersist persist;
    @Before
    public void setUpFileSystem() throws IOException {
        String root = folder.getRoot().toString();
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test", "", "", "data")), "1");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test", "colf", "", "data")), "2");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test", "colf", "colq", "data")), "3");

        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test2", "", "", "data")), "1");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test2", "colf", "", "data")), "2");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "", "test2", "colf", "colq", "data")), "3");

        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "lookup", "table", "colf", "colq", "data")), "3");

        persist = new FilePersist(root);
    }

    @After
    public void cleanFileSystem() throws IOException {
        FileUtils.cleanDirectory(folder.getRoot());
    }

    @Test
    public void testRead() throws EzPKeyError {
        String row = "test";
        String colf = "colf";
        String colq = "colq";
        String rowVal = "1";
        String colfVal = "2";
        String colqVal = "3";

        Assert.assertEquals(rowVal, persist.read(row));
        Assert.assertEquals(colfVal, persist.read(row, colf));
        Assert.assertEquals(colqVal, persist.read(row, colf, colq));
    }

    @Test
    public void testReadTable() throws EzPKeyError {
        Assert.assertEquals("3", persist.read("table", "colf", "colq", "lookup"));
    }

    @Test(expected=EzPKeyError.class)
    public void testReadError() throws EzPKeyError {
        persist.read("nokey");
    }

    @Test(expected=EzPKeyError.class)
    public void testRowError() throws EzPKeyError {
        persist.row("nokey");
    }


    @Test
    public void testall() {
        List<Map<String, String>> expected = new ArrayList<Map<String, String>>();
        Map<String, String> expectedRow1 = new TreeMap<String, String>();
        expectedRow1.put("test:None:None", "1");
        expectedRow1.put("test:colf:None", "2");
        expectedRow1.put("test:colf:colq", "3");
        Map<String, String> expectedRow2 = new TreeMap<String, String>();
        expectedRow2.put("test2:None:None", "1");
        expectedRow2.put("test2:colf:None", "2");
        expectedRow2.put("test2:colf:colq", "3");
        expected.add(expectedRow1);
        expected.add(expectedRow2);
        List<Map<String, String>> vals = persist.all();
        Assert.assertTrue(vals.containsAll(expected));
        Assert.assertTrue(expected.containsAll(vals));
    }

    @Test
    public void testRow() throws EzPKeyError {
        Map<String, String> expectedRow1 = new HashMap<String, String>();
        expectedRow1.put("test:None:None", "1");
        expectedRow1.put("test:colf:None", "2");
        expectedRow1.put("test:colf:colq", "3");
        Assert.assertEquals(expectedRow1, persist.row("test"));
    }

    @Test
    public void testRowTable() throws EzPKeyError {
        Map<String, String> expectedRow = new HashMap<String, String>();
        expectedRow.put("table:colf:colq", "3");
        Assert.assertEquals(expectedRow, persist.row("table", "lookup"));

    }

    @Test
    public void testReadSlash() throws EzPKeyError {
        EzPersist slashpersist = new FilePersist(folder.getRoot().toString()+"/");
        Assert.assertEquals("1", slashpersist.read("test"));
    }
    @Test
    public void testRowSlash() throws EzPKeyError {
        EzPersist slashpersist = new FilePersist(folder.getRoot().toString()+"/");
        Map<String, String> expectedRow1 = new HashMap<String, String>();
        expectedRow1.put("test:None:None", "1");
        expectedRow1.put("test:colf:None", "2");
        expectedRow1.put("test:colf:colq", "3");
        Assert.assertEquals(expectedRow1, slashpersist.row("test"));
    }
    @Test
    public void testRowTableSlash() throws EzPKeyError {
        EzPersist slashpersist = new FilePersist(folder.getRoot().toString()+"/");
        Map<String, String> expectedRow = new HashMap<String, String>();
        expectedRow.put("table:colf:colq", "3");
        Assert.assertEquals(expectedRow, slashpersist.row("table", "lookup"));
    }

}
