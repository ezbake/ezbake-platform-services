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

package ezbake.security.impl.ua;

import ezbake.security.api.ua.SearchResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: jhastings
 * Date: 8/26/14
 * Time: 8:18 AM
 */
public class FileUASearchTest extends FileUABase {

    FileUASearch service;
    @Before
    public void setUp() throws FileNotFoundException {
        service = new FileUASearch(new File(userFile));
    }

    @Test
    public void exactMatchSearch() {
        String first = "John";
        String last = "Snow";
        String id = "John Snow";

        SearchResult result = service.search(first, last);
        Assert.assertFalse(result.isError());
        Assert.assertEquals(1, result.getData().size());

        List<String> users = new ArrayList<>(result.getData());
        Assert.assertEquals(id, users.get(0));
    }

    @Test
    public void partialMatchSearch() {
        String first = "Tyr*";
        String last = "Lannis*";
        String id = "Tyrion Lannister";

        SearchResult result = service.search(first, last);
        Assert.assertFalse(result.isError());
        Assert.assertEquals(1, result.getData().size());

        List<String> users = new ArrayList<>(result.getData());
        Assert.assertEquals(id, users.get(0));
    }

    @Test
    public void wildCardSearch() {
        String first = "*";
        String last = "**";
        String id = "";

        SearchResult result = service.search(first, last);
        Assert.assertFalse(result.isError());
        Assert.assertEquals(3, result.getData().size());
    }

    @Test
    public void usersInGroupTest() {
        SearchResult result = service.listGroupMembers("Core", "EzBake");
        Assert.assertFalse(result.isError());
        Assert.assertEquals(3, result.getData().size());


        SearchResult adminResult = service.listGroupMembers("_Ez_administrator", "_Ez_internal_project_");
        Assert.assertFalse(adminResult.isError());
        Assert.assertEquals(1, adminResult.getData().size());
    }
}
