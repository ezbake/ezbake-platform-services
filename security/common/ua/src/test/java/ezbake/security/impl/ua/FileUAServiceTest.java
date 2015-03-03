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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import ezbake.security.api.ua.Community;
import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

/**
 * User: jhastings
 * Date: 4/29/14
 * Time: 12:00 PM
 */
public class FileUAServiceTest extends FileUABase {
    private static final String dn = "John Snow";
    private static final String nonUser = "Non User";

    private FileUAService service;

    @Before
    public void setUp() throws FileNotFoundException {
        service = new FileUAService(new File(userFile));
    }

    @Test
    public void testLoadStream() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(FileUAService.userFile);
        Map<String, User> s = FileUAService.loadJSON(is);
        Assert.assertFalse(s.isEmpty());
        Assert.assertEquals(3, s.size());
    }

    @Test
    public void assertNonUser() {
        Assert.assertFalse(service.assertUser(nonUser));
    }

    @Test
    public void assertValidUser() {
        Assert.assertTrue(service.assertUser(dn));
    }

    @Test(expected=UserNotFoundException.class)
    public void unknownUserThrowsException() throws UserNotFoundException {
        service.getUser(nonUser);
    }

    @Test
    public void getUser() throws UserNotFoundException {
        User user = service.getUser(dn);

        Set<String> formalAccess = ImmutableSortedSet.of("J", "O", "H", "N");

        Map<String, String> emails = new TreeMap<String, String>();
        emails.put("work", "jsnow@cb.com");
        emails.put("home", "ghost@dire.com");

        Assert.assertNotNull(user);
        Assert.assertEquals("John Snow", user.getName());
        Assert.assertEquals("jslongclaw", user.getUid());
        Assert.assertEquals("Stark", user.getCompany());
        Assert.assertEquals("123-456-7890", user.getPhoneNumber());
        Assert.assertEquals(emails, new TreeMap<>(user.getEmails()));


        Assert.assertNotNull(user.getAuthorizations());
        Assert.assertEquals("high", user.getAuthorizations().getLevel());
        Assert.assertArrayEquals(formalAccess.toArray(), user.getAuthorizations().getAuths().toArray());
        Assert.assertEquals("USA", user.getAuthorizations().getCitizenship());
        Assert.assertEquals("abc", user.getAuthorizations().getOrganization());
        Assert.assertEquals(Sets.newHashSet("ca_topic"), user.getAuthorizations().getCommunityAuthorizations());

        Assert.assertNotNull(user.getCommunities());
        Assert.assertEquals(1, user.getCommunities().size());
        Community community = user.getCommunities().get(0);

        Assert.assertEquals("starkies", community.getCommunityName());
        Assert.assertEquals("familiar", community.getCommunityType());
        Assert.assertEquals("dire", community.getOrganization());
        Assert.assertArrayEquals(new String[]{"TopicA"}, community.getTopics().toArray());
        Assert.assertArrayEquals(new String[]{"Region1"}, community.getRegions().toArray());
        Assert.assertEquals(0, community.getGroups().size());
    }

    @Test(expected=UserNotFoundException.class)
    public void getUserProjectsNoUser() throws UserNotFoundException {
        service.getUserGroups(nonUser);
    }

    @Test
    public void getUserProjects() throws UserNotFoundException {
        Map<String, List<String>> u = service.getUserGroups(dn);
        Map<String, List<String>> groups = new TreeMap<String, List<String>>();
        groups.put("EzBake", Arrays.asList("Core"));
        groups.put("_Ez_internal_project_", Arrays.asList("_Ez_administrator"));
        groups.put("NightsWatch", Arrays.asList("castleblack", "wolves"));


        Assert.assertEquals(groups, new TreeMap<String, List<String>>(u));
    }
}