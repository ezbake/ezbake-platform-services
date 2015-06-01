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

package ezbake.security.persistence.model;

import static org.junit.Assert.*;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;
import ezbake.crypto.utils.CryptoUtil;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppPersistenceModelTest {

    private Logger log = LoggerFactory.getLogger(AppPersistenceModelTest.class);
    @BeforeClass
    public static void init() {
        
    }

    @Test
    public void testPrivateKeyEncryptionDecryption() throws AppPersistCryptoException {
        AppPersistenceModel model = new AppPersistenceModel();
        String dummyKey = "dummy";
        
        for(int i = 0; i <= 4; i++) {
            model.setId("0" + i);
            model.setPrivateKey(dummyKey);
            String rtv = model.getPrivateKey();
        
            log.debug("Private Key {}" , rtv);
        
            assertTrue(rtv,dummyKey.equals(rtv));
        }
    }
    
    @Test
    public void testPrivateKeyMutation() throws AppPersistCryptoException {
        AppPersistenceModel model = new AppPersistenceModel();
        String dummyKey = "dummy";
        model.setId("01");
        Mutation m = model.getPrivateKeyMutation("01", dummyKey.getBytes());
        log.debug(m.toString());
    }

    @Test
    public void testEquality() {
        AppPersistenceModel app1 = new AppPersistenceModel();
        AppPersistenceModel app2 = new AppPersistenceModel();
        app1.setId("test");
        app2.setId("test");
        Assert.assertEquals(app1, app2);

        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAppName("test");
        app2.setAppName("test");
        Assert.assertEquals(app1, app2);

        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAppDn("test");
        app2.setAppDn("test");
        Assert.assertEquals(app1, app2);

        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setFormalAuthorizations(Lists.newArrayList("o1","2"));
        app2.setFormalAuthorizations(Lists.newArrayList("o1","2"));
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setFormalAuthorizations(Lists.newArrayList("o1","2"));
        app2.setFormalAuthorizations(Lists.newArrayList("o1","4"));
        Assert.assertNotEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setFormalAuthorizations(Lists.<String>newArrayList());
        app2.setFormalAuthorizations(null);
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setFormalAuthorizations(null);
        app2.setFormalAuthorizations(Lists.<String>newArrayList());
        Assert.assertEquals(app1, app2);

        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setCommunityAuthorizations(Lists.newArrayList("a","b"));
        app2.setCommunityAuthorizations(Lists.newArrayList("a","b"));
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setCommunityAuthorizations(Lists.newArrayList("a","b"));
        app2.setCommunityAuthorizations(Lists.newArrayList("a","c"));
        Assert.assertNotEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setCommunityAuthorizations(Lists.<String>newArrayList());
        app2.setCommunityAuthorizations(null);
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setCommunityAuthorizations(null);
        app2.setCommunityAuthorizations(Lists.<String>newArrayList());
        Assert.assertEquals(app1, app2);

        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAdmins(Sets.newHashSet("a", "b"));
        app2.setAdmins(Sets.newHashSet("a", "b"));
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAdmins(Sets.newHashSet("a", "b"));
        app2.setAdmins(Sets.newHashSet("a", "c"));
        Assert.assertNotEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAdmins(Sets.<String>newHashSet());
        app2.setAdmins(null);
        Assert.assertEquals(app1, app2);
        app1 = new AppPersistenceModel();
        app2 = new AppPersistenceModel();
        app1.setAdmins(null);
        app2.setAdmins(Sets.<String>newHashSet());
        Assert.assertEquals(app1, app2);
    }
}
