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

package ezbake.groups.graph;

import ezbake.configuration.EzConfigurationLoaderException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TitanGraphConfigurationTest {
    @Test
    public void testStorageProperties() throws EzConfigurationLoaderException {
        Properties p = new Properties();
        p.setProperty("storage.backend", "berkeleyje");
        p.setProperty("storage.directory", "/tmp/ezgroups-test");
        p.setProperty("nottitan.directory", "/tmp/ezgroups-test");


        TitanGraphConfiguration gc = new TitanGraphConfiguration(p);
        Assert.assertTrue(gc.containsKey("storage.backend"));
        Assert.assertTrue(gc.containsKey("storage.directory"));
        Assert.assertTrue(!gc.containsKey("nottitan.directory"));
    }
}
