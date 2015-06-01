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

package ezbake.security.persistence.impl;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.persistence.model.FileBackedRegistration;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * User: jhastings
 * Date: 12/18/14
 * Time: 3:25 PM
 */
public class FileJsonRegManagerTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testLoad() throws IOException, RegistrationException, SecurityIDNotFoundException {
        File appsFile = new File(folder.getRoot(), "apps.json");
        writeDefaultFile(appsFile);

        JsonFileRegManager manager =  setUpManager(appsFile);

        Assert.assertEquals(2, manager.all(null, null, null).size());
        AppPersistenceModel model = manager.getRegistration(null, "_Ez_Deployer", null, null);

        Assert.assertEquals("_Ez_Deployer", model.getId());
        Assert.assertEquals("CN=_Ez_Deployer", model.getAppDn());
        Assert.assertEquals("high", model.getAuthorizationLevel());
        Assert.assertEquals(Lists.newArrayList("A","B","C"), model.getFormalAuthorizations());
    }


    @Test
    public void testFileGetsUpdated() throws RegistrationException, FileNotFoundException {
        File appFile = new File(folder.getRoot(), "apps.json");
        JsonFileRegManager manager =  setUpManager(appFile);

        List<AppPersistenceModel> apps = Lists.newArrayList();
        AppPersistenceModel app1 = new AppPersistenceModel();
        app1.setId("appid1");
        app1.setAppName("Test App 1");
        app1.setOwner("tester");
        app1.setAuthorizationLevel("abc");
        app1.setFormalAuthorizations(Lists.newArrayList("A", "B", "C"));
        app1.setStatus(RegistrationStatus.PENDING);
        manager.register(app1);

        AppPersistenceModel app2 = new AppPersistenceModel();
        app2.setId("appid2");
        app2.setAppName("Test App 2");
        app2.setOwner("tester");
        app2.setAuthorizationLevel("abc");
        app2.setFormalAuthorizations(Lists.newArrayList("A", "B", "C"));
        app2.setStatus(RegistrationStatus.PENDING);
        manager.register(app2);

        List<FileBackedRegistration> fileApps = loadJson(appFile);
        apps.add(app2);
        apps.add(app1);

        Assert.assertEquals(apps.size(), fileApps.size());
    }

    @Test
    public void testConcurrentModification() throws FileNotFoundException, InterruptedException {

        int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final String appFile = new File(folder.getRoot(), "apps.json").getAbsolutePath();
        for (int i = 0; i < numThreads; ++i) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Properties properties = new Properties();
                    properties.setProperty(FileRegManager.REGISTRATION_FILE_PATH, appFile);
                    JsonFileRegManager manager = new JsonFileRegManager(properties);
                    String sid = Thread.currentThread().getName();
                    int id = Integer.parseInt(sid, 10);

                    // All threads register an app
                    AppPersistenceModel app = new AppPersistenceModel();
                    app.setId(sid);
                    app.setAppName("App " + sid);
                    app.setOwner("owner" + id);
                    try {
                        manager.register(app);
                    } catch (RegistrationException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            }, ""+i).start();
        }
        latch.await();
        List<FileBackedRegistration> fileApps = loadJson(new File(appFile));
        Assert.assertEquals(numThreads, fileApps.size());
    }

    private static JsonFileRegManager setUpManager(File testFile) {
        Properties properties = new Properties();
        properties.put(FileRegManager.REGISTRATION_FILE_PATH, testFile.getAbsolutePath());
        return new JsonFileRegManager(properties);
    }

    private static List<FileBackedRegistration> loadJson(File file) throws FileNotFoundException {
        return (List<FileBackedRegistration>) new Gson().fromJson(new FileReader(file), new TypeToken< List < FileBackedRegistration >>(){}.getType());
    }

    private static void writeDefaultFile(File path) throws IOException {
        FileUtils.writeStringToFile(path, "[{" +
                        "\"securityId\": \"_Ez_Deployer\"," +
                        "\"appDn\": \"CN=_Ez_Deployer\"," +
                        "\"level\": \"high\"," +
                        "\"authorizations\": [\"A\", \"B\", \"C\"]" +
                        "},{" +
                        "\"securityId\": \"_Ez_INS\"," +
                        "\"appDn\": \"CN=_Ez_INS\"," +
                        "\"level\": \"high\"," +
                        "\"authorizations\": [\"A\", \"B\", \"C\"]" +
                        "}]"
        );
    }
}
