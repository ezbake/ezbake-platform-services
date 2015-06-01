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

package ezbake.intent.query.processor;

import ezbake.configuration.EzConfiguration;
import ezbake.ins.thrift.gen.AppService;
import ezbake.ins.thrift.gen.InternalNameService;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class InsService {

    private InsClient inscli = null;
    private Properties configuration;

    public InsService(final EzConfiguration configuration) {
        inscli = new InsClient(configuration);
        this.configuration = configuration.getProperties();
    }

    public String getAppName(String intentname) {
        String appName = null;
        InternalNameService.Client cli = inscli.getThriftClient();
        try {
            Set<AppService> apps = cli.appsThatSupportIntent(intentname);

            // TODO seems we are losing information?
            for (AppService appservice : apps) {
                appName = appservice.getApplicationName();
            }

//		} catch (ApplicationNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            inscli.returnToPool(cli);
        }

        return appName;
    }

    public List<String> getAppNames(String intentName) throws TException {
        InternalNameService.Client client = inscli.getThriftClient();
        List<String> appNames = new ArrayList<>();

        try {
            Set<ezbake.ins.thrift.gen.AppService> apps = client.appsThatSupportIntent(intentName);

            for (AppService app : apps) {
                appNames.add(app.getApplicationName());
            }

            return appNames;
        } finally {
            inscli.returnToPool(client);
        }
    }
}
