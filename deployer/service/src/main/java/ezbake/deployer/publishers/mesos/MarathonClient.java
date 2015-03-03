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

package ezbake.deployer.publishers.mesos;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

/**
 * This class serves as a client to a running instance of Marathon. Currently it
 * has only one method, which is to launch a new Application.
 *
 * @author ehu
 */
public class MarathonClient {
    /**
     * Standard logger
     */
    private static Logger log = LoggerFactory.getLogger(MarathonClient.class);

    /**
     * Host address of Marathon, ie) http://localhost:8080
     */
    private String marathonHost;

    /**
     * Constructs a new MarathonClient given host address
     *
     * @param marathonHost - host and port, ie) http://localhost:8080
     */
    public MarathonClient(String marathonHost) {
        this.marathonHost = marathonHost;
    }

    /**
     * Launches a new app on the Mesos cluster through a REST call to Marathon.
     *
     * @param id           - unique id of the app
     * @param command      - the command that each Mesos slave node shall execute
     * @param numCpu       - number of CPU's requested
     * @param sizeMem      - size in megabytes of main memory requested
     * @param numInstances - number of instances requested
     * @param uris         - the list of URLs that has any asset files that need to
     *                     be downloaded by Mesos slaves
     * @throws JSONException
     * @throws MarathonLaunchFailureException
     */
    public void launchApp(String id, String command, int numCpu, int sizeMem, int numInstances, String[] uris)
            throws MarathonLaunchFailureException, JSONException {
        ClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create(clientConfig);
        try {


            JSONObject params = new JSONObject();
            params.put("id", id);
            params.put("cpus", numCpu);
            params.put("mem", sizeMem);
            params.put("instances", numInstances);
            params.put("cmd", command);

            if (uris != null) {
                JSONArray jarray = new JSONArray();
                for (String uri : uris)
                    jarray.put(uri);
                params.put("uris", jarray);
            }

            Log.info("Marathon REST call: " + params);

            String paramsJson = params.toString();
            System.err.println(paramsJson);

            WebResource service = client.resource(marathonHost);

            ClientResponse response = service.path("v1")
                    .path("apps")
                    .path("start")
                    .type(MediaType.APPLICATION_JSON)
                    .post(ClientResponse.class, paramsJson);

            int status = response.getStatus();
            if (status >= 300) {
                String msg = "Received status code " + status + " from Marathon REST call.\n" + response.getEntity(String.class);
                throw new MarathonLaunchFailureException(msg);
            }
        } finally {
            try {
                client.destroy();
            } catch (Exception e) {
                log.error("Error destrying jersey client for marathon", e);
            }
        }
    }

    public void validate() {
        // TODO test if the marathon client is properly configured.
    }
}
