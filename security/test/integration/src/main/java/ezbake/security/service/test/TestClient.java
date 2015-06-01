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

package ezbake.security.service.test;

import com.google.common.io.Closeables;
import ezbake.base.thrift.*;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityTokenUtils;
import ezbake.security.thrift.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 12/9/13
 * Time: 12:04 PM
 */
public class TestClient {
	private static long expiry = 10 * 60 * 1000;
	
    enum RequestType {
        DN,
        PROXY_DN,
        User,
        App
    }

    @Option(name="--help", usage="print help message")
    public boolean help;

    @Option(name="-z", aliases="--zookeeper", usage="host:port of the zookeeper")
    private String zoo = "localhost:2181";

    @Option(name="-c", aliases="--config-dir", usage="ezconfiguration directory")
    private String config = "conf";

    @Option(name="-q", aliases="--query", usage="User or app to query security service for")
    private String dn = "CN=EzbakeClient,OU=42six,O=CSC,C=US";

    @Option(name="-r", aliases="--request-type")
    private RequestType request = RequestType.User;

    @Option(name="-o", aliases="--output-file", usage="File to write serialized (TBinaryProtocol) token")
    private String outputFile;

    @Option(name="-x", aliases="--security-id", usage="The security ID")
    private String appId = "SecurityClientTest";

    @Option(name="-y", aliases="--target-security-id", usage="Application this token should be valid for")
    private String target = "";

    @Option(name="-s", aliases="--ssl-dir", usage="The ssl directory")
    private String sslDir = "conf/ssl/SecurityClientTest";

    public void run() throws TException, EzSecurityTokenException, UserNotFoundException, IOException, AppNotRegisteredException {
        Properties config;
        try {
            config = new EzConfiguration(new DirectoryConfigurationLoader(new File(this.config).toPath())).getProperties();
        } catch (EzConfigurationLoaderException e) {
            try {
                config = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
            } catch (EzConfigurationLoaderException e1) {
                throw new RuntimeException("Unable to load EzConfiguration");
            }
        }

        if (config.get(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING) == null) {
            config.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, this.zoo);
        }
        if (config.get(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY) == null) {
            config.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, this.sslDir);
        }
        if(config.get(EzBakePropertyConstants.EZBAKE_SECURITY_ID) == null) {
            config.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, this.appId);
        }

        EzbakeSecurityClient client = new EzbakeSecurityClient(config);

        switch (this.request) {
            case User:
                EzSecurityToken usertoken = client.fetchTokenForProxiedUser(issuedTokenPrincipal(dn), this.target);
                if (this.outputFile != null) {
                    writeTokenToFile(this.outputFile, usertoken);
                }
                break;
            case App:
                EzSecurityToken appToken = client.fetchAppToken(dn);
                if (this.outputFile != null) {
                    writeTokenToFile(this.outputFile, appToken);
                }
                break;
            case DN:
                ProxyTokenRequest req = new ProxyTokenRequest();
                req.setX509(new X509Info(dn));
                req.setValidity(new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis() + 1000, ""));

                EzSecurity.Client c = client.getClient();
                ProxyTokenResponse principal = c.requestProxyToken(req);
                client.returnClient(c);

                System.out.println(principal.getToken());
                System.out.println(principal.getSignature());
                break;
            case PROXY_DN:
                ProxyTokenRequest proxyReq = new ProxyTokenRequest();
                proxyReq.setX509(new X509Info(dn));
                proxyReq.setValidity(new ValidityCaveats("EFE", "EzSecurity", System.currentTimeMillis()+1000, ""));
                proxyReq.getValidity().setIssuedTime(System.currentTimeMillis());

                EzSecurity.Client pc = client.getClient();
                ProxyTokenResponse presp = pc.requestProxyToken(proxyReq);
                client.returnClient(pc);

                System.out.println(presp.getToken());
                System.out.println(presp.getSignature());
                break;
        }
        Closeables.close(client, true);
    }

    private ProxyPrincipal issuedTokenPrincipal(String principal) throws TException {
        return new ProxyPrincipal(EzSecurityTokenUtils.serializeProxyUserTokenToJSON(
                new ProxyUserToken(new X509Info(dn), "EzSecurity", "", System.currentTimeMillis()+expiry)), "");
    }

    private static void writeTokenToFile(String file, TBase token) {
        if (file != null && file.equals("stdout")) {
            System.out.println(token);
        } else {
            OutputStream os = null;
            try {
                os = new FileOutputStream(new File(file));
                os.write(new TSerializer().serialize(token));
            } catch (FileNotFoundException e) {
                System.err.println("Output file not found: " + file + " error: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Error writing to output file: " + file + " error: " + e.getMessage());
            } catch (TException e) {
                System.err.println("Unable to serialize token to file: " + e.getMessage());
            } finally {
                if (os != null) {
                    try {
                        os.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        TestClient app = new TestClient();
        CmdLineParser cmd = new CmdLineParser(app);
        try {
            cmd.parseArgument(args);
            if (app.help == true) {
                cmd.printUsage(System.out);
                System.exit(1);
            }
            app.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmd.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }
}
