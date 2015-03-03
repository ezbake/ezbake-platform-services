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

package ezbake.deployer.publishers;

import com.google.inject.Inject;
import ezbake.common.ssl.SSLContextException;
import ezbake.crypto.utils.EzSSL;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.SSLCertsService;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;

/**
 * Reads in a tar file of Certificates from Security Service and returns them
 * as a list of CertDataEntry objects.
 *
 * @author ehu
 */
public class SecurityServiceClient implements SSLCertsService {
    /**
     * Standard logger
     */
    private static Logger log = LoggerFactory.getLogger(SecurityServiceClient.class);

    /**
     * Configuration
     */
    private EzDeployerConfiguration config;

    @Inject
    public SecurityServiceClient(EzDeployerConfiguration config) {
        this.config = config;
    }

    static {
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(new javax.net.ssl.HostnameVerifier() {
            /**
             * Currently hostname is not part of our certificates, so add this to resolve the conflict.
             */
            public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
                return true;
            }
        });
    }

    /**
     * The applicationId is part of the REST call to the Security Service.
     * Returns an empty list if no certificates were found or throws exception if
     * something went wrong with Security Service communication.
     */
    @Override
    public List<ArtifactDataEntry> get(String applicationId, String securityId) throws DeploymentException {
        try {
            ArrayList<ArtifactDataEntry> certList = new ArrayList<ArtifactDataEntry>();
            String endpoint = config.getSecurityServiceBasePath() + "/registrations/" + securityId + "/download";
            URL url = new URL(endpoint);
            HttpsURLConnection urlConn = openUrlConnection(url);
            urlConn.connect();

            // Read in tar file of certificates into byte array
            BufferedInputStream in = new BufferedInputStream(urlConn.getInputStream());
            // Create CertDataEntry list from tarFile byte array
            TarArchiveInputStream tarIn = new TarArchiveInputStream(in);
            TarArchiveEntry entry = tarIn.getNextTarEntry();
            while (entry != null) {
                if (!entry.isDirectory()) {
                    ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    IOUtils.copy(tarIn, bout);
                    byte[] certData = bout.toByteArray();
                    String certFileName = entry.getName().substring(entry.getName().lastIndexOf("/") + 1, entry.getName().length());
                    TarArchiveEntry tae = new TarArchiveEntry(Files.get(SSL_CONFIG_DIRECTORY, securityId, certFileName));
                    ArtifactDataEntry cde = new ArtifactDataEntry(tae, certData);
                    certList.add(cde);
                    bout.close();
                }

                entry = tarIn.getNextTarEntry();
            }
            tarIn.close();

            return certList;
        } catch (Exception e) {
            log.error("Unable to download certificates from security service.", e);
            throw new DeploymentException("Unable to download certificates from security service. " + e.getMessage());
        }
    }

    protected HttpsURLConnection openUrlConnection(URL endpoint) throws IOException, SSLContextException {

        SSLContext sslContext = EzSSL.getSSLContext(config.getEzConfiguration());

        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });
        return (HttpsURLConnection) endpoint.openConnection();
    }
}
