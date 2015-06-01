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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.reverseproxy.thrift.EzReverseProxy;
import ezbake.reverseproxy.thrift.EzReverseProxyConstants;
import ezbake.reverseproxy.thrift.RegistrationInvalidException;
import ezbake.reverseproxy.thrift.RegistrationMismatchException;
import ezbake.reverseproxy.thrift.RegistrationNotFoundException;
import ezbake.reverseproxy.thrift.UpstreamServerRegistration;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.Language;
import ezbake.services.deploy.thrift.WebAppInfo;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Set;

/**
 * This class will hook up with the ezReverseProxy thrift service to register/un-register webapps.
 * <p/>
 * It will lazily connect to the service on first use.
 * <p/>
 * See the ezReverseProxy project for details.
 */
public class EzReverseProxyRegister {
    private static final Logger log = LoggerFactory.getLogger(EzReverseProxyRegister.class);

    private final EzDeployerConfiguration configuration;
    private final ThriftClientPool pool;
    private final String reverseProxyApplicationName;

    /**
     * Creates an instance of EzReverseProxy based on the configuration passed in.
     * This will only lazily connect to the service.
     *
     * @param configuration the ezConfiguration object to get the configuration from.
     */
    @Inject
    public EzReverseProxyRegister(EzDeployerConfiguration configuration, ThriftClientPool pool) {
        this.configuration = configuration;
        this.pool = pool;
        this.reverseProxyApplicationName = configuration.getReverseProxyApplicationName();
    }

    /**
     * Registers an application with the EzReverseProxy found via service discovery.
     * <p/>
     * If the UserFacingUrlPrefix is not specified one will be generated automatically from using the userfacingdomain
     * ezConfiguration property followed by a "/" then the application name given in the registration.  Unless you need
     * to customize the user facing url you probably shouldn't set the UserFacingUrlPrefix.
     *
     * @param registration - the registration to use for the application
     * @throws DeploymentException on any errors registering with the ReverseProxy.  See the ezReverseProxy project for
     *                             some details on what could cause some errors.
     */
    public void publish(UpstreamServerRegistration registration, String userFacingUrl) throws DeploymentException {
        try {
            autoSetMissing(registration, userFacingUrl);
            EzReverseProxy.Client client = getClient();
            try {
                log.info("Registering to EzReverseProxy {}", registration);
                client.addUpstreamServerRegistration(registration);
            } catch (RegistrationMismatchException e) {
                log.error("Reverse-proxy returned registration mismatch for: " + registration, e);
                throw new DeploymentException("Reverse-proxy: " + e.getMessage());
            } catch (RegistrationInvalidException e) {
                log.error("Reverse-proxy returned InvalidRegistration for: " + registration, e);
                throw new DeploymentException("Reverse-proxy: " + e.getMessage());
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    public void publish(DeploymentArtifact artifact, URL applicationURL) throws DeploymentException {

        if (artifact.getMetadata().getManifest().getWebAppInfo() == null) {
            throw new DeploymentException("WebApp requires to have the WebApp Info set");
        }
        UpstreamServerRegistration registration = new UpstreamServerRegistration();
        registration.setAppName(ArtifactHelpers.getServiceId(artifact));
        WebAppInfo webAppInfo = artifact.getMetadata().getManifest().getWebAppInfo();

        String port = ":443";
        if (artifact.getMetadata().getManifest().getArtifactInfo().getLanguage() == Language.NodeJs &&
                !webAppInfo.isWebsocketSupportDisabled()) {
            port = ":8443";
        }

        registration.setUpstreamHostAndPort(applicationURL.getHost() + port);
        registration.setSticky(webAppInfo.isStickySession());
        registration.setTimeout(webAppInfo.getTimeout());
        registration.setTimeoutTries(webAppInfo.getTimeoutRetries());
        registration.setUploadFileSize(webAppInfo.getUploadFileSize());
        registration.setDisableChunkedTransferEncoding(webAppInfo.isChunkedTransferEncodingDisabled());
        if (webAppInfo.isSetInternalWebUrl()) {
            registration.setUpstreamPath(webAppInfo.getInternalWebUrl());
        }
        if (webAppInfo.isSetHostname()) {
            String hostname = webAppInfo.getHostname();
            if (hostname.endsWith("/")) {
                hostname = hostname.substring(0, hostname.length() - 1);
            }
            String externalWebUrl = "";
            if (webAppInfo.isSetExternalWebUrl()) {
                externalWebUrl = webAppInfo.getExternalWebUrl();
                if (externalWebUrl.startsWith("/")) {
                    externalWebUrl = externalWebUrl.substring(1);
                }
            }
            registration.setUserFacingUrlPrefix(hostname + "/" + externalWebUrl);
        }

        publish(registration, webAppInfo.getExternalWebUrl());
    }

    /**
     * Since for OpenShift these will be mostly following the same pattern, might as well, just auto create
     * the UserFacingUrlPrefix and UpStreamPath if they are missing
     *
     * @param registration - the registration to set the UserFacingUrlPrefix and UpStreamPath
     */
    private void autoSetMissing(UpstreamServerRegistration registration, String userFacingUrl) {
        String prefix = Objects.firstNonNull(Strings.emptyToNull(userFacingUrl), registration.getAppName());
        if (!registration.isSetUserFacingUrlPrefix())
            registration.setUserFacingUrlPrefix(getUserFacingDomain() + "/" + prefix + "/");
        if (!registration.isSetUpstreamPath())
            registration.setUpstreamPath("");
    }

    /**
     * From a given appName attempts to remove all entries for the application.  This will generate the userfacing domain
     * via the EzConfiguration property and add on the given appName.
     *
     * @param appName the application to remove from EzReverseProxy
     * @throws DeploymentException on errors removing the entry from EzReverseProxy
     */
    public void unpublishByAppName(String appName, String userFacingUrl) throws DeploymentException {
        String prefix = Objects.firstNonNull(Strings.emptyToNull(userFacingUrl), appName);
        unpublishByURLPrefix(getUserFacingDomain() + "/" + prefix);
    }

    /**
     * Remove all entries for a given userFacingUrlPrefix.  Most likely you want the unpublishByAppName unless you
     * customized your urlFacingUrlPrefix.
     *
     * @param userFacingUrlPrefix the userFacingUrlPrefix in the reverse proxy to unregister
     * @throws DeploymentException on any error unregistering.  Please see the EReverseProxy for errors that can crop up
     */
    public void unpublishByURLPrefix(String userFacingUrlPrefix) throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                if (!userFacingUrlPrefix.endsWith("/")) {
                    userFacingUrlPrefix = userFacingUrlPrefix + "/";
                }
                log.info("removeReverseProxiedPath({})", userFacingUrlPrefix);
                client.removeReverseProxiedPath(userFacingUrlPrefix);
            } catch (RegistrationNotFoundException e) {
                log.error("Error with reverse-proxy service client", e);
                throw new DeploymentException("Reverse-proxy: " + e.getMessage());
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    /**
     * Remove only 1 instance of a UserFacingPrefix from the reverse proxy
     *
     * @param registration - the registration to use for the reverseProxy to remove
     * @throws DeploymentException on any errors.  Please see the EReverseProxy for errors that can crop up
     */
    public void removeGivenInstance(UpstreamServerRegistration registration, String userFacingUrl) throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                autoSetMissing(registration, userFacingUrl);
                client.removeUpstreamServerRegistration(registration);
            } catch (RegistrationNotFoundException e) {
                log.error("Error with reverse-proxy service client", e);
                throw new DeploymentException("Reverse-proxy: " + e.getMessage());
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    /**
     * Test if a given registration is already registered with the EzReverseProxy.
     * <p/>
     * Please see the EzReverseProxy project for more details.
     *
     * @param registration - the registration to use for the reverseProxy to check
     * @return true if already registered
     * @throws DeploymentException on any errors
     */
    public boolean isUpstreamServerRegistered(UpstreamServerRegistration registration, String userFacingUrl) throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                autoSetMissing(registration, userFacingUrl);
                return client.isUpstreamServerRegistered(registration);
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    /**
     * Test if a given appName is already registered with the reverse proxy.
     * The UserFacingUrlPrefix is auto generated from the UserFacingDomain EzConfiguration property and appName
     * <p/>
     * Please see the EzReverseProxy project for more details.
     *
     * @param appName the application to test against the EzReverseProxy
     * @return true if atleast one path already exist
     * @throws DeploymentException on any error
     */
    public boolean isReverseProxiedPathRegisteredByName(String appName) throws DeploymentException {
        return isReverseProxiedPathRegisteredUrlPrefix(getUserFacingDomain() + "/" + appName);
    }

    /**
     * Test if a given userFacingUrlPrefix is already registered with the reverse proxy.
     * The UserFacingUrlPrefix is auto generated from the UserFacingDomain EzConfiguration property and appName
     * <p/>
     * Please see the EzReverseProxy project for more details.
     *
     * @param userFacingUrlPrefix the userFacingUrlPrefix in the reverse proxy to test against
     * @return true if atleast one path already exist
     * @throws DeploymentException on any error
     */
    public boolean isReverseProxiedPathRegisteredUrlPrefix(String userFacingUrlPrefix) throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                return client.isReverseProxiedPathRegistered(userFacingUrlPrefix);
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    /**
     * @return the UserFacingDomain from EzConfiguration
     */
    protected String getUserFacingDomain() {
        return configuration.getUserFacingDomain();
    }

    /**
     * Gets the ezReverseProxy client.  You must always call returnClient on this client after retrieving the client
     * from this call. You should wrap it up in a try{} finally{} block.
     *
     * @return ezReverseProxy client from the thrift service pool
     * @throws TException - on an error getting the client
     */
    protected EzReverseProxy.Client getClient() throws TException {
        return this.pool.getClient(reverseProxyApplicationName, EzReverseProxyConstants.SERVICE_NAME, EzReverseProxy.Client.class);
    }

    /**
     * Validate our connection with the ezReverseProxy by successfully connecting and pinging the service.
     *
     * @throws DeploymentException on any reason why this object would be invalid like unable to contact the EzReverseProxy service
     */
    public void validate() throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                if (!client.ping()) {
                    DeploymentException e = new DeploymentException("Unable to ping reverse-proxy service client.");
                    log.error("Error pinging service", e);
                    throw e;
                }
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }

    public Set<UpstreamServerRegistration> getAllUpstreamServerRegistrations() throws DeploymentException {
        try {
            EzReverseProxy.Client client = getClient();
            try {
                return client.getAllUpstreamServerRegistrations();
            } finally {
                pool.returnToPool(client);
            }
        } catch (TException e) {
            log.error("Error with reverse-proxy service client", e);
            throw new DeploymentException("Reverse-proxy: " + e.getMessage());
        }
    }
}
