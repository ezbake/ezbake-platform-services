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

package ezbake.deployer.publishers.openShift;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.openshift.client.ApplicationScale;
import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import com.openshift.client.IGearProfile;
import com.openshift.client.IOpenShiftConnection;
import com.openshift.client.IUser;
import com.openshift.client.OpenShiftConnectionFactory;
import com.openshift.client.OpenShiftEndpointException;
import com.openshift.client.cartridge.IStandaloneCartridge;
import ezbake.deployer.impl.Files;
import ezbake.services.deploy.thrift.DeploymentException;
import org.eclipse.jgit.errors.UnsupportedCredentialItem;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.URIish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;


/**
 * Rhc (Red Hat Cloud, e.g. OpenShift) - helper class for adding/updating an OpenShift Application to be put
 * into OpenShift.  This will not actually update the artifact, just create the app in OpenShift, and retrieve the
 * git repository to local disk.  Use the returned RhcApplication to modify the actual artifact deployed.
 */
public class Rhc {
    private static final Logger log = LoggerFactory.getLogger(Rhc.class);
    private static final String CLIENT_ID = "EzBakeOpenShiftClient";

    private final String openshiftUsername;
    private final String openshiftPassword;
    private final String openshiftHost;
    private final String openshiftKeyPassphrase;
    private final int openshiftTimeout;
    private final int failureRetries;

    private final CredentialsProvider gitCredentialsProvider;
    private final File rootGitDir;

    public Rhc() {
        openshiftUsername = "default";
        openshiftPassword = "default";
        openshiftHost = "localhost";
        openshiftKeyPassphrase = null;
        gitCredentialsProvider = generateGitCredentials();
        rootGitDir = DeployerOpenShiftConfigurationHelper.getGitTempBaseDir();
        openshiftTimeout = 300000;
        failureRetries = 2;
    }

    public Rhc(DeployerOpenShiftConfigurationHelper configuration) {
        openshiftUsername = configuration.getUsername();
        openshiftPassword = configuration.getPassword();
        openshiftHost = configuration.getHost();
        openshiftKeyPassphrase = configuration.getSshPassphrase();
        gitCredentialsProvider = generateGitCredentials();
        rootGitDir = configuration.getGitCheckoutBaseDir();
        openshiftTimeout = configuration.getOpenshiftTimeout();
        failureRetries = Integer.parseInt(configuration.getConfiguration().getProperty("ezdeploy.failed.retries", "2"));
    }

    /**
     * @throws DeploymentException if connection is invalid
     */
    public void testConnection() throws DeploymentException {
        getOrCreateConnection().getUser().getDefaultDomain();
    }

    private CredentialsProvider generateGitCredentials() {
        return new CredentialsProvider() {
            @Override
            public boolean isInteractive() {
                return false;
            }

            @Override
            public boolean supports(CredentialItem... items) {
                log.info("Supported credentials: " + Joiner.on(',').join(items));
                return true;
            }


            @Override
            public boolean get(URIish uri, CredentialItem... items) throws UnsupportedCredentialItem {
                log.info("Number of items: " + items.length);
                for (CredentialItem item : items) {
                    log.info(item.getPromptText());
                    if (item instanceof CredentialItem.StringType)
                        ((CredentialItem.StringType) item).setValue(openshiftKeyPassphrase);
                    else if (item instanceof CredentialItem.YesNoType) {
                        //The authenticity of host 'hostname' can't be established.
                        // TODO: This isn't secured to always passed in true.
                        ((CredentialItem.YesNoType) item).setValue(true);
                    }
                }
                return true;
            }
        };
    }


    private IOpenShiftConnection getOrCreateConnection() throws DeploymentException {
        try {
            return new OpenShiftConnectionFactory().getConnection(CLIENT_ID, openshiftUsername, openshiftPassword,
                    openshiftHost);
        } catch (com.openshift.client.OpenShiftEndpointException e) {
            log.error("Error connecting to openshift application", e);
            throw new DeploymentException(e.getMessage());
        }
    }

    /**
     * Either gets or creates a new Openshift application.  This will not modify the artifact in OpenShift.
     * Use the returned RhcApplication to modify/submit the artifact to OpenShift.
     *
     * @param applicationId    - The application to add to openshift
     * @param domainName       - The name of the domain where the application will reside in
     * @param primaryCartridge - The primary cartridge for this application (Probably want @{link StandaloneCartridge.NAME_JBOSSAS}
     * @param scaling          - tell openshift if you want the app to scale or not.
     * @param slotSize         - How big of a gear to use for this web application
     * @return RhcApplication that is pointing to the local copy (git repo) and has the IApplication object from OpenShift
     * @throws DeploymentException - on any error creating or retrieving the application.
     */
    public RhcApplication getOrCreateApplication(String applicationId, String domainName,
                                                 IStandaloneCartridge primaryCartridge, ApplicationScale scaling, IGearProfile slotSize)
            throws DeploymentException {

        IOpenShiftConnection connection = getOrCreateConnection();
        IDomain domain = getOrCreateDomain(domainName, connection.getUser(), true);
        String errorString = String.format("OpenShift user %s was unable to get or create domain %s!",
                connection.getUser().getRhlogin(), domainName);
        domain = Preconditions.checkNotNull(domain, errorString);
        IApplication applicationInstance = domain.getApplicationByName(applicationId);
        File appDir = Files.resolve(rootGitDir, domainName, applicationId);
        if (applicationInstance == null) {
            //Apparently Openshift doesn't like multiple deployments to 1 broker at the same time
            log.info("Waiting for my turn to create {} {}", applicationId, domainName);
            synchronized (Rhc.class) {
                log.info("Creating new application {} : {} : {} : {}", applicationId, primaryCartridge, scaling, slotSize);
                int tries = 0;
                while (tries < failureRetries) {
                    try {
                        applicationInstance = domain.createApplication(applicationId, primaryCartridge, scaling,
                                slotSize, null, openshiftTimeout);
                        break;
                    } catch (OpenShiftEndpointException ex) {
                        //Openshift seems to throw this error a lot, but re-trying usually works
                        if (ex.getMessage().contains("unable to create user group")) {
                            tries++;
                            log.warn("Received the OpenShift group error.  Retrying", ex);
                        } else {
                            throw ex;
                        }
                    }
                }
            }
            if (appDir.exists()) {
                log.info("Clearing out existing directory: " + appDir.getAbsolutePath());
                try {
                    Files.deleteRecursively(appDir);
                } catch (IOException e) {
                    log.error("Error clearing directory for new application", e);
                    throw new DeploymentException(e.getMessage());
                }
            }
        }
        RhcApplication application = new RhcApplication(applicationInstance, domain, appDir, gitCredentialsProvider);
        application.getOrCreateGitRepo(); // side-effect of pulling the git repository
        return application;
    }

    /**
     * This is only until we can use the real scaling of OpenShift
     *
     * @param applicationId The application to list the instances from in openshift
     * @param domainName    the domain for which the application lives in
     * @return RhcApplication for each instance
     */
    public List<RhcApplication> listApplicationInstances(final String applicationId, final String domainName) throws DeploymentException {
        IOpenShiftConnection connection = getOrCreateConnection();
        final IDomain domain = getOrCreateDomain(domainName, connection.getUser(), false);
        if (domain == null) {
            return Collections.emptyList();
        }
        //noinspection ConstantConditions
        return Lists.newArrayList(Iterables.transform(Iterables.filter(domain.getApplications(),
                new Predicate<IApplication>() {
                    @Override
                    public boolean apply(IApplication input) {
                        String appName = input.getName();
                        int index = appName.lastIndexOf("xx");
                        if (index != -1) {
                            appName = appName.substring(0, index);
                        }
                        return appName.equals(applicationId);
                    }
                }), new Function<IApplication, RhcApplication>() {
            @Override
            public RhcApplication apply(IApplication input) {
                File appDir = Files.resolve(rootGitDir, input.getName());
                return new RhcApplication(input, domain, appDir, gitCredentialsProvider);
            }
        }));
    }

    /**
     * A method to get or create an openshift domain.
     *
     * @param domainName         name of the domain to get or create, if null we will return the users default domain
     * @param user               the user from the openshift connection
     * @param shouldCreateDomain a boolean telling us if we should create a domain or not if it doesn't exist
     * @return an IDomain which is an openshift domain, the users default domain if the domainName was empty or null,
     * or null if we shouldCreate is false and we couldn't get the domain
     */
    private static IDomain getOrCreateDomain(String domainName, IUser user, boolean shouldCreateDomain) {
        IDomain domain = null;
        if (Strings.isNullOrEmpty(domainName)) {
            log.warn("Use of the default domain is deprecated and will be removed in an upcoming release!");
            domain = user.getDefaultDomain();
        } else {
            // check to see if the user already has the domain
            domain = user.getDomain(domainName);
            if (domain == null && shouldCreateDomain) {
                log.info("Attempting to create domain " + domainName + "!");
                domain = user.createDomain(domainName);
            }
        }

        return domain;
    }

}
