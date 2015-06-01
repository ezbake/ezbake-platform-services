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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.openshift.client.ApplicationScale;
import com.openshift.client.IApplication;
import com.openshift.client.IEnvironmentVariable;
import com.openshift.client.IGearProfile;
import com.openshift.client.cartridge.EmbeddableCartridge;
import com.openshift.client.cartridge.IEmbeddedCartridge;
import com.openshift.client.cartridge.IStandaloneCartridge;
import com.openshift.internal.client.GearProfile;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.publishers.artifact.ArtifactContentsPublisher;
import ezbake.deployer.publishers.openShift.DeployerOpenShiftConfigurationHelper;
import ezbake.deployer.publishers.openShift.OpenShiftDeployerModule;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.deployer.publishers.openShift.inject.ArtifactResource;
import ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactTypeKey;
import ezbake.deployer.utilities.Utilities;
import ezbake.reverseproxy.thrift.UpstreamServerRegistration;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.Language;
import ezbake.services.deploy.thrift.ResourceReq;
import ezbake.services.deploy.thrift.ResourceRequirements;
import ezbake.services.deploy.thrift.Scaling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Publisher that will publish artifacts given to it to OpenShift.  Assuming that the artifacts are web apps to be
 * deployed to a JBoss Container.
 * <p/>
 * This class will give the GearSize to match that of the highest (Math.max) resource requirement between CPU and memory
 * This ignores the disk space requirement.
 * <p/>
 * Application Scaling will be turned on iif both Scaling_min/Scaling_max is not 1.
 * <p/>
 * This will 'checkout' the Application from OpenShift (creating a new application if needed)
 * Update the application to the artifact's tarball.
 * Publish application to openshift (deploy/git push).  After this the web application should be visible from OpenShift.
 * <p/>
 * This class should be thread-safe.
 * <p/>
 * #################
 * #### This class needs some refactoring
 * #################
 */
public class EzOpenShiftPublisher implements EzPublisher {
    private static final Logger log = LoggerFactory.getLogger(EzOpenShiftPublisher.class);

    private final EzDeployerConfiguration configuration;
    private final DeployerOpenShiftConfigurationHelper openShiftConfigurationHelper;
    private Optional<EzReverseProxyRegister> reverseProxyRegister;

    private Map<ArtifactTypeKey, Set<ArtifactContentsPublisher>> fileProcessors;
    private Map<ArtifactTypeKey, Set<ArtifactResourceInjector>> injectors;

    private final static String APP_INSTANCE_NUM_SEPERATOR = "xx";
    private final static String EZBAKE_WSGI_ENTRY_POINT = "EZBAKE_WSGI_ENTRY_POINT";
    private final static String EZBAKE_APP_NAME_ENV_VAR = "EZBAKE_APPLICATION_NAME";
    private final static String EZBAKE_SERVICE_NAME_ENV_VAR = "EZBAKE_SERVICE_NAME";

    /**
     * Just for unit tests, and making this default constructable. If you want a valid object, you will need to use
     * the {@link #EzOpenShiftPublisher(EzDeployerConfiguration, EzReverseProxyRegister,
           Map<ezbake.deployer.utilities.ArtifactTypeKey,
               Set<ezbake.deployer.publishers.openShift.inject.ArtifactResourceInjector>>)} constructor.
     */
    public EzOpenShiftPublisher() {
        configuration = null;
        openShiftConfigurationHelper = new DeployerOpenShiftConfigurationHelper();
        injectors = Maps.newHashMap();
        fileProcessors = Maps.newHashMap();
        reverseProxyRegister = Optional.absent();
    }

    /**
     * Construct an Instance of EzOpenShiftPublisher with the given configuration.
     *
     * @param configuration ezDeployerConfiguration to configure this object
     */
    @Inject
    public EzOpenShiftPublisher(EzDeployerConfiguration configuration, EzReverseProxyRegister reverseProxyRegister,
                                Map<ArtifactTypeKey, Set<ArtifactResourceInjector>> resourceInjectors,
                                @Named(OpenShiftDeployerModule.OPENSHIFT_CONTENTS_PUBLISHER_MAP)
                                Map<ArtifactTypeKey, Set<ArtifactContentsPublisher>> fileProcessors) {
        this.configuration = configuration;
        this.openShiftConfigurationHelper = new DeployerOpenShiftConfigurationHelper(configuration.getEzConfiguration());
        this.injectors = resourceInjectors;
        this.fileProcessors = fileProcessors;

        if (this.configuration.isEzFrontEndEnabled())
            this.reverseProxyRegister = Optional.fromNullable(reverseProxyRegister);
        else
            this.reverseProxyRegister = Optional.absent();
    }

    /**
     * Publishes the artifact.  This is a blocking call waiting for the artifact to be completely published to
     * openShift.
     *
     * @param artifact - The artifact to deploy
     * @throws DeploymentException
     */
    @Override
    public void publish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {

        // Add our security token
        final String securityTokenPropsFileName = "openshift.properties";
        Map<String, String> tokenMap = Maps.newHashMap();
        tokenMap.put(EzBakePropertyConstants.EZBAKE_SHARED_SECRET_ENVIRONMENT_VARIABLE, "OPENSHIFT_SECRET_TOKEN");

        final String openShiftAppName = getOpenShiftAppName(artifact);
        final String openShiftDomainName = getOpenShiftDomainName(artifact);

        Rhc rhc = createRhc();
        final int numberOfInstances = getNumberOfInstances(artifact);
        List<RhcApplication> currentInstances = findAllApplicationInstances(openShiftAppName, openShiftDomainName);
        if (currentInstances.size() > 1) {
            // downsize the cluster, with this first for loop
            for (int i = numberOfInstances; i < currentInstances.size(); i++) {
                RhcApplication appInstance = currentInstances.get(i);
                maybeUnRegisterReverseProxy(artifact, appInstance.getApplicationInstance());
                appInstance.delete();
            }
        }

        for (int i = 0; i < numberOfInstances; i++) {
            tokenMap.put(EzBakePropertyConstants.EZBAKE_APPLICATION_INSTANCE_NUMBER, Integer.toString(i));
            String contents = Joiner.on('\n').withKeyValueSeparator("=").join(tokenMap);
            ArtifactDataEntry entry = Utilities.createConfCertDataEntry(securityTokenPropsFileName, contents.getBytes());
            ArtifactHelpers.addFilesToArtifact(artifact, Collections.singletonList(entry));

            pushApplicationInstance(rhc, artifact, openShiftAppName, openShiftDomainName, i);
        }
    }

    private int getNumberOfInstances(DeploymentArtifact artifact) {
        if (artifact.getMetadata().getManifest().getScaling() != null)
            return Math.max(artifact.getMetadata().getManifest().getScaling().getNumberOfInstances(), 1);
        else
            return 1;

    }

    private String getOpenShiftAppName(DeploymentArtifact artifact) throws DeploymentException {
        final String serviceId = sanatizeForOpenShift(ArtifactHelpers.getServiceId(artifact));
        Preconditions.checkArgument(!serviceId.matches("xx\\d+$"),
                "Artifact's service id \"" + serviceId + "\" must not end with xx<num>");
        return serviceId;
    }


    private String getOpenShiftDomainName(DeploymentArtifact artifact) throws DeploymentException {
        return sanatizeForOpenShift(ArtifactHelpers.getAppId(artifact));
    }

    @Override
    public void unpublish(DeploymentArtifact artifact, EzSecurityToken callerToken) throws DeploymentException {
        final String openShiftAppName = getOpenShiftAppName(artifact);
        final String openShiftDomainName = getOpenShiftDomainName(artifact);
        maybeUnRegisterAllReverseProxy(artifact);

        List<RhcApplication> currentInstances = findAllApplicationInstances(openShiftAppName, openShiftDomainName);
        for (RhcApplication appInstance : currentInstances) {
            log.info("Removing {} from openshift! ", appInstance.getApplicationInstance().getName());
            appInstance.delete();
        }
        String deleted = Iterables.toString(Iterables.transform(currentInstances, new Function<RhcApplication, String>() {
            @Override
            public String apply(RhcApplication input) {
                return input.getApplicationName();
            }
        }));

        log.info("Unpublished " + deleted + " from openShift.");
    }

    @Override
    public void validate() throws DeploymentException, IllegalStateException {
        Rhc rhc = createRhc();
        rhc.testConnection();
    }

    /**
     * If configured to register webapps behind a reverse proxy, this will do the registration.  This will only
     * register webapps behind the reverse proxy.
     *
     * @param artifact    - The artifact to register
     * @param application - The openShift application to get the information from
     * @throws DeploymentException - on any errors
     */
    private void maybeRegisterReverseProxy(DeploymentArtifact artifact, IApplication application) throws DeploymentException {
        if (!reverseProxyRegister.isPresent()
                || artifact.getMetadata().getManifest().getArtifactType() != ArtifactType.WebApp) return;
        reverseProxyRegister.get().publish(artifact, getApplicationUrl(application));
    }

    /**
     * If configured to register webapps behind a reverse proxy, this will do the un-registration.  This will only
     * unregister webapps behind the reverse proxy.
     *
     * @param artifact    - The artifact to un-register
     * @param application - The Openshift application object to grab the information from
     */
    private void maybeUnRegisterReverseProxy(DeploymentArtifact artifact, IApplication application) {
        if (!reverseProxyRegister.isPresent()
                || artifact.getMetadata().getManifest().getArtifactType() != ArtifactType.WebApp) return;

        UpstreamServerRegistration registration = new UpstreamServerRegistration();
        registration.setAppName(ArtifactHelpers.getServiceId(artifact));
        final URL applicationURL = getApplicationUrl(application);
        registration.setUpstreamHostAndPort(applicationURL.getHost() + ":443");
        registration.setTimeout(10);
        registration.setTimeoutTries(3);
        try {
            reverseProxyRegister.get().removeGivenInstance(registration, ArtifactHelpers.getExternalWebUrl(artifact));
        } catch (DeploymentException e) {
            log.warn("Error while removing instance from reverse-proxy", e);
        }

    }

    private URL getApplicationUrl(IApplication application) {
        try {
            return new URL(application.getApplicationUrl());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * If configured to register webapps behind a reverse proxy, this will do the un-registration.  This will only
     * unregister webapps behind the reverse proxy.
     *
     * @param artifact - The artifact to un-register
     */
    private void maybeUnRegisterAllReverseProxy(DeploymentArtifact artifact) {
        if (!reverseProxyRegister.isPresent()
                || artifact.getMetadata().getManifest().getArtifactType() != ArtifactType.WebApp) return;
        try {
            reverseProxyRegister.get().unpublishByAppName(ArtifactHelpers.getServiceId(artifact),
                    ArtifactHelpers.getExternalWebUrl(artifact));
        } catch (DeploymentException e) {
            log.warn("Error while removing instance from reverse-proxy", e);
        }
    }

    public void setReverseProxyRegister(EzReverseProxyRegister reverseProxyRegister) {
        this.reverseProxyRegister = Optional.fromNullable(reverseProxyRegister);
    }

    public void setInjectors(Map<ArtifactTypeKey, Set<ArtifactResourceInjector>> injectors) {
        this.injectors = injectors;
    }

    public void setProcessors(Map<ArtifactTypeKey, Set<ArtifactContentsPublisher>> processors) {
        this.fileProcessors = processors;
    }

    /**
     * This is to get the list of all openshift instances of a given application
     *
     * @param artifact   - The deployment artifact
     * @return - list of upstream paths
     * @throws DeploymentException - on any error retrieving the list
     */
    public List<URL> findAllApplicationInstances(DeploymentArtifact artifact) throws DeploymentException {
        final String openShiftAppName = getOpenShiftAppName(artifact);
        final String openShiftDomainName = getOpenShiftDomainName(artifact);
        return Lists.transform(findAllApplicationInstances(openShiftAppName, openShiftDomainName),
                new Function<RhcApplication, URL>() {
                    @Override
                    public URL apply(RhcApplication rhcApplication) {
                        return getApplicationUrl(rhcApplication.getApplicationInstance());
                    }
                });
    }

    /**
     * Get all openshift applications that are prefixed with name.  This is to get the list of all openshift applications
     * that was created for the given application name.
     *
     * @param name   - The application name to get a list of openshift applications for
     * @param domain - the domain name to look up
     * @return - list of OpenShift applications.  The git repo's for these have not been initialized yet.
     * @throws DeploymentException - on any error retrieving the list
     */
    private List<RhcApplication> findAllApplicationInstances(String name, String domain) throws DeploymentException {
        Rhc rhc = createRhc();
        return rhc.listApplicationInstances(name, domain);
    }

    /**
     * Push an application artifact instance to openshift.  Registering with the reverse proxy if necessary.
     * This will create the openshift application (if 'fake scaling' this function maybe called multiple times)
     * The name would be like [serviceName]xx[##] if 'fake scaling'
     *
     * @param rhc         - The openshift broker to push to
     * @param artifact    - The artifact to deploy
     * @param name        - The name of the service to push
     * @param domainName  - the name of the domain to push too
     * @param instanceNum the instance number to push
     * @throws DeploymentException - on any errors pushing
     */
    private void pushApplicationInstance(Rhc rhc, DeploymentArtifact artifact, String name, String domainName,
                                         int instanceNum) throws DeploymentException {
        RhcApplication rhcApplication =
                rhc.getOrCreateApplication(name + APP_INSTANCE_NUM_SEPERATOR + Integer.toString(instanceNum),
                        domainName, getCartridgeForArtifactType(artifact), ApplicationScale.NO_SCALE,
                        calculateGearProfile(artifact.getMetadata().getManifest().getArtifactInfo().getResourceRequirements()));

        Collection<ArtifactDataEntry> injectFiles = Lists.newArrayList();
        ArtifactTypeKey key = new ArtifactTypeKey(artifact.getMetadata().getManifest().getArtifactType(), artifact.getMetadata().getManifest().getArtifactInfo().getLanguage());
        Set<ArtifactContentsPublisher> processors = fileProcessors.get(key);
        if (processors != null) {
            for (ArtifactContentsPublisher processor : fileProcessors.get(key)) {
                Collection<ArtifactDataEntry> files = processor.generateEntries(artifact);
                if (files != null) {
                    injectFiles.addAll(files);
                }
            }
        } else {
            log.warn("No ArtifactContentsPublisher objects found for {}", key.toString());
        }
        ArtifactHelpers.addFilesToArtifact(artifact, injectFiles);
        rhcApplication.updateWithTarBall(artifact.getArtifact(), artifact.getMetadata().getVersion());

        // Add files to the git repo
        Set<ArtifactResourceInjector> injectorsToRun = injectors.get(ArtifactHelpers.getTypeKey(artifact));
        if (injectorsToRun != null) {
            List<ArtifactResource> resourceToInject = Lists.newArrayList();
            for (ArtifactResourceInjector injector : injectorsToRun) {
                resourceToInject.addAll(injector.getInjectableResources());
            }
            injectApplicationResources(resourceToInject, rhcApplication);
        }

        if (artifact.getMetadata().getManifest().getArtifactInfo().getLanguage() == Language.Python) {
            String bin = artifact.getMetadata().getManifest().getArtifactInfo().getBin();
            IApplication application = rhcApplication.getApplicationInstance();
            Map<String, IEnvironmentVariable> environmentVariableMap = application.getEnvironmentVariables();
            if (!environmentVariableMap.containsKey(EZBAKE_WSGI_ENTRY_POINT)) {
                application.addEnvironmentVariable(EZBAKE_WSGI_ENTRY_POINT, bin);
            }
        }



        addLogstashCartridge(rhcApplication, ArtifactHelpers.getAppId(artifact), ArtifactHelpers.getServiceId(artifact));
        rhcApplication.publishChanges();
        maybeRegisterReverseProxy(artifact, rhcApplication.getApplicationInstance());
    }


    /**
     * Connect to the OpenShift PaaS and return an object connected to it.
     *
     * @return the Openshift object
     */
    protected Rhc createRhc() {
        return new Rhc(openShiftConfigurationHelper);
    }

    /**
     * Get the Cartridge to use for the artifact type
     *
     * @param artifact the artifact to get the Cartridge for
     * @return the cartridge
     */
    private IStandaloneCartridge getCartridgeForArtifactType(DeploymentArtifact artifact) throws DeploymentException {
        return openShiftConfigurationHelper.getConfiguredCartridge(artifact.getMetadata().getManifest());
    }

    /**
     * Inject Environment variables and logstash cartridge on to every cartridge.
     */
    private void addLogstashCartridge(RhcApplication rhcApplication, String appName, String serviceName) throws DeploymentException {
        IApplication application = rhcApplication.getApplicationInstance();
        Map<String, IEnvironmentVariable> environmentVariableMap = application.getEnvironmentVariables();
        if (!environmentVariableMap.containsKey(EZBAKE_APP_NAME_ENV_VAR)) {
            application.addEnvironmentVariable(EZBAKE_APP_NAME_ENV_VAR, appName);
        }
        if (!environmentVariableMap.containsKey(EZBAKE_SERVICE_NAME_ENV_VAR)) {
            application.addEnvironmentVariable(EZBAKE_SERVICE_NAME_ENV_VAR, serviceName);
        }
        List<IEmbeddedCartridge> cartridges = application.getEmbeddedCartridges();
        boolean addLogstash = true;
        boolean addCron = true;
        for (IEmbeddedCartridge cartridge : cartridges) {
            log.info("Cartridge: {}", cartridge.getDisplayName());
            if (cartridge.getDisplayName().toLowerCase().contains("logstash")) {
                addLogstash = false;
            } else if (cartridge.getDisplayName().toLowerCase().startsWith("cron")) {
                addCron = false;
            }
        }
        if (addLogstash) {
            log.info("Adding logstash cartridge");
            application.addEmbeddableCartridge(new EmbeddableCartridge("logstash"));
        } else {
            log.info("Skipping logstash cartridge");
        }
        if (addCron) {
            log.info("Adding cron cartridge");
            application.addEmbeddableCartridge(new EmbeddableCartridge("cron"));
        } else {
            log.info("Skipping cron cartridge");
        }
        rhcApplication.setApplicationInstance(application);
    }

    /**
     * Process the list of resources that need to be injected into the artifact
     *
     * @param toBeInjected - the list of artifact resources ot inject
     * @param rhcApplication - the openshift application to put the script files into
     * @throws DeploymentException on any errors injecting the script files or thriftrunner binary
     */
    private void injectApplicationResources(List<ArtifactResource> toBeInjected, RhcApplication rhcApplication) throws DeploymentException {
        for (ArtifactResource resource : toBeInjected) {
            try (InputStream stream = resource.getStream()) {
                log.debug("Adding {}", resource.getPath());
                rhcApplication.addStreamAsFile(resource.getPath(), stream, resource.getPermissions());
            } catch (IOException e) {
                log.error("Failed injecting resource", e);
            }
        }
    }

    /**
     * Get the Application Scale state based on the User request.  If Min and max != 1 its scaling mode
     *
     * @param scale the metadata to based the scaling mode off of
     * @return the OpenShift Scale enum describing if we should scale or not
     */
    public static ApplicationScale getApplicationScaling(Scaling scale) {
        return scale.getNumberOfInstances() > 1 ? ApplicationScale.SCALE : ApplicationScale.NO_SCALE;
    }

    /**
     * Figure out the OpenShift application size based on the resource requirements of the artifact
     *
     * @param reqs the resource requirements to based the gear size off of
     * @return the GearSize
     * @throws DeploymentException on an unknown resource requirement size
     */
    public static IGearProfile calculateGearProfile(ResourceRequirements reqs) throws DeploymentException {
        ResourceReq maxValue = ResourceReq.findByValue(Math.max(reqs.getCpu().getValue(), reqs.getMem().getValue()));
        return reqToGearProfile(maxValue);
    }

    /**
     * Convert from ResourceReq to IGearProfile.  SMALL->SMALL, MEDIUM->MEDIUM, etc
     *
     * @param req the ResourceRequirement to convert
     * @return The GearProfile corresponding to the resourceRequirement given
     * @throws DeploymentException DeploymentException on an unknown resource requirement size
     */
    public static IGearProfile reqToGearProfile(ResourceReq req) throws DeploymentException {
        switch (req) {
            case small:
                return GearProfile.SMALL;
            case medium:
                return GearProfile.MEDIUM;
            case large:
                return GearProfile.EXLARGE;
        }
        throw new DeploymentException("Unknown Resource requirement size: " + req);
    }

    /**
     * Sanatize (replace all special characters with "")  a name to be used by openshift
     *
     * @param name the name to be sanatized
     * @return string which is sanatized
     */
    private static String sanatizeForOpenShift(String name) throws DeploymentException {
        if (Strings.isNullOrEmpty(name)) {
            String errorMsg = "OpenShift can not parse null or empty name!";
            log.error(errorMsg);
            throw new DeploymentException(errorMsg);
        }

        return name.replaceAll("[_\\W]", "").toLowerCase();
    }
}
