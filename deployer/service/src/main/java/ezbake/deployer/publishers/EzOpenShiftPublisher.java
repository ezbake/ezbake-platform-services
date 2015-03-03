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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
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
import com.openshift.client.cartridge.StandaloneCartridge;
import com.openshift.internal.client.GearProfile;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.impl.PosixFilePermission;
import ezbake.deployer.publishers.openShift.DeployerOpenShiftConfigurationHelper;
import ezbake.deployer.publishers.openShift.Rhc;
import ezbake.deployer.publishers.openShift.RhcApplication;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.deployer.utilities.ArtifactDataEntry;
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
import org.apache.commons.io.IOUtils;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

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
    private final List<String> resourcesToInject;
    private final List<String> thriftRunnerControlScripts;
    private final List<String> thriftRunnerWWWFiles;
    private final List<String> securityActionHooks;

    private final Optional<File> pathToThriftRunnerBinaryJar;
    private final File thriftRunnerArtifactPath = Files.get("bin", "thriftrunner.jar");

    private final static String EXTRA_FILES_CLASSPATH = "ezdeploy.openshift.extraFiles";
    private final static File EXTRA_FILES_BASEPATH = Files.get("/", EXTRA_FILES_CLASSPATH.replace('.', '/'));

    private final static String THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES = "ezdeploy.openshift.thriftrunner.action_hooks";
    private final static String SECURITY_ACTION_HOOKS = "ezdeploy.openshift.security.action_hooks";
    private final static File OPENSHIFT_ACTION_HOOKS_PATH = Files.get(".openshift", "action_hooks");

    private final static String OPENSHIFT_CRON_FILES = "ezdeploy.openshift.cron";
    private final static File OPENSHIFT_CRON_PATH = Files.get(".openshift", "cron", "daily");

    private final static String THRIFT_RUNNER_EXTRA_FILES_CART_FILES = "ezdeploy.openshift.thriftrunner.extraFiles.www";
    private final static File THRIFT_RUNNER_EXTRA_FILES_PATH = Files.get("www");

    private final static Set<String> executableScripts = ImmutableSet.<String>builder().add("start", "stop", "pre_build",
            "pre_start_jbossas", "pre_restart_jbossas").build();
    private final static Set<PosixFilePermission> executablePerms = ImmutableSet.<PosixFilePermission>builder()
            .add(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE).build();

    private final static String APP_INSTANCE_NUM_SEPERATOR = "xx";

    private final static String EZBAKE_APP_NAME_ENV_VAR = "EZBAKE_APPLICATION_NAME";
    private final static String EZBAKE_SERVICE_NAME_ENV_VAR = "EZBAKE_SERVICE_NAME";

    /**
     * Just for unit tests, and making this default constructable. If you want a valid object, you will need to use
     * the {@link #EzOpenShiftPublisher(EzDeployerConfiguration, EzReverseProxyRegister)} constructor.
     */
    public EzOpenShiftPublisher() {
        configuration = null;
        openShiftConfigurationHelper = new DeployerOpenShiftConfigurationHelper();
        resourcesToInject = generresourcesToInject();
        thriftRunnerControlScripts = openshiftThriftRunnerControlScripts();
        thriftRunnerWWWFiles = openshiftThriftRunnerWWW();
        securityActionHooks = getSecurityOpenShiftActionHooks();
        File trJar = Files.get("thriftrunner.jar");
        if (!Files.exists(trJar)) trJar = null;
        pathToThriftRunnerBinaryJar = Optional.fromNullable(trJar);
        reverseProxyRegister = Optional.absent();
    }

    /**
     * Construct an Instance of EzOpenShiftPublisher with the given configuration.
     *
     * @param configuration ezDeployerConfiguration to configure this object
     */
    @Inject
    public EzOpenShiftPublisher(EzDeployerConfiguration configuration, EzReverseProxyRegister reverseProxyRegister) {
        this.configuration = configuration;
        this.openShiftConfigurationHelper = new DeployerOpenShiftConfigurationHelper(configuration.getEzConfiguration());
        resourcesToInject = generresourcesToInject();
        thriftRunnerControlScripts = openshiftThriftRunnerControlScripts();
        thriftRunnerWWWFiles = openshiftThriftRunnerWWW();
        securityActionHooks = getSecurityOpenShiftActionHooks();
        pathToThriftRunnerBinaryJar = Optional.fromNullable(configuration.getThriftRunnerJar());
        if (this.configuration.isEzFrontEndEnabled())
            this.reverseProxyRegister = Optional.fromNullable(reverseProxyRegister);
        else
            this.reverseProxyRegister = Optional.absent();
    }

    /**
     * This searches the classpath {@link #EXTRA_FILES_CLASSPATH} for files to auto inject into every tar files to
     * publish.
     *
     * @return List of paths that can be read using class.getResourceByName()
     */
    private static List<String> generresourcesToInject() {
        return Utilities.getResourcesFromClassPath(EzOpenShiftPublisher.class, EXTRA_FILES_CLASSPATH);
    }

    /**
     * This searches the classpath {@link #THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES} for files to auto inject into every tar files to
     * publish.  This is for the DIY start/stop script for thrift runner projects.  This code could probably go away once we are
     * confident in the ThriftRunner Cartridge
     *
     * @return List of paths that can be read using class.getResourceByName()
     */
    private static List<String> openshiftThriftRunnerControlScripts() {
        return Utilities.getResourcesFromClassPath(EzOpenShiftPublisher.class, THRIFT_RUNNER_CART_CONTROL_SCRIPT_FILES);
    }

    /**
     * This searches the classpath {@link #THRIFT_RUNNER_EXTRA_FILES_CART_FILES} for files to auto inject into every tar files to
     * publish.  This injects a base www folder into the project so that each thriftrunner project can have a web assessible page.
     * Once Soup delivers a WAR file, this is probably unneeded.
     *
     * @return List of paths that can be read using class.getResourceByName()
     */
    private static List<String> openshiftThriftRunnerWWW() {
        return Utilities.getResourcesFromClassPath(EzOpenShiftPublisher.class, THRIFT_RUNNER_EXTRA_FILES_CART_FILES);
    }

    /**
     * This searches the classpath for files to auto inject for all openshift cartridges
     * This will get the prebuild action hooks.
     *
     * @return List of paths that can be read using class.getResourceByName()
     */
    private static List<String> getSecurityOpenShiftActionHooks() {
        return Utilities.getResourcesFromClassPath(EzOpenShiftPublisher.class, SECURITY_ACTION_HOOKS);
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
        ArtifactType artifactType = artifact.getMetadata().getManifest().getArtifactType();
        RhcApplication rhcApplication =
                rhc.getOrCreateApplication(name + APP_INSTANCE_NUM_SEPERATOR + Integer.toString(instanceNum),
                        domainName, getCartridgeForArtifactType(artifact), ApplicationScale.NO_SCALE,
                        calculateGearProfile(artifact.getMetadata().getManifest().getArtifactInfo().getResourceRequirements()));
        rhcApplication.updateWithTarBall(artifact.getArtifact(), artifact.getMetadata().getVersion());
        injectSpecialScripts(rhcApplication);
        if (artifactType != ArtifactType.WebApp) {
            injectThriftRunnerFiles(rhcApplication);
        }
        // All Openshift cartridges get this
        injectListOfResources(securityActionHooks, OPENSHIFT_ACTION_HOOKS_PATH, rhcApplication);
        injectListOfResources(Utilities.getResourcesFromClassPath(EzOpenShiftPublisher.class, OPENSHIFT_CRON_FILES),
                OPENSHIFT_CRON_PATH, rhcApplication);
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
     * Injects the extra files into the application
     *
     * @param rhcApplication - the openshift application to put the extra files into
     * @throws DeploymentException on any errors injecting
     */
    private void injectSpecialScripts(RhcApplication rhcApplication) throws DeploymentException {
        for (String resourcePath : resourcesToInject) {
            InputStream resource = EzOpenShiftPublisher.class.getResourceAsStream("/" + resourcePath);
            try {
                final File artifactPath = Files.relativize(EXTRA_FILES_BASEPATH, Files.get("/", resourcePath));
                rhcApplication.addStreamAsFile(artifactPath, resource);
            } finally {
                IOUtils.closeQuietly(resource);
            }
        }
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
     * If configured retrieves the thriftrunner binary from the filesystem
     *
     * @return thriftrunner binary
     * @throws DeploymentException - on any errors reading it
     */
    protected InputStream getThriftRunnerBinary() throws DeploymentException {
        try {
            if (pathToThriftRunnerBinaryJar.isPresent())
                return new FileInputStream(pathToThriftRunnerBinaryJar.get());
            else
                return null;
        } catch (FileNotFoundException e) {
            log.error("Could not find thrift runner binary at: " + pathToThriftRunnerBinaryJar.get().toString(), e);
            throw new DeploymentException("Could not find thrift runner binary at: "
                    + pathToThriftRunnerBinaryJar.get().toString() + ":" + e.getMessage());
        }
    }

    /**
     * For the DIY version of thriftrunner apps, injects the thriftrunner binary and the start/stop control scripts from
     * the classpath in order to start the thriftrunner service
     *
     * @param rhcApplication - the openshift application to put the script files into
     * @throws DeploymentException on any errors injecting the script files or thriftrunner binary
     */
    private void injectThriftRunnerFiles(RhcApplication rhcApplication) throws DeploymentException {
        InputStream resource = getThriftRunnerBinary();
        try {
            if (resource != null)
                rhcApplication.addStreamAsFile(thriftRunnerArtifactPath, resource);
        } catch (DeploymentException e) {
            log.error("Error injecting thrift runner binary into application bundle", e);
            throw new DeploymentException(e.getMessage());
        } finally {
            IOUtils.closeQuietly(resource);
        }
        injectListOfResources(thriftRunnerControlScripts, OPENSHIFT_ACTION_HOOKS_PATH, rhcApplication);
        injectListOfResources(thriftRunnerWWWFiles, THRIFT_RUNNER_EXTRA_FILES_PATH, rhcApplication);
    }

    /**
     * This is the helper function that actually injects a list of resources from the classpath into the application
     * This will be placed inside of the git repository for the OpenShift Application
     *
     * @param resources      - the list of resources to inject
     * @param basePath       - the base path (relative to the git repository root) 'www' would go to gitProject/www/*
     * @param rhcApplication - the openshift application to put the resources into
     * @throws DeploymentException on any errors injecting the resources into the application
     */
    public void injectListOfResources(List<String> resources, File basePath, RhcApplication rhcApplication) throws DeploymentException {
        InputStream resource;
        for (String resourcePathStr : resources) {
            resourcePathStr = Files.get("/", resourcePathStr).toString();
            resource = EzOpenShiftPublisher.class.getResourceAsStream(resourcePathStr);
            try {
                final File resourcePath = new File(Files.get(resourcePathStr).getName());
                final File artifactPath = Files.resolve(basePath, resourcePath);
                if (resource == null) {
                    log.warn("Resource file " + resourcePathStr + " couldn't be opened.");
                } else {
                    log.info("Adding: " + resourcePath.toString());
                    if (executableScripts.contains(resourcePath.toString())) {
                        rhcApplication.addStreamAsFile(artifactPath, resource, executablePerms);
                    } else {
                        rhcApplication.addStreamAsFile(artifactPath, resource);
                    }
                }
            } finally {
                IOUtils.closeQuietly(resource);
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
