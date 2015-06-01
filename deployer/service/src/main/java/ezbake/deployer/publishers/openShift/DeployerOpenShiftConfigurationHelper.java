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

import com.google.common.base.Strings;
import com.openshift.client.cartridge.IStandaloneCartridge;
import com.openshift.client.cartridge.StandaloneCartridge;
import ezbake.common.properties.EzProperties;
import ezbake.common.security.TextCryptoProvider;
import ezbake.deployer.configuration.OpenShiftConfiguration;
import ezbake.deployer.impl.Files;
import ezbake.deployer.utilities.ArtifactTypeKey;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbakehelpers.ezconfigurationhelpers.system.SystemConfigurationHelper;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * This configuration helper contains all of the OpenShift specific configuration's the Deployer needs
 */
public class DeployerOpenShiftConfigurationHelper {
    public static final String OPENSHIFT_USERNAME_KEY = "ezDeploy.openshift.username";
    public static final String OPENSHIFT_PASSWORD_KEY = "ezDeploy.openshift.password";
    public static final String OPENSHIFT_HOST_KEY = "ezDeploy.openshift.host";
    public static final String OPENSHIFT_SSH_PASSPHRASE_KEY = "ezDeploy.openshift.ssh.passphrase";
    public static final String OPENSHIFT_GIT_CHECKOUT_BASE_DIR_KEY = "openshift.git.checkout.base";
    public static final String OPENSHIFT_TIMEOUT = "openshift.timeout";
    public static final String THIRFT_RUNNER_OPENSHIFT_CARTRIDGE_NAME_KEY = "thriftrunner.openshift.cartridge.name";
    public static final String THIRFT_RUNNER_OPENSHIFT_CARTRIDGE_URL_KEY = "thriftrunner.openshift.cartridge.url";
    public static final String JBOSSAS_OPENSHIFT_CARTRIDGE_NAME_KEY = "jbossas.openshift.cartridge.name";
    public static final String JBOSSAS_OPENSHIFT_CARTRIDGE_URL_KEY = "jboassas.openshift.cartridge.url";
    public static final String PLAY_FRAMEWORK_OPENSHIFT_CARTRIDGE_NAME_KEY = "playframework.openshift.cartridge.name";
    public static final String PLAY_FRAMEWORK_OPENSHIFT_CARTRIDGE_URL_KEY = "playframework.openshift.cartridge.url";
    public static final String WILDFLY_OPENSHIFT_CARTRIDGE_NAME_KEY = "wildfly.openshift.cartridge.name";
    public static final String WILDFLY_OPENSHIFT_CARTRIDGE_URL_KEY = "wildfly.openshift.cartridge.url";
    public static final String NODEJS_OPENSHIFT_CARTRIDGE_NAME_KEY = "nodejs.openshift.cartridge.name";
    public static final String NODEJS_OPENSHIFT_CARTRIDGE_URL_KEY = "nodejs.openshift.cartridge.url";
    public static final String PYTHON_OPENSHIFT_CARTRIDGE_NAME_KEY = "python.openshift.cartridge.name";
    public static final String PYTHON_OPENSHIFT_CARTRIDGE_URL_KEY = "python.openshift.cartridge.url";

    private static File TMP_BASE_DIR;

    private EzProperties configuration;
    private CartridgeMapping cartridgeMapping;

    public DeployerOpenShiftConfigurationHelper() {
        this(new Properties());
    }

    public DeployerOpenShiftConfigurationHelper(Properties configuration) {
        this(configuration, new SystemConfigurationHelper(configuration).getTextCryptoProvider());
    }

    public DeployerOpenShiftConfigurationHelper(Properties configuration, TextCryptoProvider provider) {
        this.configuration = new EzProperties(configuration, true);
        this.configuration.setTextCryptoProvider(provider);
        this.cartridgeMapping = new CartridgeMapping(this);
    }

    public EzProperties getConfiguration() {
        return configuration;
    }

    public CartridgeMapping getCartridgeMapping() {
        return cartridgeMapping;
    }

    /**
     * Get the Openshift username to use for deployments
     * @return the username
     */
    public String getUsername() {
        return configuration.getProperty(OPENSHIFT_USERNAME_KEY);
    }

    /**
     * Get the Openshift password to use for deployments
     * @return the username
     */
    public String getPassword() {
        return configuration.getProperty(OPENSHIFT_PASSWORD_KEY);
    }

    /**
     * Get the Openshift host to use
     * @return the hostname
     */
    public String getHost() {
        return configuration.getProperty(OPENSHIFT_HOST_KEY);
    }

    /**
     * Get the SSH Pass-phrase for connecting to OpenShift
     * @return the pass-phrase
     */
    public String getSshPassphrase() {
        return configuration.getProperty(OPENSHIFT_SSH_PASSPHRASE_KEY);
    }

    /**
     * A specific base local disk location to place git repositories while deploying artifacts.
     *
     * This understand environment variables in the form of '${OPENSHIFT_DATA_DIR}/appRepo'
     * @return the local directory
     */
    public File getGitCheckoutBaseDir() {
        String unparsedDir = configuration.getProperty(OPENSHIFT_GIT_CHECKOUT_BASE_DIR_KEY,
                getGitTempBaseDir().getAbsolutePath());

        // replace things like: ${OPENSHIFT_DATA_DIR}/appRepo
        StrSubstitutor substitutor = new StrSubstitutor(System.getenv());
        File path = Files.get(substitutor.replace(unparsedDir));
        Files.createDirectories(path);
        return path;
    }

    /**
     * Amount of time to wait for Open Shift to create an application
     * @return time in <seconds?>
     */
    public int getOpenshiftTimeout() {
        return configuration.getInteger(OPENSHIFT_TIMEOUT, 300000);
    }

    /**
     * Return the cartridge configured for this artifact, according to the manifest
     *
     * By default, this will use the pre-defined cartridge mapping. If WebAppInfo is defined in the manifest,
     * it will use the preferred container if any.
     *
     * Supported preferred containers:
     *   - jbossas (same as java webapp gets)
     *   - play-framework
     *   - wildfly
     *
     * @param manifest artifact manifest
     * @return the configured cartridge
     * @throws DeploymentException
     */
    public IStandaloneCartridge getConfiguredCartridge(ArtifactManifest manifest) throws DeploymentException {
        if (manifest.isSetWebAppInfo() && !Strings.isNullOrEmpty(manifest.getWebAppInfo().getPreferredContainer())) {
            String preferredContainer = manifest.getWebAppInfo().getPreferredContainer();
            switch (preferredContainer) {
                case "jbossas":
                    return getJbossasCartridge();
                case "play-framework":
                    return getPlayFrameworkCartridge();
                case "wildfly":
                    return getWildflyCartridge();
                default:
                    throw new DeploymentException(String.format("Invalid preferred container: %s", preferredContainer));
            }
        } else{
            return cartridgeMapping.get(new ArtifactTypeKey(
                    manifest.getArtifactType(),
                    manifest.getArtifactInfo().getLanguage()));
        }
    }


    /**
     * The name of the cartridge to use for deploying thrift services to OpenShift
     * @return the cartridge name
     */
    public String getThriftRunnerCartridgeName() {
        return configuration.getProperty(THIRFT_RUNNER_OPENSHIFT_CARTRIDGE_NAME_KEY, "java-thriftrunner");
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getThriftRunnerCartridgeUrl() {
        return configuration.getProperty(THIRFT_RUNNER_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for Thrift Runner
     * @return the cartridge
     */
    public IStandaloneCartridge getThriftRunnerCartridge() {
        return getCartridge(getThriftRunnerCartridgeName(), getThriftRunnerCartridgeUrl());
    }

    /**
     * The name of the cartridge to use for deploying JBoss apps to OpenShift
     * @return the cartridge name
     */
    public String getJbossasCartridgeName() {
        return configuration.getProperty(JBOSSAS_OPENSHIFT_CARTRIDGE_NAME_KEY, StandaloneCartridge.NAME_JBOSSAS);
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getJbossasCartridgeUrl() {
        return configuration.getProperty(JBOSSAS_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for JBoss
     * @return the cartridge
     */
    public IStandaloneCartridge getJbossasCartridge() {
        return getCartridge(getJbossasCartridgeName(), getJbossasCartridgeUrl());
    }

    /**
     * The name of the cartridge to use for deploying play framework apps to OpenShift
     * @return the cartridge name
     */
    public String getPlayFrameworkCartridgeName() {
        return configuration.getProperty(PLAY_FRAMEWORK_OPENSHIFT_CARTRIDGE_NAME_KEY, "play-framework");
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getPlayFrameworkCartridgeUrl() {
        return configuration.getProperty(PLAY_FRAMEWORK_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for Play
     * @return the cartridge
     */
    public IStandaloneCartridge getPlayFrameworkCartridge() {
        return getCartridge(getPlayFrameworkCartridgeName(), getPlayFrameworkCartridgeUrl());
    }

    /**
     * The name of the cartridge to use for deploying WildFly apps to OpenShift
     * @return the cartridge name
     */
    public String getWildflyCartridgeName() {
        return configuration.getProperty(WILDFLY_OPENSHIFT_CARTRIDGE_NAME_KEY, "wildfly");
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getWildflyCartridgeUrl() {
        return configuration.getProperty(WILDFLY_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for WildFly
     * @return the cartridge
     */
    public IStandaloneCartridge getWildflyCartridge() {
        return getCartridge(getWildflyCartridgeName(), getWildflyCartridgeUrl());
    }

    /**
     * The name of the cartridge to use for deploying NodeJS apps to OpenShift
     * @return the cartridge name
     */
    public String getNodeJsCartridgeName() {
        return configuration.getProperty(NODEJS_OPENSHIFT_CARTRIDGE_NAME_KEY, "nodejs-0.10");
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getNodeJsCartridgeUrl() {
        return configuration.getProperty(NODEJS_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for NodeJs
     * @return the cartridge
     */
    public IStandaloneCartridge getNodeJsCartridge() {
        return getCartridge(getNodeJsCartridgeName(), getNodeJsCartridgeUrl());
    }

    /**
     * The name of the cartridge to use for deploying Python apps to OpenShift
     * @return the cartridge name
     */
    public String getPythonCartridgeName() {
        return configuration.getProperty(PYTHON_OPENSHIFT_CARTRIDGE_NAME_KEY, "python-2.7");
    }

    /**
     * Points to the URL for this cartridge. This is for development and using an experimental cartridge
     * @return the URL to the web location for this cartridge
     */
    public String getPythonCartridgeUrl() {
        return configuration.getProperty(PYTHON_OPENSHIFT_CARTRIDGE_URL_KEY);
    }

    /**
     * Get the IStandaloneCartridge for python
     * @return the cartridge
     */
    public IStandaloneCartridge getPythonCartridge() {
        return getCartridge(getPythonCartridgeName(), getPythonCartridgeUrl());
    }

    public static synchronized File getGitTempBaseDir() {
        if (TMP_BASE_DIR == null) {
            TMP_BASE_DIR = generateGitTempBaseDir();
        }
        return TMP_BASE_DIR;
    }

    private static File generateGitTempBaseDir() {
        String openshiftTmpDir = OpenShiftConfiguration.EnvVariables.OPENSHIFT_TMP_DIR.getEnvValue();
        if (openshiftTmpDir != null) {
            return Files.createTempDirectory(Files.get(openshiftTmpDir), "appRepo");
        } else {
            return Files.createTempDirectory("appRepo");
        }
    }

    /**
     * Get the cartridge for the given name & url
     *
     * @param name name of the cartridge.
     * @param url url for the cartridge. optional
     * @return an IStandaloneCartridge. Will return null if name is null
     */
    private IStandaloneCartridge getCartridge(final String name, final String url) {
        IStandaloneCartridge cartridge = null;
        if (name != null) {
            if (url == null) {
                cartridge = new StandaloneCartridge(name);
            } else {
                try {
                    cartridge = new StandaloneCartridge(name, new URL(url));
                } catch (MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        return cartridge;
    }

}
