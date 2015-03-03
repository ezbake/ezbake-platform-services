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

package ezbake.deployer.utilities;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.deployer.utilities.YmlKeys.ApplicationKeys;
import ezbake.deployer.utilities.YmlKeys.ArtifactKeys;
import ezbake.deployer.utilities.YmlKeys.BatchServiceKeys;
import ezbake.deployer.utilities.YmlKeys.CustomServiceKeys;
import ezbake.deployer.utilities.YmlKeys.DatabaseKeys;
import ezbake.deployer.utilities.YmlKeys.FrackServiceKeys;
import ezbake.deployer.utilities.YmlKeys.ManifestKey;
import ezbake.deployer.utilities.YmlKeys.ResourcesKeys;
import ezbake.deployer.utilities.YmlKeys.RootManifestKeys;
import ezbake.deployer.utilities.YmlKeys.ScalingKeys;
import ezbake.deployer.utilities.YmlKeys.ThriftServiceKeys;
import ezbake.deployer.utilities.YmlKeys.WebAppKeys;
import ezbake.services.deploy.thrift.ApplicationInfo;
import ezbake.services.deploy.thrift.ArtifactInfo;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.BatchJobInfo;
import ezbake.services.deploy.thrift.DatabaseInfo;
import ezbake.services.deploy.thrift.Language;
import ezbake.services.deploy.thrift.ResourceReq;
import ezbake.services.deploy.thrift.ResourceRequirements;
import ezbake.services.deploy.thrift.Scaling;
import ezbake.services.deploy.thrift.WebAppInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ezbake.deployer.utilities.ArtifactHelpers.getServiceId;

/**
 * This file parses a Yaml manifest file into an ArtifactManifest deployer
 * thrift object.
 */
// The uncheck and ConstantConditions are expected for this class because of the use of the YamlReader.  I have to cast
// objects it returns to maps.
@SuppressWarnings({"unchecked", "ConstantConditions"})
public class YamlManifestFileReader implements ManifestFileReader {
    private static final Logger log = LoggerFactory.getLogger(YamlManifestFileReader.class);
    private UserProvider userProvider;
    private Map<String, Object> overrides;

    public YamlManifestFileReader(UserProvider userProvider) {
        this.userProvider = userProvider;
        this.overrides = new HashMap<>();
    }

    public YamlManifestFileReader(UserProvider userProvider, Map<String, Object> overrides) {
        this.userProvider = userProvider;
        this.overrides = overrides;
    }

    @Override
    public List<ArtifactManifest> readFile(File file) throws IOException, IllegalStateException {
        InputStream is = new FileInputStream(file);
        try {
            return readFile(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Override
    public List<ArtifactManifest> readFile(InputStream inputStream) throws IOException, IllegalStateException {
        return readFile(new InputStreamReader(inputStream));
    }

    @Override
    public List<ArtifactManifest> readFile(Reader reader) throws IOException, IllegalStateException {

        YamlReader artifactReader = new YamlReader(reader);

        // the thing to be returned with yaml filled stuff
        ArtifactManifest applicationManifest = new ArtifactManifest();
        applicationManifest.setApplicationInfo(new ApplicationInfo());
        applicationManifest.setUser(userProvider.getUser());
        List<ArtifactManifest> result = Lists.newArrayList();

        try {
            Map<String, Object> ymlTopManifest = (Map<String, Object>) artifactReader.read();

            Map<String, Object> ymlAppManifest = (Map<String, Object>) ymlTopManifest.get("Application");
            Preconditions.checkState(ymlAppManifest != null, "The root node must be called Application: " + ymlTopManifest.toString());
            setApplicationInfo(ymlAppManifest, applicationManifest.getApplicationInfo(), true);
            Preconditions.checkState(applicationManifest.getApplicationInfo().isSetApplicationId(), "name must be set in application");

            List<Map<String, Object>> services = (List<Map<String, Object>>) ymlAppManifest.get("Services");
            Preconditions.checkState(services != null && !services.isEmpty(), "the Services block must be set under the Application block and have more than 1 item");

            for (Map<String, Object> ymlServiceManifest : services) {
                ArtifactManifest manifest = new ArtifactManifest(applicationManifest);
                manifest.setArtifactInfo(new ArtifactInfo());
                setArtifactType(ymlServiceManifest, manifest);
                setApplicationInfo(ymlServiceManifest, manifest.getApplicationInfo(), false);
                setArtifactInfo(ymlServiceManifest, manifest.getArtifactInfo(), manifest.getApplicationInfo());
                setScaling(ymlServiceManifest, manifest);
                setWebAppInfo(ymlServiceManifest, manifest);
                setDatabaseInfo(ymlServiceManifest, manifest);
                setThriftService(ymlServiceManifest, manifest);
                setFrackService(ymlServiceManifest, manifest);
                setCustomService(ymlServiceManifest, manifest);
                setBatchService(ymlServiceManifest, manifest);
                validateManifest(manifest);
                result.add(manifest);
            }
            return result;
        } catch (YamlException | IllegalArgumentException e) {
            throw new IOException(e);
        }
    }

    private void validateManifest(ArtifactManifest artifactManifest) throws IllegalStateException {
        Preconditions.checkState(artifactManifest.getApplicationInfo().isSetServiceId(), getServiceIdName(artifactManifest) + " is expected to be set");
        Preconditions.checkState(StringUtils.isAlphanumeric(getServiceId(artifactManifest)), getServiceIdName(artifactManifest) + " must be alphanumeric: " + getServiceId(artifactManifest));
        Preconditions.checkState(artifactManifest.isSetArtifactType(), "ArtifactType is expected to be set");
        Preconditions.checkState(artifactManifest.getApplicationInfo().isSetSecurityId(), "SecurityId is expected to be set");
    }

    private String getServiceIdName(ArtifactManifest artifactManifest) {
        switch (artifactManifest.getArtifactType()) {
            case Thrift:
                return ThriftServiceKeys.ServiceName.getName();
            case DataSet:
                return DatabaseKeys.ServiceName.getName();
            case WebApp:
                return WebAppKeys.WebAppName.getName();
            case Frack:
                return FrackServiceKeys.PipelineName.getName();
            case Custom:
                return CustomServiceKeys.ServiceName.getName();
            case Batch:
                return BatchServiceKeys.JobName.getName();
            default:
                return "ServiceId";
        }
    }

    private void setArtifactInfo(Map<String, Object> ymlManifest, ArtifactInfo artifactInfo, ApplicationInfo applicationInfo) {
        Map<String, Object> subMap = (Map<String, Object>) ymlManifest.get(RootManifestKeys.resources.getName());
        if (subMap != null) artifactInfo.setResourceRequirements(toRequirements(subMap));
        String value = toString(ymlManifest.get(RootManifestKeys.language.getName()));
        if (value != null) artifactInfo.setLanguage(languageFromString(value));
        updateValue(ymlManifest, artifactInfo, ArtifactInfo._Fields.BIN, ArtifactKeys.bin);
        updateValue(ymlManifest, artifactInfo, ArtifactInfo._Fields.PURGEABLE, ArtifactKeys.purgeable,
                Boolean.class, Optional.of(false));
        updateValue(ymlManifest, artifactInfo, ArtifactInfo._Fields.SYSTEM_LOGFILE_DISABLED, ArtifactKeys.disableSystemLogfile,
                Boolean.class, Optional.of(false));

        Iterable<String> iterable = (Iterable<String>) ymlManifest.get(ArtifactKeys.config.getName());
        if (iterable != null && !Iterables.isEmpty(iterable)) {
            artifactInfo.setConfig(Sets.newHashSet(iterable));
        }
        setApplicationInfo(ymlManifest, applicationInfo, false);
        Map<String, Object> innerArtifactInfo = (Map<String, Object>) ymlManifest.get(RootManifestKeys.artifactInfo.getName());
        if (innerArtifactInfo != null) setArtifactInfo(innerArtifactInfo, artifactInfo, applicationInfo);
    }

    private void setApplicationInfo(Map<String, Object> ymlManifest, ApplicationInfo applicationInfo, boolean inAppBlock) {
        updateValue(ymlManifest, applicationInfo, ApplicationInfo._Fields.APPLICATION_ID, RootManifestKeys.applicationName);
        if (inAppBlock) {
            updateValue(ymlManifest, applicationInfo, ApplicationInfo._Fields.APPLICATION_ID, ApplicationKeys.AppName);
        }
        updateValue(ymlManifest, applicationInfo, ApplicationInfo._Fields.SECURITY_ID, RootManifestKeys.securityId);

        Iterable<String> iterable = (Iterable<String>) ymlManifest.get(RootManifestKeys.datasets.getName());
        if (iterable != null && !Iterables.isEmpty(iterable)) {
            applicationInfo.setDatasets(Lists.newArrayList(iterable));
        }
        iterable = (Iterable<String>) ymlManifest.get(RootManifestKeys.auths.getName());
        if (iterable != null && !Iterables.isEmpty(iterable)) {
            applicationInfo.setAuths(Sets.newHashSet(iterable));
        }
        updateValue(ymlManifest, applicationInfo, ApplicationInfo._Fields.SERVICE_ID, RootManifestKeys.serviceName);
    }

    private void setArtifactType(Map<String, Object> ymlServiceManifest, ArtifactManifest artifactManifest) {
        String value = reqStr(ymlServiceManifest, RootManifestKeys.type.getName());
        artifactManifest.setArtifactType(artifactTypeFromString(value));
    }

    private void setWebAppInfo(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        if (artifactManifest.getArtifactType() != ArtifactType.WebApp) return;
        log.trace("Reading web app info attributes");
        WebAppInfo info = new WebAppInfo();
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.EXTERNAL_WEB_URL, WebAppKeys.ExternalWebUrl);
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.INTERNAL_WEB_URL, WebAppKeys.InternalWebUrl);
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.HOSTNAME, WebAppKeys.Hostname);
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.TIMEOUT, WebAppKeys.Timeout, Integer.class, Optional.of(60));
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.TIMEOUT_RETRIES, WebAppKeys.TimeoutRetries, Integer.class, Optional.of(2));
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.UPLOAD_FILE_SIZE, WebAppKeys.UploadFileSize, Integer.class, Optional.of(5));
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.STICKY_SESSION, WebAppKeys.StickySession, Boolean.class, Optional.of(false));
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.PREFERRED_CONTAINER, WebAppKeys.PreferredContainer, String.class);
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.CHUNKED_TRANSFER_ENCODING_DISABLED,
                WebAppKeys.DisableChunkedTransferEncoding, Boolean.class, Optional.of(false));
        updateValue(ymlAppManifest, info, WebAppInfo._Fields.WEBSOCKET_SUPPORT_DISABLED, WebAppKeys.DisableWebsocketSupport,
                Boolean.class, Optional.of(false));
        String value = toString(ymlAppManifest.get(WebAppKeys.WebAppName.getName()));
        if (value != null) artifactManifest.getApplicationInfo().setServiceId(value);
        artifactManifest.setWebAppInfo(info);
    }

    private void setDatabaseInfo(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        DatabaseInfo info = new DatabaseInfo();
        updateValue(ymlAppManifest, info, DatabaseInfo._Fields.DATABASE_TYPE, DatabaseKeys.Database);
        if (!Strings.isNullOrEmpty(info.getDatabaseType())) {
            artifactManifest.setDatabaseInfo(info);
        }
    }

    private void setThriftService(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        if (artifactManifest.getArtifactType() != ArtifactType.Thrift) return;
        String value = toString(ymlAppManifest.get(ThriftServiceKeys.ServiceName.getName()));
        if (value != null) artifactManifest.getApplicationInfo().setServiceId(value);
    }

    private void setBatchService(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        log.info("===== Current artifact type ====:" + artifactManifest.getArtifactType());
        if (artifactManifest.getArtifactType() != ArtifactType.Batch) return;

        BatchJobInfo info = new BatchJobInfo();
        updateValue(ymlAppManifest, info, BatchJobInfo._Fields.START_DATE, BatchServiceKeys.StartDate);
        updateValue(ymlAppManifest, info, BatchJobInfo._Fields.START_TIME, BatchServiceKeys.StartTime);
        updateValue(ymlAppManifest, info, BatchJobInfo._Fields.REPEAT, BatchServiceKeys.Repeat);
        updateValue(ymlAppManifest, info, BatchJobInfo._Fields.FLOW_NAME, BatchServiceKeys.FlowName);

        String value = toString(ymlAppManifest.get(BatchServiceKeys.JobName.getName()));
        if (value != null) artifactManifest.getApplicationInfo().setServiceId(value);
        artifactManifest.setBatchJobInfo(info);
    }

    private void setFrackService(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        if (artifactManifest.getArtifactType() != ArtifactType.Frack) return;
        String value = toString(ymlAppManifest.get(FrackServiceKeys.PipelineName.getName()));
        if (value != null) artifactManifest.getApplicationInfo().setServiceId(value);
    }

    private void setCustomService(Map<String, Object> ymlAppManifest, ArtifactManifest artifactManifest) {
        if (artifactManifest.getArtifactType() != ArtifactType.Custom) return;
        String value = toString(ymlAppManifest.get(CustomServiceKeys.ServiceName.getName()));
        if (value != null) artifactManifest.getApplicationInfo().setServiceId(value);
    }

    private void updateValue(Map<String, Object> ymlAppManifest, TBase objToSet, TFieldIdEnum fieldToSet, ManifestKey key) {
        updateValue(ymlAppManifest, objToSet, fieldToSet, key, String.class, Optional.<String>absent());
    }

    private <T> void updateValue(Map<String, Object> ymlAppManifest, TBase objToSet, TFieldIdEnum fieldToSet, ManifestKey key,
                                 Class<T> typeOfValue) {
        updateValue(ymlAppManifest, objToSet, fieldToSet, key, typeOfValue, Optional.<T>absent());
    }

    private <T> void updateValue(Map<String, Object> ymlAppManifest, TBase objToSet, TFieldIdEnum fieldToSet, ManifestKey key,
                                 Class<T> typeOfValue, Optional<T> defaultValue) {
        log.debug("Getting manifest value {}", key.getName());
        Object value = ymlAppManifest.get(key.getName());
        boolean override = true;
        //This is a bit hacky, but we don't want to override the application id for common services
        if (fieldToSet == ApplicationInfo._Fields.APPLICATION_ID) {
            Object currentValue = objToSet.getFieldValue(fieldToSet);
            if (currentValue != null && currentValue.toString().equals("common_services")) {
                override = false;
            }
            if (value != null && value.equals("common_services")) {
                override = false;
            }
        }
        if (override && overrides.containsKey(key.getName())) {
            value = overrides.get(key.getName());
        } else if (value == null && defaultValue.isPresent()) {
            value = defaultValue.get();
        }

        if (value != null) {
            log.debug("Setting field {} to {}", fieldToSet.getFieldName(), value);
            if (typeOfValue != null && !typeOfValue.equals(String.class)) {
                if (typeOfValue.equals(Integer.class)) {
                    objToSet.setFieldValue(fieldToSet, Integer.parseInt(toString(value)));
                } else if (typeOfValue.equals(Boolean.class)) {
                    objToSet.setFieldValue(fieldToSet, Boolean.parseBoolean(toString(value)));
                } else if (typeOfValue.equals(Double.class)) {
                    objToSet.setFieldValue(fieldToSet, Double.parseDouble(toString(value)));
                }
            } else {
                objToSet.setFieldValue(fieldToSet, value);
            }
        }
    }

    private String toString(Object o) {
        if (o == null) return null;
        return o.toString();
    }

    private Boolean toBoolean(Object o) {
        if (o == null) return null;
        return Boolean.parseBoolean(o.toString());
    }

    private String reqStr(Map<String, Object> artifactManifest, String key) {
        Object value = artifactManifest.get(key);
        Preconditions.checkState(value != null, "Required manifest property '" + key + "' does not exist.");
        return value.toString();
    }

    private void setScaling(Map<String, Object> ymlManifest, ArtifactManifest manifest) {
        Map<String, Object> ymlScaling = (Map<String, Object>) ymlManifest.get(RootManifestKeys.scaling.getName());
        if (ymlScaling == null) return;

        String value = (String) ymlScaling.get(ScalingKeys.numberOfInstances.getName());
        if (value != null)
            manifest.setScaling(new Scaling().setNumberOfInstances(Short.parseShort(value)));
    }

    private ResourceRequirements toRequirements(Map<String, Object> requirements) {
        ResourceRequirements reqs = new ResourceRequirements();
        String value = (String) requirements.get(ResourcesKeys.cpu.getName());
        if (value != null)
            reqs.setCpu(ResourceReq.valueOf(value));
        value = (String) requirements.get(ResourcesKeys.mem.getName());
        if (value != null)
            reqs.setMem(ResourceReq.valueOf(value));
        value = (String) requirements.get(ResourcesKeys.disk.getName());
        if (value != null)
            reqs.setDisk(ResourceReq.valueOf(value));
        return reqs;
    }

    private ArtifactType artifactTypeFromString(String text) {
        if (text != null) {
            for (ArtifactType artifactType : ArtifactType.values()) {
                if (text.equalsIgnoreCase(artifactType.name())) {
                    return artifactType;
                }
            }
        }
        return null;
    }

    private Language languageFromString(String text) {
        if (text != null) {
            for (Language language : Language.values()) {
                if (text.equalsIgnoreCase(language.name())) {
                    return language;
                }
            }
        }
        return null;
    }
}