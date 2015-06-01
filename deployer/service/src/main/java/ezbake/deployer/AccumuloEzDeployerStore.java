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

package ezbake.deployer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import ezbake.deployer.configuration.EzDeployerConfiguration;
import ezbake.deployer.utilities.ArtifactHelpers;
import ezbake.services.deploy.thrift.ArtifactManifest;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.services.deploy.thrift.DeploymentMetadata;
import ezbake.services.deploy.thrift.DeploymentStatus;
import ezbakehelpers.accumulo.AccumuloHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ezbake.deployer.utilities.ArtifactHelpers.getAppId;
import static ezbake.deployer.utilities.ArtifactHelpers.getFqAppId;
import static org.apache.commons.lang.time.DurationFormatUtils.formatDuration;

/**
 * An Accumulo implementation of the EzBake Service Deployer.  This will store all artifacts within Accumulo+.
 * Then it will publish artifacts from accumulo to the correct PaaS.
 * <p/>
 */
public class AccumuloEzDeployerStore extends EzDeployerStore {

    private static final Logger log = LoggerFactory.getLogger(AccumuloEzDeployerStore.class);

    private static final int CURRENT_TABLE_VERSION = 2;

    public static final Text ARTIFACT_CF = new Text("application");
    public static final Text ARTIFACT_METADATA_CQ = new Text("metadata");
    public static final Text ARTIFACT_INDEX_CF = new Text("index");
    public static final Text ARTIFACT_INDEX_USER_CQ = new Text("user");
    public static final Text ARTIFACT_INDEX_SECURITY_CQ = new Text("securityid");
    public static final Text ARTIFACT_INDEX_APPLICATION_CQ = new Text("application");
    public static final Text ARTIFACT_INDEX_STATUS_CQ = new Text("status");
    public static final String VERSION_ROW_ID = "00000VERSION";
    public static final String VERSION_CF = "VERSION";
    public static final String VERSION_CQ = "VERSION";

    private Connector connector;
    private String tableName;

    private final int maxThreads = deployerConfiguration.getAccumuloMaxThreads();
    private final long maxLatency = deployerConfiguration.getAccumuloMaxLatency();
    private final long maxMemory = deployerConfiguration.getAccumuloMaxMemory();
    private final ArtifactWriter artifactWriter;

    /**
     * Creates a new instance of the handler grabbing the configuration details from EzDeployerConfiguration.
     *
     * @throws IOException - when can not connect to accumulo
     */
    @Inject
    public AccumuloEzDeployerStore(EzDeployerConfiguration deployerConfiguration, ArtifactWriter artifactWriter) throws IOException, DeploymentException, IllegalStateException {
        super(deployerConfiguration.getEzConfiguration(), deployerConfiguration);
        AccumuloHelper accumuloHelper = new AccumuloHelper(deployerConfiguration.getEzConfiguration());
        this.connector = accumuloHelper.getConnector(false);
        this.tableName = deployerConfiguration.getDeploymentTableName();
        this.artifactWriter = artifactWriter;
        checkDatabaseVersion();
    }


    /**
     * This constructor is for unit tests can add mock objects for the services it use.
     *
     * @param connector - the Accumulo connector to use
     * @param tableName - the table name within accumulo to write to
     */
    public AccumuloEzDeployerStore(Connector connector, String tableName, EzDeployerConfiguration deployerConfiguration, ArtifactWriter artifactWriter) throws IOException, DeploymentException {
        super(deployerConfiguration.getEzConfiguration(), deployerConfiguration);
        this.connector = connector;
        this.tableName = tableName;
        this.artifactWriter = artifactWriter;
        checkDatabaseVersion();
    }

    @Override
    public DeploymentMetadata getLatestApplicationMetaDataFromStore(String applicationId, String serviceId) throws TException, DeploymentException {
        return getLatestApplicationMetaDataFromStore(getFqAppId(applicationId, serviceId));
    }

    @Override
    public DeploymentArtifact writeArtifactToStore(ArtifactManifest manifest, ByteBuffer artifactData, DeploymentStatus status) throws DeploymentException, TException {
        long versionTimestamp = createVersionNumber();
        DeploymentMetadata application = new DeploymentMetadata(manifest, Long.toString(versionTimestamp));
        application.setStatus(status);
        DeploymentArtifact artifact = new DeploymentArtifact(application, artifactData);
        writeToStore(application, artifact);
        return artifact;
    }

    @Override
    public void updateDeploymentMetadata(DeploymentMetadata metadata) throws DeploymentException, TException {
        DeploymentArtifact oldVersion = getArtifactFromStore(ArtifactHelpers.getAppId(metadata),
                ArtifactHelpers.getServiceId(metadata), metadata.getVersion());
        removeFromStore(oldVersion.getMetadata());
        metadata.setVersion(Long.toString(createVersionNumber()));
        oldVersion.setMetadata(metadata);
        writeToStore(metadata, oldVersion);
    }

    private void writeToStore(DeploymentMetadata metadata, DeploymentArtifact artifact) throws TException {
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        String serviceRowId = getFqAppId(metadata.getManifest());
        Mutation mutation = new Mutation(serviceRowId);
        if (artifact != null) {
            artifactWriter.writeArtifact(metadata, artifact);
        }
        mutation.put(ARTIFACT_CF, ARTIFACT_METADATA_CQ, new Value(serializer.serialize(metadata)));

        List<Mutation> additionalMutations = Lists.newArrayList();
        addIndexMutations(metadata, serviceRowId, additionalMutations);
        BatchWriter writer = createBatchWriter();
        try {
            writer.addMutation(mutation);
            for (Mutation additionalMutation : additionalMutations) {
                writer.addMutation(additionalMutation);
            }
        } catch (MutationsRejectedException e) {
            log.error("Error writing deployment: ", e);
            throw new DeploymentException("Error writing deployment row: " + e.toString());
        } finally {
            closeWriter(writer);
        }
    }

    public void removeFromStore(DeploymentMetadata metadata) throws DeploymentException, TException {
        removeFromStore(metadata, false);
    }

    private void removeFromStore(DeploymentMetadata metadata, boolean metadataOnly) throws TException {
        String serviceRowId = getFqAppId(metadata.getManifest());
        Mutation mutation = new Mutation(serviceRowId);
        if (!metadataOnly) {
            artifactWriter.deleteArtifact(metadata);
        }
        mutation.putDelete(ARTIFACT_CF, ARTIFACT_METADATA_CQ);

        List<Mutation> additionalMutations = Lists.newArrayList();
        deleteIndexMutations(metadata, serviceRowId, additionalMutations);
        BatchWriter writer = createBatchWriter();
        try {
            writer.addMutation(mutation);
            for (Mutation additionalMutation : additionalMutations)
                writer.addMutation(additionalMutation);
        } catch (MutationsRejectedException e) {
            log.error("Error deleting deployment: ", e);
            throw new DeploymentException("Error deleting deployment row: " + e.toString());
        } finally {
            closeWriter(writer);
        }
    }

    @Override
    public FluentIterable<DeploymentMetadata> getApplicationMetaDataMatching(FieldName fieldName, String fieldValue) throws TException, DeploymentException {
        final Text indexFieldName;
        switch (fieldName) {
            case SecurityId:
                indexFieldName = ARTIFACT_INDEX_SECURITY_CQ;
                break;
            case ApplicationId:
                indexFieldName = ARTIFACT_INDEX_APPLICATION_CQ;
                break;
            case User:
                indexFieldName = ARTIFACT_INDEX_USER_CQ;
                break;
            case Status:
                indexFieldName = ARTIFACT_INDEX_STATUS_CQ;
                break;
            default:
                throw new DeploymentException("Unknown field name: \"" + fieldName.name + "\"");
        }
        return searchWithinStore(indexFieldName, fieldValue, ARTIFACT_METADATA_CQ).transform(createDeploymentMetaDataTransformer());
    }

    protected DeploymentArtifact getArtifactCombinedFromStore(String fqApplicationId) throws TException {
        TDeserializer deSerializer = new TDeserializer(new TCompactProtocol.Factory());
        Value metadata = getOrThrow(getValueFromStore(fqApplicationId, ARTIFACT_METADATA_CQ));
        DeploymentMetadata meta = new DeploymentMetadata();
        deSerializer.deserialize(meta, metadata.get());
        return artifactWriter.readArtifact(meta);
    }

    @Override
    public DeploymentArtifact getArtifactFromStore(String applicationId, String serviceId) throws TException, DeploymentException {
        return getArtifactCombinedFromStore(getFqAppId(applicationId, serviceId));
    }

    @Override
    public DeploymentArtifact getArtifactFromStore(String applicationId, String serviceId, String version) throws TException, DeploymentException {
        return getArtifactCombinedFromStore(getFqAppId(applicationId, serviceId));
    }


    private DeploymentMetadata getLatestApplicationMetaDataFromStore(String fqApplicationId) throws TException {
        return getApplicationMetaDataFromStore(fqApplicationId, null);
    }

    protected DeploymentMetadata getApplicationMetaDataFromStore(String fqApplicationId, String version) throws TException {
        Value rowValue = getOrThrow(getValueFromStore(fqApplicationId, ARTIFACT_METADATA_CQ));
        TDeserializer deSerializer = new TDeserializer(new TCompactProtocol.Factory());
        DeploymentMetadata deploymentMetadata = new DeploymentMetadata();
        deSerializer.deserialize(deploymentMetadata, rowValue.get());
        return deploymentMetadata;
    }


    @Override
    public FluentIterable<DeploymentMetadata> getApplicationMetaDataFromStoreForAllVersions(String applicationId, String serviceId) throws TException {
        return getApplicationMetaDataFromStoreForAllVersions(getFqAppId(applicationId, serviceId));
    }

    protected void checkDatabaseVersion() throws DeploymentException {
        int tableVersion = getTableVersion();
        if (tableVersion != CURRENT_TABLE_VERSION) {
            upgradeDatabase(tableVersion);
        }
    }

    private void upgradeDatabase(int oldVersion) throws DeploymentException {
        Stopwatch timer = Stopwatch.createStarted();
        try {
            log.info("Older database Table version found: {}, needed: {}, updating...", oldVersion, CURRENT_TABLE_VERSION);
            Function<Value, DeploymentMetadata> metadataTransformer = createDeploymentMetaDataTransformer();

            Scanner scanner = createScanner();
            scanner.fetchColumn(new Text(ARTIFACT_CF), new Text(ARTIFACT_METADATA_CQ));

            List<Exception> exceptions = Lists.newArrayList();
            List<Mutation> mutations = Lists.newArrayList();
            BatchWriter writer = getBatchWriter();
            try {
                for (Map.Entry<Key, Value> row : scanner) {
                    try {
                        DeploymentMetadata md = metadataTransformer.apply(row.getValue());
                        if (md == null) continue;
                        final String serviceRowId = getFqAppId(md.manifest);
                        if (!md.isSetStatus()) {
                            //Status didn't exist before version 2 of the database
                            md.setStatus(DeploymentStatus.Deployed);
                        }
                        mutations.clear();
                        addIndexMutations(md, serviceRowId, mutations);
                        writer.addMutations(mutations);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
                writer.flush();
            } catch (MutationsRejectedException e) {
                exceptions.add(e);
            } finally {
                closeWriter(writer);
            }
            if (exceptions.size() == 1) {
                log.error("Error upgrading database: ", exceptions.get(0));
                //noinspection ThrowableResultOfMethodCallIgnored
                throw new DeploymentException(exceptions.get(0).getMessage());
            } else if (exceptions.size() > 1) {
                String msg = String.format("%d exceptions occurred while upgrading database, showing first", exceptions.size());
                log.error(msg, exceptions.get(0));
                throw new DeploymentException(msg);
            }
            setTableVersion();
        } finally {
            log.info("Took {} to upgrade the database", formatDuration(timer.stop().elapsed(TimeUnit.MILLISECONDS), "mm:ss.S"));
        }
    }

    private Function<Value, DeploymentMetadata> createDeploymentMetaDataTransformer() {
        final TDeserializer deSerializer = new TDeserializer(new TCompactProtocol.Factory());

        return new Function<Value, DeploymentMetadata>() {
            @Override
            public DeploymentMetadata apply(Value value) {
                DeploymentMetadata artifact = new DeploymentMetadata();
                try {
                    deSerializer.deserialize(artifact, value.get());
                    return artifact;
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private FluentIterable<DeploymentMetadata> getApplicationMetaDataFromStoreForAllVersions(String applicationId) throws TException {
        try {
            return getValueFromStoreIterator(new Text(applicationId), ARTIFACT_METADATA_CQ).transform(createDeploymentMetaDataTransformer());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof DeploymentException) {
                throw (DeploymentException) e.getCause();
            } else if (e.getCause() instanceof TException) {
                throw (TException) e.getCause();
            } else {
                log.error("Error getting metadata for artifact", e);
                throw new DeploymentException(e.getMessage());
            }
        }
    }

    private Optional<Value> getValueFromStore(String fqApplicationId, Text columnQualifier) throws TException {
        FluentIterable<Value> rows = getValueFromStoreIterator(new Text(fqApplicationId), columnQualifier);
        return rows.first();
    }

    private FluentIterable<Value> getValueFromStoreIterator(Text fqApplicationId, Text columnQualifier) throws TException {
        Scanner scanner = createScanner();
        scanner.setRange(Range.exact(fqApplicationId, ARTIFACT_CF, columnQualifier));

        return FluentIterable.from(scanner).transform(new Function<Map.Entry<Key, Value>, Value>() {
            @Override
            public Value apply(Map.Entry<Key, Value> keyValueEntry) {
                return keyValueEntry.getValue();
            }
        });
    }

    private FluentIterable<Value> searchWithinStore(Text fieldName, String fieldValue, final Text resultColumnQualifier) throws TException {
        Scanner scanner = createScanner();
        scanner.setRange(Range.exact(new Text(fieldValue), new Text(ARTIFACT_INDEX_CF + ":" + fieldName)));
        FluentIterable<Range> matches = FluentIterable.from(scanner).transform(new Function<Map.Entry<Key, Value>, Range>() {
            @Override
            public Range apply(Map.Entry<Key, Value> input) {
                return Range.exact(asText(input.getValue()), ARTIFACT_CF, resultColumnQualifier);
            }
        });
        if (matches.isEmpty()) {
            return FluentIterable.from(Collections.<Value>emptyList());
        }

        final BatchScanner batchScanner = createBatchScanner();
        try {
            batchScanner.setRanges(matches.toList());

            return FluentIterable.from(FluentIterable.from(batchScanner).transform(new Function<Map.Entry<Key, Value>, Value>() {
                @Override
                public Value apply(Map.Entry<Key, Value> keyValueEntry) {
                    return keyValueEntry.getValue();
                }
            }).toList());
        } finally {
            closeWriter(batchScanner);
        }
    }


    private static Range exact(Text row, Text cf, Text cq, long ts) {
        Key startKey = new Key(row, cf, cq, ts);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
    }

    private Authorizations getAccumuloAuths() {
        return new Authorizations();
    }


    private BatchWriter createBatchWriter() throws DeploymentException {
        try {
            createTableIfNeeded();
            return connector.createBatchWriter(tableName, getBatchWriterConfig());
        } catch (TableNotFoundException e) {
            log.error("Error finding table for deployment information", e);
            throw new DeploymentException("Error finding table for deployment information." + e.toString());
        }
    }


    private BatchScanner createBatchScanner() throws DeploymentException {
        try {
            return connector.createBatchScanner(tableName, getAccumuloAuths(), maxThreads);
        } catch (TableNotFoundException e) {
            throw new DeploymentException(e.toString());
        }
    }

    private Scanner createScanner() throws DeploymentException {
        try {
            createTableIfNeeded();
            return connector.createScanner(tableName, getAccumuloAuths());
        } catch (TableNotFoundException e) {
            log.error("Error finding table for deployment information", e);
            throw new DeploymentException("Error finding table for deployment information." + e.toString());
        }
    }

    private void createTableIfNeeded() throws DeploymentException {
        if (!connector.tableOperations().exists(tableName)) {
            try {
                connector.tableOperations().create(tableName, true);
            } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
                log.error("Error creating table for deployments", e);
                throw new DeploymentException(e.getMessage());
            }
        }
    }

    private void closeWriter(BatchWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (Exception ignored) {

        }
    }


    private void closeWriter(BatchScanner scanner) {
        try {
            if (scanner != null)
                scanner.close();
        } catch (Exception ignored) {

        }
    }

    private int getTableVersion() throws DeploymentException {
        Scanner scanner = createScanner();

        scanner.setRange(Range.exact(VERSION_ROW_ID, VERSION_CF, VERSION_CQ));

        Map.Entry<Key, Value> value = Iterables.getFirst(scanner, null);
        if (value == null) return 0;
        return Integer.parseInt(value.getValue().toString());
    }

    private void setTableVersion() throws DeploymentException {
        BatchWriter writer = getBatchWriter();
        try {
            Mutation mutation = new Mutation(VERSION_ROW_ID);
            mutation.put(VERSION_CF, VERSION_CQ, new Value(Integer.toString(CURRENT_TABLE_VERSION).getBytes()));

            writer.addMutation(mutation);
            writer.flush();
        } catch (MutationsRejectedException e) {
            log.error("Error updating table version: ", e);
            throw new DeploymentException("Error updating table version");
        } finally {
            closeWriter(writer);
        }
    }

    private BatchWriter getBatchWriter() throws DeploymentException {
        BatchWriter writer;
        try {
            writer = connector.createBatchWriter(tableName, getBatchWriterConfig());
        } catch (TableNotFoundException e) {
            log.error("Error updating table version: ", e);
            throw new DeploymentException("Could not update table version");
        }
        return writer;
    }

    private BatchWriterConfig getBatchWriterConfig() {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(maxMemory);
        config.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS);
        config.setMaxWriteThreads(maxThreads);
        return config;
    }

    private Text asText(Value v) {
        return new Text(v.get());
    }


    private void addIndexMutations(DeploymentMetadata metadata, String serviceRowId, List<Mutation> additionalMutations) {
        ArtifactManifest manifest = metadata.getManifest();
        additionalMutations.add(createIndexMutation(ARTIFACT_INDEX_USER_CQ, new Text(manifest.getUser()), serviceRowId));
        additionalMutations.add(createIndexMutation(ARTIFACT_INDEX_SECURITY_CQ,
                new Text(manifest.getApplicationInfo().getSecurityId()), serviceRowId));
        additionalMutations.add(createIndexMutation(ARTIFACT_INDEX_STATUS_CQ,
                new Text(metadata.getStatus().toString()), serviceRowId));
        if (getAppId(manifest) != null) {
            additionalMutations.add(createIndexMutation(ARTIFACT_INDEX_APPLICATION_CQ, new Text(getAppId(manifest)), serviceRowId));
        }
    }

    private Mutation createIndexMutation(Text fieldName, Text fieldValue, String mainRowId) {
        Mutation additionalMutation = new Mutation(fieldValue);
        additionalMutation.put(ARTIFACT_INDEX_CF.toString() + ":" + fieldName, mainRowId, mainRowId);
        return additionalMutation;
    }

    private void deleteIndexMutations(DeploymentMetadata metadata, String serviceRowId, List<Mutation> additionalMutations) {
        ArtifactManifest manifest = metadata.getManifest();
        additionalMutations.add(deleteIndexMutation(ARTIFACT_INDEX_USER_CQ, new Text(manifest.getUser()), serviceRowId));
        additionalMutations.add(deleteIndexMutation(ARTIFACT_INDEX_SECURITY_CQ,
                new Text(manifest.getApplicationInfo().getSecurityId()), serviceRowId));
        additionalMutations.add(deleteIndexMutation(ARTIFACT_INDEX_STATUS_CQ,
                new Text(metadata.getStatus().toString()), serviceRowId));
        if (getAppId(manifest) != null) {
            additionalMutations.add(deleteIndexMutation(ARTIFACT_INDEX_APPLICATION_CQ, new Text(getAppId(manifest)), serviceRowId));
        }
    }

    private Mutation deleteIndexMutation(Text fieldName, Text fieldValue, String mainRowId) {
        Mutation deleteMutation = new Mutation(fieldValue);
        deleteMutation.putDelete(ARTIFACT_INDEX_CF.toString() + ":" + fieldName, mainRowId);
        return deleteMutation;
    }
}
