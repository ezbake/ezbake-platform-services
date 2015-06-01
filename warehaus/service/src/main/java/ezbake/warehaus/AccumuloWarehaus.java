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

package ezbake.warehaus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;

import ezbake.base.thrift.*;
import ezbake.common.properties.EzProperties;
import ezbake.data.common.TimeUtil;
import ezbake.data.iterator.EzBakeVisibilityFilter;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.centralPurge.thrift.ezCentralPurgeServiceConstants;
import ezbake.services.provenance.thrift.PositionsToUris;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.thrift.ThriftClientPool;
import ezbake.security.permissions.PermissionUtils;
import ezbake.security.serialize.VisibilitySerialization;
import ezbake.security.serialize.thrift.VisibilityWrapper;
import ezbake.thrift.ThriftUtils;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;
import ezbake.util.AuditLoggerConfigurator;
import ezbakehelpers.accumulo.AccumuloHelper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloWarehaus extends ezbake.base.thrift.EzBakeBaseThriftService implements WarehausService.Iface, EzBakeBasePurgeService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloWarehaus.class);

    private EzbakeSecurityClient securityClient = null;
    private Connector connector;
    //    private FileSystem hdfs;
    private String accumuloNamespace;
    private String purgeVisibility;
    private String purgeAppSecurityId;
    private static AuditLogger auditLogger;

    public AccumuloWarehaus() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public TProcessor getThriftProcessor() {

        Properties properties = getConfigurationProperties();
        AccumuloHelper accumulo = new AccumuloHelper(properties);

        try {
            EzProperties ezproperties = new EzProperties(getConfigurationProperties(), false);
            purgeVisibility = ezproperties.getProperty(WarehausConstants.PURGE_VISIBILITY_KEY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (purgeVisibility == null || purgeVisibility.trim().isEmpty()) {
            String msg = "The required Warehaus purge visibility configuration parameter '" + WarehausConstants.PURGE_VISIBILITY_KEY + "' could not be found.";
            logger.error(msg + ". Set the parameter in the Warehaus application configuration file before starting the Warehaus service.");
            throw new RuntimeException(msg);
        }

        accumuloNamespace = accumulo.getAccumuloNamespace();
        if (logger.isDebugEnabled()) {
            logger.debug("Setting configuration...");
            for (Object prop : properties.keySet()) {
                logger.debug("Property: {} = {}", prop, properties.get(prop));
            }
        }

//        try {
//            this.hdfs = HDFSHelper.getFileSystemFromProperties(properties);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        try {
            this.connector = accumulo.getConnector(true);
            ensureTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        securityClient = new EzbakeSecurityClient(properties);
        AuditLoggerConfigurator.setAdditivity(true);
        auditLogger = AuditLogger.getAuditLogger(AccumuloWarehaus.class);
        return new WarehausService.Processor(this);
    }

    @Override
    public boolean ping() {
        try {
            boolean ping = connector.tableOperations().exists(WarehausConstants.TABLE_NAME);
            if (!ping) {
                logger.error("Ping: The warehaus table does not exist.");
            }
            boolean purgePing = connector.tableOperations().exists(WarehausConstants.PURGE_TABLE_NAME);
            if (!purgePing) {
                logger.error("Ping: The purge warehaus table does not exist.");
            }
            return ping && purgePing;
        } catch (Exception ex) {
            logger.error("Ping failed with an unexpected exception", ex);
            return false;
        }
    }

    @Override
    public IngestStatus insert(Repository data, Visibility visibility, EzSecurityToken security) throws TException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "insert");
        auditArgs.put("uri", data.getUri());
        auditLog(security, AuditEventType.FileObjectCreate, auditArgs);
        UpdateEntry entry = new UpdateEntry();
        entry.setUri(data.getUri());
        entry.setParsedData(data.getParsedData());
        entry.setRawData(data.getRawData());
        entry.setUpdateVisibility(data.isUpdateVisibility());
        IngestStatus status = insertUpdate(entry, visibility, security);
        logger.debug("insert status : " + status);
        return status;
    }

    @Override
    public BinaryReplay getLatestRaw(String uri, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getLatestRaw");
        auditArgs.put("uri", uri);
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getLatest(uri, WarehausUtils.getUriPrefixFromUri(uri), GetDataType.RAW.toString(), security);
    }

    @Override
    public BinaryReplay getLatestParsed(String uri, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getLatestParsed");
        auditArgs.put("uri", uri);
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getLatest(uri, WarehausUtils.getUriPrefixFromUri(uri), GetDataType.PARSED.toString(), security);
    }

    @Override
    public BinaryReplay getRaw(String uri, long timestamp, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getRaw");
        auditArgs.put("uri", uri);
        auditArgs.put("timestamp", String.valueOf(timestamp));
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getVersion(uri, WarehausUtils.getUriPrefixFromUri(uri), GetDataType.RAW.toString(), timestamp, security);
    }

    @Override
    public BinaryReplay getParsed(String uri, long timestamp, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getParsed");
        auditArgs.put("uri", uri);
        auditArgs.put("timestamp", String.valueOf(timestamp));
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getVersion(uri, WarehausUtils.getUriPrefixFromUri(uri), GetDataType.PARSED.toString(), timestamp, security);
    }

    @Override
    public List<ezbake.warehaus.BinaryReplay> get(ezbake.warehaus.GetRequest getRequest,
                                                  ezbake.base.thrift.EzSecurityToken security) throws MaxGetRequestSizeExceededException, org.apache.thrift.TException {
        securityClient.validateReceivedToken(security);
        List<BinaryReplay> retList;
        GetDataType getType = getRequest.getGetDataType();

        String colQualifier = null;

        switch (getType) {
            case RAW:
                colQualifier = getType.toString();
                break;
            case PARSED:
                colQualifier = getType.toString();
                break;
            default:
                break;
        }

        List<Key> keys = Lists.newArrayList();
        boolean getLatestErr = false;
        boolean getTimestampErr = false;
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "get");

        for (RequestParameter param : getRequest.getRequestParams()) {
            long timestamp = 0;
            // if latest, a timestamp value must not be provided
            if (getRequest.isLatestVersion() && (param.getTimestamp() != null)) {
                getLatestErr = true;
                break;
            }
            // if not latest, a timestamp value must be provided
            if (!getRequest.isLatestVersion()) {
                if (param.getTimestamp() == null) {
                    getTimestampErr = true;
                    break;
                } else {
                    timestamp = TimeUtil.convertFromThriftDateTime(param.getTimestamp());
                }
            }
            String uri = param.getUri();

            if (getType == GetDataType.VIEW) {
                colQualifier = param.getSpacename() + "_" + param.getView();
            }

            Key key = new Key(WarehausUtils.getKey(uri), new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(colQualifier), timestamp);
            keys.add(key);

            auditArgs.put("uri", uri);
            auditLog(security, AuditEventType.FileObjectAccess, auditArgs);

        }

        if (getLatestErr) {
            throw new TException("A uri timestamp value CAN NOT be provided when requesting the latest version");
        }
        if (getTimestampErr) {
            throw new TException("A uri timestamp value MUST be provided when not requesting the latest version");
        }

        if (getRequest.isLatestVersion()) {
            retList = getLatest(keys, security);
        } else {
            retList = getVersion(keys, security);
        }
        return retList;
    }

    @Override
    public List<DatedURI> replay(String uriPrefix, boolean replayOnlyLatest, DateTime start, DateTime finish, GetDataType type,
                                 EzSecurityToken security) throws TException {
        securityClient.validateReceivedToken(security);
        if (uriPrefix == null || "".equals(uriPrefix.trim())) {
            throw new TException("Cannot replay a null or empty URI prefix.");
        }
        if (type == GetDataType.VIEW) {
            throw new TException("Cannot replay data from a view");
        }

        // Default to PARSED if the user did not provide a data type
        GetDataType typeToReplay = type == null ? GetDataType.PARSED : type;
        String auths = WarehausUtils.getAuthsListFromToken(security);

        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "replay");
        auditArgs.put("uriPrefix", uriPrefix);
        auditArgs.put("start", start != null ? "" + TimeUtil.convertFromThriftDateTime(start) : "");
        auditArgs.put("finish", finish != null ? "" + TimeUtil.convertFromThriftDateTime(finish) : "");
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);

        BatchScanner scanner = null;
        List<DatedURI> retVal = null;
        try {
            scanner = createScanner(auths);

            IteratorSetting iteratorSetting = new IteratorSetting(13, "warehausReplayVisibilityIterator",
                    EzBakeVisibilityFilter.class);
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ), iteratorSetting);

            IteratorSetting is = new IteratorSetting(10, "replay", VersioningIterator.class);
            if (replayOnlyLatest) {
                VersioningIterator.setMaxVersions(is, 1);
            } else {
                VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            }
            scanner.addScanIterator(is);

            long startTime = 0, endTime = System.currentTimeMillis();
            if (start != null) {
                startTime = TimeUtil.convertFromThriftDateTime(start);
            }
            if (finish != null) {
                endTime = TimeUtil.convertFromThriftDateTime(finish);
            }

            IteratorSetting tis = new IteratorSetting(20, "timestamp", TimestampFilter.class);
            TimestampFilter.setRange(tis, startTime, true, endTime, true);
            scanner.addScanIterator(tis);

            scanner.setRanges(Lists.newArrayList(new Range()));
            scanner.fetchColumn(new Text(uriPrefix), new Text(typeToReplay.toString()));

            retVal = Lists.newArrayList();
            Map<String, Integer> uriToRetValPosition = Maps.newHashMap();
            for (Entry<Key, Value> entry : scanner) {
                long ts = entry.getKey().getTimestamp();
                String uri = WarehausUtils.getUriFromComputed(entry.getKey().getRow().toString());
                DateTime currentDateTime = TimeUtil.convertToThriftDateTime(ts);
                Visibility visibility = VisibilitySerialization.deserializeVisibilityWrappedValue(entry.getValue()).getVisibilityMarkings();
                DatedURI uriToAdd = new DatedURI(currentDateTime, uri, visibility);

                // If we're only replaying the latest, check if we've already inserted this URI into the list being returned.
                if (replayOnlyLatest) {
                    // If we've seen this URI already, check the timestamp that we've seen, and if it's older than what we
                    // currently have, replace it. Otherwise ignore it
                    if (uriToRetValPosition.containsKey(uri)) {
                        int position = uriToRetValPosition.get(uri);
                        long oldTimeStamp = TimeUtil.convertFromThriftDateTime(retVal.get(position).getTimestamp());
                        if (oldTimeStamp < ts) {
                            retVal.remove(position);
                            uriToRetValPosition.put(uri, retVal.size());
                            retVal.add(uriToAdd);
                        }
                    } else {
                        retVal.add(uriToAdd);
                    }
                } else {
                    retVal.add(uriToAdd);
                }
            }
        } catch (IOException e) {
            logger.error("Could not deserialize value from Accumulo", e);
            throw new TException("Could not retrieve data for request", e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        Collections.sort(retVal, new Comparator<DatedURI>() {
            @Override
            public int compare(DatedURI o1, DatedURI o2) {
                return o1.getTimestamp().compareTo(o2.getTimestamp());
            }
        });
        return retVal;
    }

    @Override
    public int replayCount(String urn, DateTime start, DateTime finish, GetDataType type, EzSecurityToken security) throws TException {
        securityClient.validateReceivedToken(security);
        logger.info("Next Replay Call is for count");
        return replay(urn, false, start, finish, type, security).size();
    }

    @Override
    public List<Long> getVersions(String uri, EzSecurityToken security) throws TException {
        String auths = WarehausUtils.getAuthsListFromToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getVersions");
        auditArgs.put("uri", uri);
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        List<Long> forReturn = Lists.newLinkedList();
        BatchScanner scanner = null;
        try {
            scanner = createScanner(auths);
            IteratorSetting iteratorSetting = new IteratorSetting(16, "warehausVersionsVisibilityIterator",
                    EzBakeVisibilityFilter.class);
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ), iteratorSetting);

            IteratorSetting is = new IteratorSetting(10, VersioningIterator.class);
            VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            scanner.addScanIterator(is);
            Key key = new Key(WarehausUtils.getKey(uri), new Text(WarehausUtils.getUriPrefixFromUri(uri)));
            scanner.setRanges(Lists.newArrayList(new Range(key, true, key.followingKey(PartialKey.ROW_COLFAM), false)));
            for (Entry<Key, Value> entry : scanner) {
                long ts = entry.getKey().getTimestamp();
                if (!forReturn.contains(ts)) {
                    forReturn.add(ts);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return forReturn;
    }

    @Override
    public IngestStatus insertView(ByteBuffer data, ViewId id, Visibility visibility, EzSecurityToken security) throws TException {
        securityClient.validateReceivedToken(security);
        IngestStatus status = new IngestStatus();
        Long timestamp =  id.isSetTimestamp() ? TimeUtil.convertFromThriftDateTime(id.getTimestamp()) :
                Calendar.getInstance().getTimeInMillis();
        status.setTimestamp(timestamp);

        try {
            checkWritePermission(id.getUri(), visibility, security, false);
        } catch (EzBakeAccessDenied ad) {
            status.setStatus(IngestStatusEnum.FAIL);
            status.setFailedURIs(Lists.newArrayList(id.getUri()));
            status.setFailureReason(ad.getMessage());
            logger.debug("insertView status : " + status);
            return status;
        }

        String accessorId = confirmToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "insertView");
        auditArgs.put("uri", id.getUri());
        auditArgs.put("accessorId", accessorId);
        AuditEventType eventType = id.isSetTimestamp() || id.isSquashPrevious() ? AuditEventType.FileObjectModify :
                AuditEventType.FileObjectCreate;
        auditLog(security, eventType, auditArgs);
        Mutation dataMutator = new Mutation(WarehausUtils.getKey(id.getUri()));
        try {
            dataMutator.put(new Text(WarehausUtils.getUriPrefixFromUri(id.getUri())), new Text(id.getSpacename() + "_" + id.getView()), new ColumnVisibility(PermissionUtils.getVisibilityString(visibility)), timestamp,
                    VisibilitySerialization.serializeVisibilityWithDataToValue(visibility, new TSerializer().serialize(new VersionControl(data, accessorId))));
        } catch (IOException e) {
            logger.error("Could not serialize value to insert into Accumulo", e);
            throw new TException("Could not insert data into the Warehaus", e);
        }

        if (id.isSquashPrevious()) {
            List<Range> ranges = Lists.newArrayList(Range.exact(WarehausUtils.getKey(id.getUri()), new Text(WarehausUtils.getUriPrefixFromUri(id.getUri())),
                    new Text(id.getSpacename() + "_" + id.getView())));
            BatchDeleter deleter = createDeleter(WarehausUtils.getAuthsListFromToken(security));
            deleter.setRanges(ranges);
            try {
                deleter.delete();
            } catch (Exception e) {
                logger.error("Failed to remove old views", e);
                throw new TException("Could not remove old views", e);
            } finally {
                deleter.close();
            }
        }


        BatchWriter writer = null;
        try {
            writer = createWriter();
            writeMutation(dataMutator, writer);
            flushWriter(writer);
        } finally {
            closeWriter(writer);
        }
        status.setStatus(IngestStatusEnum.SUCCESS);
        logger.debug("insertView status : " + status);
        return status;
    }

    @Override
    public BinaryReplay getLatestView(ViewId id, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getLatestView");
        auditArgs.put("uri", id.getUri());
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getLatest(id.getUri(), WarehausUtils.getUriPrefixFromUri(id.getUri()), id.getSpacename() + "_" + id.getView(), security);
    }

    @Override
    public BinaryReplay getView(ViewId id, long timestamp, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        securityClient.validateReceivedToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getView");
        auditArgs.put("uri", id.getUri());
        auditArgs.put("timestamp", String.valueOf(timestamp));
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);
        return getVersion(id.getUri(), WarehausUtils.getUriPrefixFromUri(id.getUri()), id.getSpacename() + "_" + id.getView(), timestamp, security);
    }

    @Override
    public void importFromHadoop(String filename, Visibility visibility, EzSecurityToken security) throws TException {
//        logRequest("importFromHadoop", filename, confirmToken(security), WarehausUtils.getAuthsListFromToken(security));
//        InputStream is;
//        try {
//            is = hdfs.open(new Path(filename));
//        } catch (IOException e) {
//            throw new TException(e);
//        }
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        try {
//        int character = is.read();
//            while (character > -1) {
//                baos.write(character);
//                character = is.read();
//            }
//        } catch (IOException e) {
//            throw new TException(e);
//        } finally {
//            Closeables.closeQuietly(is);
//        }
//        Repository data = new Repository();
//        new TDeserializer().deserialize(data, baos.toByteArray());
//        insert(data, visibility, security);
        throw new TException("This endpoint is not implemented");
    }

    @Override
    public IngestStatus updateEntry(UpdateEntry update, Visibility visibility, EzSecurityToken security) throws TException {
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "updateEntry");
        auditArgs.put("uri", update.getUri());
        auditLog(security, AuditEventType.FileObjectModify, auditArgs);
        IngestStatus status = insertUpdate(update, visibility, security);
        logger.debug("updateEntry status : " + status);
        return status;
    }

    private IngestStatus insertUpdate(UpdateEntry update, Visibility visibility, EzSecurityToken security) throws TException {

        try {
            checkWritePermission(update.getUri(), visibility, security, update.isUpdateVisibility());
        } catch (EzBakeAccessDenied ad) {
            IngestStatus status = new IngestStatus();
            status.setTimestamp(Calendar.getInstance().getTimeInMillis());
            status.setStatus(IngestStatusEnum.FAIL);
            status.setFailedURIs(Lists.newArrayList(update.getUri()));
            status.setFailureReason(ad.getMessage());
            return status;
        }

        String id = confirmToken(security);
        Map<String, VersionControl> parsed = Maps.newHashMap();
        Map<String, VersionControl> raw = Maps.newHashMap();
        Map<String, Boolean> updateVisibilityFlagMap = Maps.newHashMap();
        Map<String, Visibility> visibilityMap = Maps.newHashMap();

        if (update.isSetParsedData()) {
            VersionControl vc = new VersionControl(ByteBuffer.wrap(update.getParsedData()), id);
            parsed.put(update.getUri(), vc);
        }
        if (update.isSetRawData()) {
            VersionControl vc = new VersionControl(ByteBuffer.wrap(update.getRawData()), id);
            raw.put(update.getUri(), vc);
        }
        updateVisibilityFlagMap.put(update.getUri(), update.isUpdateVisibility());
        visibilityMap.put(update.getUri(), visibility);

        return updateEntries(Lists.newArrayList(update.getUri()), parsed, raw,
                updateVisibilityFlagMap, visibilityMap, security);
    }

    @Override
    public IngestStatus put(ezbake.warehaus.PutRequest putRequest, EzSecurityToken security) throws TException {
        String id = confirmToken(security);
        Map<String, VersionControl> parsedMap = Maps.newHashMap();
        Map<String, VersionControl> rawMap = Maps.newHashMap();
        Map<String, Boolean> updateVisibilityMap = Maps.newHashMap();
        List<String> uriList = Lists.newArrayList();
        Map<String, Visibility> visibilityMap = Maps.newHashMap();

        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "put");

        for (PutUpdateEntry putEntry : putRequest.getEntries()) {

            UpdateEntry update = putEntry.getEntry();

            uriList.add(update.getUri());
            updateVisibilityMap.put(update.getUri(), update.isUpdateVisibility());
            visibilityMap.put(update.getUri(), putEntry.getVisibility());

            if (update.isSetParsedData()) {
                VersionControl vc = new VersionControl(ByteBuffer.wrap(update.getParsedData()), id);
                parsedMap.put(update.getUri(), vc);
            }
            if (update.isSetRawData()) {
                VersionControl vc = new VersionControl(ByteBuffer.wrap(update.getRawData()), id);
                rawMap.put(update.getUri(), vc);
            }

            auditArgs.put("uri", update.getUri());
            auditLog(security, AuditEventType.FileObjectModify, auditArgs);
        }

        IngestStatus status = updateEntries(uriList, parsedMap, rawMap, updateVisibilityMap,
                visibilityMap, security);
        logger.debug("put status : " + status);
        return status;
    }

    private IngestStatus updateEntries(List<String> uriList,
                                       Map<String, VersionControl> parsedMap,
                                       Map<String, VersionControl> rawMap,
                                       Map<String, Boolean> updateVisibilityMap,
                                       Map<String, Visibility> visibilityMap,
                                       EzSecurityToken security) throws TException {

        String userAuths = WarehausUtils.getAuthsListFromToken(security);
        String id = confirmToken(security);
        long timestamp = Calendar.getInstance().getTimeInMillis();
        Set<GetDataType> dataTypes = Sets.newHashSet(GetDataType.values());
        Map<String, Mutation> mutationMap = Maps.newHashMap();
        List<Range> ranges = Lists.newArrayList();
        List<String> writableURIs = Lists.newArrayList();
        List<String> accessDenied = Lists.newArrayList();
        IngestStatus status = new IngestStatus();
        BatchScanner scanner = null;
        BatchWriter writer = null;

        // Below code is mostly organized to avoid scanning on a uri basis 
        // and instead take advantage of batch scans for improved performance.
        // A rough order of tasks -
        // 1. update visibilities, when requested and different from old ones
        // 2. add new rows for parsed/raw types, version index

        try {
            writer = createWriter();

            for (String uri : uriList) {
                for (GetDataType type : dataTypes) {
                    Key key = new Key(WarehausUtils.getKey(uri), new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(type.toString()));
                    ranges.add(new Range(key, true, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false));
                }
            }

            if (!ranges.isEmpty()) {
                try {
                    scanner = createScanner(userAuths);
                    scanner.setRanges(ranges);

                    // need existing group auths
                    IteratorSetting iteratorSetting = new IteratorSetting(21, "warehausEntriesVisibilityIterator",
                            EzBakeVisibilityFilter.class);
                    addEzBakeVisibilityFilter(scanner, security,
                            EnumSet.of(Permission.READ, Permission.MANAGE_VISIBILITY, Permission.WRITE), iteratorSetting);

                    for (Entry<Key, Value> entry : scanner) {
                        String uri = WarehausUtils.getUriFromKey(entry.getKey());
                        writableURIs.add(uri);

                        // update visibility of old entry if the flag is set.
                        if (updateVisibilityMap.get(uri)) {
                            long oldTimeStamp = entry.getKey().getTimestamp();

                            Visibility visibilityForUpdate = visibilityMap.get(uri);
                            ColumnVisibility oldVisibility = new ColumnVisibility(entry.getKey().getColumnVisibility());
                            // Update if new visibility is different than the old one.
                            if (visibilityForUpdate.toString().equals(oldVisibility.toString())) {
                                continue;
                            }
                            VisibilityWrapper wrapper = VisibilitySerialization.deserializeVisibilityWrappedValue(entry.getValue());
                            VersionControl value = ThriftUtils.deserialize(VersionControl.class, wrapper.getValue());
                            // Delete to ensure removal of entry with old visibility.
                            // Update only the visibility, leave everything else (incl. timestamp) as is.
                            Mutation mutation = mutationMap.get(uri);
                            if (mutation == null) {
                                mutation = new Mutation(WarehausUtils.getKey(uri));
                            }
                            mutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), oldVisibility, oldTimeStamp);
                            mutation.put(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), new ColumnVisibility(PermissionUtils.getVisibilityString(visibilityForUpdate)), oldTimeStamp,
                                    VisibilitySerialization.serializeVisibilityWithDataToValue(visibilityForUpdate, ThriftUtils.serialize(new VersionControl(ByteBuffer.wrap(value.getPacket()), id))));

                            mutationMap.put(uri, mutation);
                        }
                    }
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            }

            // Scan for existing URIs since writableURIs would only contain
            // those that user can write to in case of updates. 
            // This will let distinguish between inserts and updates.
            List<String> existingURIs = Lists.newArrayList();
            if (!ranges.isEmpty()) {
                try {
                    scanner = createScanner(userAuths);
                    scanner.setRanges(ranges);

                    for (Entry<Key, Value> entry : scanner) {
                        String uri = WarehausUtils.getUriFromKey(entry.getKey());
                        existingURIs.add(uri);
                    }
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            }

            // Add new updates to parsed/raw data
            for (String uri : uriList) {
                // updates should be writable
                if (existingURIs.contains(uri) && !writableURIs.contains(uri)) {
                    accessDenied.add(uri);
                    continue;
                }
                Visibility visibilityForUpdate = visibilityMap.get(uri);
                try {
                    checkWritePermission(uri, visibilityForUpdate, security, false);
                } catch (EzBakeAccessDenied ad) {
                    accessDenied.add(uri);
                    continue;
                }

                Mutation mutation = mutationMap.get(uri);
                if (mutation == null) {
                    mutation = new Mutation(WarehausUtils.getKey(uri));
                }

                if (parsedMap.containsKey(uri)) {
                    mutation.put(new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(GetDataType.PARSED.toString()),
                            new ColumnVisibility(PermissionUtils.getVisibilityString(visibilityForUpdate)), timestamp,
                            VisibilitySerialization.serializeVisibilityWithDataToValue(visibilityForUpdate,
                                    ThriftUtils.serialize(
                                            new VersionControl(ByteBuffer.wrap(parsedMap.get(uri).getPacket()), id))));
                }
                if (rawMap.containsKey(uri)) {
                    mutation.put(new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(GetDataType.RAW.toString()),
                            new ColumnVisibility(PermissionUtils.getVisibilityString(visibilityForUpdate)), timestamp,
                            VisibilitySerialization.serializeVisibilityWithDataToValue(visibilityForUpdate,
                                    ThriftUtils.serialize(
                                            new VersionControl(ByteBuffer.wrap(rawMap.get(uri).getPacket()), id))));
                }

                writeMutation(mutation, writer);
            }

            flushWriter(writer);
        } catch (IOException e) {
            logger.error("Could not deserialize value from Accumulo", e);
            throw new TException("Could not retrieve data for request", e);
        } finally {
            closeWriter(writer);
        }

        status.setTimestamp(timestamp);
        status.setStatus(IngestStatusEnum.SUCCESS);
        if (!accessDenied.isEmpty()) {
            if (accessDenied.size() == uriList.size()) {
                status.setStatus(IngestStatusEnum.FAIL);
            } else {
                status.setStatus(IngestStatusEnum.PARTIAL);
            }
            status.setFailedURIs(accessDenied);
            status.setFailureReason("Given user token does not have the "
                    + "required authorizations to update documents with listed URIs");
            return status;
        }

        return status;
    }

    /**
     * @throws EntryNotInWarehausException If the document identified by the
     *                                     given URI was not found.
     * @throws TException                  If an error occurs during the fetching of the entry.
     */
    @Override
    public ezbake.warehaus.EntryDetail getEntryDetails(String uri,
                                                       ezbake.base.thrift.EzSecurityToken security) throws org.apache.thrift.TException {
        securityClient.validateReceivedToken(security);
        String auths = WarehausUtils.getAuthsListFromToken(security);
        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "getEntryDetails");
        auditArgs.put("uri", uri);
        auditLog(security, AuditEventType.FileObjectAccess, auditArgs);

        List<Long> forReturn = Lists.newLinkedList();
        List<VersionDetail> versions = Lists.newLinkedList();
        BatchScanner scanner = null;
        int count = 0;

        try {
            scanner = createScanner(auths);
            IteratorSetting iteratorSetting = new IteratorSetting(27, "warehausEntryDetailVisibilityIterator",
                    EzBakeVisibilityFilter.class);
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ), iteratorSetting);

            IteratorSetting is = new IteratorSetting(10, VersioningIterator.class);
            VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            scanner.addScanIterator(is);
            Key key = new Key(WarehausUtils.getKey(uri), new Text(WarehausUtils.getUriPrefixFromUri(uri)));
            scanner.setRanges(Lists.newArrayList(new Range(key, true, key.followingKey(PartialKey.ROW_COLFAM), false)));
            for (Entry<Key, Value> entry : scanner) {
                count++;
                long ts = entry.getKey().getTimestamp();
                if (!forReturn.contains(ts)) {
                    forReturn.add(ts);
                    VersionDetail vd = new VersionDetail();
                    vd.setUri(uri);
                    vd.setTimestamp(ts);

                    VisibilityWrapper wrapper = VisibilitySerialization.deserializeVisibilityWrappedValue(entry.getValue());
                    vd.setVisibility(wrapper.getVisibilityMarkings());

                    VersionControl versionData = ThriftUtils.deserialize(VersionControl.class, wrapper.getValue());
                    vd.setSecurityId(versionData.getName());
                    versions.add(vd);
                }
            }
        } catch (IOException e) {
            logger.error("Could not deserialize the data from Accumulo associated with " + uri + ".", e);
            throw new TException("Could not deserialize the data from Accumulo associated with " + uri + ".", e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        if (count == 0) {
            logger.debug("The following document URI was not found in the warehouse: " + uri);
            throw new EntryNotInWarehausException("The following document URI was not found in the warehouse: " + uri);
        }

        EntryDetail entryDetail = new EntryDetail();
        entryDetail.setUri(uri);
        entryDetail.setVersions(versions);
        return entryDetail;
    }

    /*********************************************************
     /*
     /*        Helper Functions
     /*
     *********************************************************/

    /**
     * Validates the security token with the security service and returns either the application security ID or
     * the user DN associated with the token.
     *
     * @param security security token to validate
     * @return the ID (either application security ID or user DN) associated with the token
     * @throws TException
     */
    private String confirmToken(EzSecurityToken security) throws TException {
        securityClient.validateReceivedToken(security);

        String id;
        if (security.getType() == TokenType.APP) {
            id = security.getValidity().getIssuedTo();
        } else {
            id = security.getTokenPrincipal().getPrincipal();
        }
        return id;
    }

    /**
     * <p>
     * Ensures that the warehaus table is in accumulo and attempts to create
     * it if it does not exist.
     * Likewise, checks that the warehaus purge table is in accumulo and will
     * recreate the table if it does not exist.
     * </p>
     *
     * @throws TException If an error occurs while checking for the accumulo
     *                    namespace, warehaus table or warehaus purge table. If an
     *                    error occurs while creating the accumulo namespace, warehaus
     *                    table or warehaus purge table.
     */
    private void ensureTable() throws TException {

        try {
            if (!connector.namespaceOperations().exists(accumuloNamespace)) {
                logger.warn("The accumulo namespace '" + accumuloNamespace + "' does not exist. An attempt to create namespace will start now.");
                connector.namespaceOperations().create(accumuloNamespace);
                logger.warn("The accumulo namespace '" + accumuloNamespace + "' was created.");
            }
        } catch (Exception e) {
            logger.error("An error occurred while checking for the existence of or while creating the accumulo namespace '" + accumuloNamespace + "'.");
            throw new TException(e);
        }

        try {
            if (!connector.tableOperations().exists(WarehausConstants.TABLE_NAME)) {
                logger.warn("The warehaus table '" + WarehausConstants.TABLE_NAME + "' does not exist. An attempt to create the table will start now.");
                connector.tableOperations().create(WarehausConstants.TABLE_NAME, false);
                logger.warn("The warehaus table '" + WarehausConstants.TABLE_NAME + "' was created.");

                logger.info("Adding table splits");
                String splitsAsString = getConfigurationProperties().getProperty(WarehausConstants.WAREHAUS_SPLITS_KEY, WarehausConstants.DEFAULT_WAREHAUS_SPLITS);
                String splitsArray[] = splitsAsString.split(",");
                List<Text> splitsAsText = Lists.transform(Arrays.asList(splitsArray), new Function<String, Text>() {
                    @Override
                    public Text apply(String input) {
                        return new Text(input);
                    }
                });
                SortedSet<Text> splits = new TreeSet<>(splitsAsText);
                connector.tableOperations().addSplits(WarehausConstants.TABLE_NAME, splits);
            }
        } catch (Exception e) {
            logger.error("An error occurred while checking for the existence of or while creating the warehaus table '" + WarehausConstants.TABLE_NAME + "'.", e);
            throw new TException(e);
        }

        try {
            if (!connector.tableOperations().exists(WarehausConstants.PURGE_TABLE_NAME)) {
                logger.warn("The warehaus purge table '" + WarehausConstants.PURGE_TABLE_NAME + "' does not exist. An attempt to create the table will start now.");
                connector.tableOperations().create(WarehausConstants.PURGE_TABLE_NAME, true);
                logger.warn("The warehaus purge table '" + WarehausConstants.PURGE_TABLE_NAME + "' was created.");
            }
        } catch (Exception e) {
            logger.error("An error occurred while checking for the existence of or while creating the warehaus purge table '" + WarehausConstants.PURGE_TABLE_NAME + "'.", e);
            throw new TException(e);
        }

        try {
            String ezBatchUser = getConfigurationProperties().getProperty("ezbatch.user", "ezbake");
            connector.securityOperations().grantTablePermission(ezBatchUser, WarehausConstants.TABLE_NAME, TablePermission.READ);
            logger.info("READ permission granted to ezbatch user");
        } catch (Exception e) {
            logger.error("An error occurred while trying to give the ezbatch user access");
            throw new TException(e);
        }
    }

    private BatchScanner createScanner(String auths) throws TException {
        try {
            return connector.createBatchScanner(WarehausConstants.TABLE_NAME, WarehausUtils.getAuthsFromString(auths), WarehausConstants.QUERY_THREADS);
        } catch (TableNotFoundException e) {
            throw new TException(e);
        }
    }

    private BatchDeleter createDeleter(String auths) throws TException {
        try {
            EzProperties properties = new EzProperties(getConfigurationProperties(), false);
            long maxMemory = properties.getLong(WarehausConstants.BATCH_WRITER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_WRITER_MAX_MEMORY);
            long latency = properties.getLong(WarehausConstants.BATCH_WRITER_LATENCY_MS_KEY, WarehausConstants.DEFAULT_LATENCY);
            int threads = properties.getInteger(WarehausConstants.BATCH_WRITER_WRITE_THREADS_KEY, WarehausConstants.DEFAULT_WRITE_THREADS);
            BatchWriterConfig config = new BatchWriterConfig().setMaxLatency(latency, TimeUnit.MILLISECONDS).setMaxMemory(maxMemory).setMaxWriteThreads(threads);
            return connector.createBatchDeleter(WarehausConstants.TABLE_NAME, WarehausUtils.getAuthsFromString(auths), WarehausConstants.QUERY_THREADS, config);
        } catch (TableNotFoundException e) {
            throw new TException(e);
        }
    }

    private BatchWriter createWriter() throws TException {
        try {
            EzProperties properties = new EzProperties(getConfigurationProperties(), false);
            long maxMemory = properties.getLong(WarehausConstants.BATCH_WRITER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_WRITER_MAX_MEMORY);
            long latency = properties.getLong(WarehausConstants.BATCH_WRITER_LATENCY_MS_KEY, WarehausConstants.DEFAULT_LATENCY);
            int threads = properties.getInteger(WarehausConstants.BATCH_WRITER_WRITE_THREADS_KEY, WarehausConstants.DEFAULT_WRITE_THREADS);
            BatchWriterConfig config = new BatchWriterConfig().setMaxLatency(latency, TimeUnit.MILLISECONDS).setMaxMemory(maxMemory).setMaxWriteThreads(threads);

            BatchWriter writer = connector.createBatchWriter(WarehausConstants.TABLE_NAME, config);
            logger.debug("Writer initialized with max memory of {}, latency of {}, and {} threads", maxMemory, latency, threads);
            return writer;
        } catch (TableNotFoundException e) {
            logger.error("Could not initialize batch writer because table is missing", e);
            throw new RuntimeException(e);
        }
    }

    private void writeMutation(Mutation mutator, BatchWriter writer) throws TException {
        try {
            writer.addMutation(mutator);
        } catch (MutationsRejectedException e) {
            throw new TException(e);
        }
    }

    private void flushWriter(BatchWriter writer) throws TException {
        try {
            if (writer != null) { // shouldn't normally be null, but anyway
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new TException(e);
        }
    }

    private void closeWriter(BatchWriter writer) throws TException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (MutationsRejectedException e) {
            throw new TException(e);
        }
    }

    private void addEzBakeVisibilityFilter(ScannerBase scanner, EzSecurityToken token,
                                           Set<Permission> permissions, IteratorSetting iteratorSetting)
            throws TException {
        iteratorSetting.clearOptions();
        EzBakeVisibilityFilter.setOptions(iteratorSetting, token.getAuthorizations(), permissions);
        scanner.addScanIterator(iteratorSetting);
    }

    private void checkWritePermission(String uri, Visibility visibility, EzSecurityToken token,
                                      boolean updateVisibility) throws EzBakeAccessDenied {
        if (!PermissionUtils.getPermissions(token.getAuthorizations(), visibility, true).contains(Permission.WRITE)) {
            throw new EzBakeAccessDenied().setMessage("Given user token does not have the "
                    + "required authorizations to add/update document with uri " + uri);
        }
        if (updateVisibility
                && !PermissionUtils.getPermissions(token.getAuthorizations(), visibility, true).contains(Permission.MANAGE_VISIBILITY)) {
            throw new EzBakeAccessDenied().setMessage("Given user token does not have the "
                    + "required authorizations to add/update visibilty of document with uri " + uri);
        }
    }

    private void auditLog(EzSecurityToken userToken, AuditEventType eventType, Map<String, String> args) {
        AuditEvent event = new AuditEvent(eventType, userToken);
        for (String argName : args.keySet()) {
            event.arg(argName, args.get(argName));
        }
        if (auditLogger != null) {
            auditLogger.logEvent(event);
        }
    }

    /**
     * ******************************************************
     * /*
     * /*        Code Consolidation
     * /*
     * *******************************************************
     */

    private BinaryReplay getLatest(String uri, String columnFamily, String columnQualifier, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        List<Key> keys = Lists.newArrayList(new Key(WarehausUtils.getKey(uri), new Text(columnFamily), new Text(columnQualifier)));
        List<BinaryReplay> results = null;
        try {
            results = getLatest(keys, security);
            if (results.size() == 0) {
                throw new EntryNotInWarehausException(String.format("No entry found in warehaus for uri %s, and data type %s", uri, columnQualifier));
            }
            return results.get(0);
        } catch (MaxGetRequestSizeExceededException ex) {
            // should not really happen when fetching one specific key.
            logger.error("Batch scan max memory exceeded error occured.", ex);
            throw new TException(ex);
        }
    }

    private List<BinaryReplay> getLatest(List<Key> keys, EzSecurityToken security)
            throws TException, MaxGetRequestSizeExceededException {
        List<BinaryReplay> results = Lists.newArrayList();
        if (keys == null || keys.size() == 0) {
            return results;
        }

        String auths = WarehausUtils.getAuthsListFromToken(security);
        BatchScanner scanner = null;

        try {
            scanner = createScanner(auths);

            IteratorSetting iteratorSetting = new IteratorSetting(33, "warehausLatestVisibilityIterator",
                    EzBakeVisibilityFilter.class);
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ), iteratorSetting);

            List<Range> ranges = Lists.newArrayList();
            for (Key key : keys) {
                Range range = new Range(key, true, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
                ranges.add(range);
            }
            scanner.setRanges(ranges);

            Map<String, Long> uriTimestamps = Maps.newHashMap();
            Map<String, BinaryReplay> uriRetVals = Maps.newHashMap();

            long currentScanSize = 0l;
            long maxBatchScanSize = new EzProperties(this.getConfigurationProperties(), false).getLong(
                    WarehausConstants.BATCH_SCANNER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_SCANNER_MAX_MEMORY);
            for (Entry<Key, Value> latest : scanner) {
                if (latest != null) {
                    String uri = WarehausUtils.getUriFromKey(latest.getKey());
                    long ts = latest.getKey().getTimestamp();
                    // Do some kludgy stuff with timestamp checks to ensure
                    // we're only grabbing the latest.
                    // Ideally, we'd like to use a VersioningIterator with 
                    // maxVersions set to 1 that gets us the latest, but that
                    // won't always work because updateEntry() doesn't always
                    // update all versions of a uri with the same visibility.
                    if ((!uriTimestamps.containsKey(uri)) ||
                            (uriTimestamps.containsKey(uri) && uriTimestamps.get(uri).longValue() < ts)) {

                        BinaryReplay forReturn = new BinaryReplay();
                        VisibilityWrapper visibilityAndValue = VisibilitySerialization.deserializeVisibilityWrappedValue(latest.getValue());
                        VersionControl versionData = ThriftUtils.deserialize(VersionControl.class, visibilityAndValue.getValue());
                        forReturn.setPacket(versionData.getPacket());
                        forReturn.setTimestamp(TimeUtil.convertToThriftDateTime(ts));
                        forReturn.setUri(uri);
                        forReturn.setVisibility(visibilityAndValue.getVisibilityMarkings());

                        // if max batch scan size exceeded, break
                        int len = ThriftUtils.serialize(forReturn).length;
                        currentScanSize = currentScanSize + len;
                        if (currentScanSize > maxBatchScanSize) {
                            throw new MaxGetRequestSizeExceededException("Max get request size of " + maxBatchScanSize + " exceeded. "
                                    + "Configure the " + WarehausConstants.BATCH_SCANNER_MAX_MEMORY_KEY + " property appropriately and re-try");
                        }
                        uriRetVals.put(uri, forReturn);
                        uriTimestamps.put(uri, ts);
                    }
                }
            }
            results.addAll(uriRetVals.values());
        } catch (IOException e) {
            logger.error("Could not deserialize value from Accumulo", e);
            throw new TException("Could not retrieve data for request", e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return results;
    }

    private BinaryReplay getVersion(String uri, String columnFamily, String columnQualifier, long timestamp, EzSecurityToken security) throws TException, EntryNotInWarehausException {
        List<Key> keys = Lists.newArrayList(new Key(WarehausUtils.getKey(uri), new Text(columnFamily), new Text(columnQualifier), timestamp));
        List<BinaryReplay> results = null;
        try {
            results = getVersion(keys, security);
            if (results.size() == 0) {
                throw new EntryNotInWarehausException(String.format("No entry found in warehaus for uri %s, and data type %s, at time %s", uri, columnQualifier, timestamp));
            }
            return results.get(0);
        } catch (MaxGetRequestSizeExceededException ex) {
            // should not really happen when fetching one specific key.
            logger.error("Batch scan max memory exceeded error occured.", ex);
            throw new TException(ex);
        }
    }

    private List<BinaryReplay> getVersion(List<Key> keys, EzSecurityToken security)
            throws TException, MaxGetRequestSizeExceededException {
        String auths = WarehausUtils.getAuthsListFromToken(security);
        List<BinaryReplay> results = Lists.newArrayList();
        if (keys == null || keys.size() == 0) {
            return results;
        }

        Map<String, Long> uriTimestamps = Maps.newHashMap();

        BatchScanner scanner = null;
        try {
            scanner = createScanner(auths);

            IteratorSetting iteratorSetting = new IteratorSetting(41, "warehausVersionVisibilityIterator",
                    EzBakeVisibilityFilter.class);
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ), iteratorSetting);

            List<Range> ranges = Lists.newArrayList();
            for (Key key : keys) {
                Range range = new Range(key, true, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
                ranges.add(range);
                uriTimestamps.put(WarehausUtils.getUriFromKey(key), key.getTimestamp());
            }
            scanner.setRanges(ranges);

            // We don't know what maxVersions is going to be configured as on the accumulo cluster, so lets be safe
            // here and return MAX_VALUE versions for this scanner
            IteratorSetting is = new IteratorSetting(10, VersioningIterator.class);
            VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            scanner.addScanIterator(is);

            long currentScanSize = 0l;
            long maxBatchScanSize = new EzProperties(this.getConfigurationProperties(), false).getLong(
                    WarehausConstants.BATCH_SCANNER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_SCANNER_MAX_MEMORY);
            for (Entry<Key, Value> entry : scanner) {
                if (entry != null) {
                    String uri = WarehausUtils.getUriFromKey(entry.getKey());
                    long ts = entry.getKey().getTimestamp();
                    // I REALLY wanted to use the TimestampFilter iterator provided with Accumulo here, but it does not work at
                    // millisecond granularity. Which is ridiculous since Accumulo stores timestamps with millisecond granularity.
                    // Doesn't make a lot of sense...does it? /rant
                    // So here's some kludgy stuff to filter by timestamp.
                    if (ts == uriTimestamps.get(uri).longValue()) {
                        BinaryReplay forReturn = new BinaryReplay();
                        VisibilityWrapper visibilityAndValue = VisibilitySerialization.deserializeVisibilityWrappedValue(entry.getValue());
                        VersionControl versionData = ThriftUtils.deserialize(VersionControl.class, visibilityAndValue.getValue());
                        forReturn.setPacket(versionData.getPacket());
                        forReturn.setTimestamp(TimeUtil.convertToThriftDateTime(ts));
                        forReturn.setUri(uri);
                        forReturn.setVisibility(visibilityAndValue.getVisibilityMarkings());

                        // if max batch scan size exceeded, break
                        int len = ThriftUtils.serialize(forReturn).length;
                        currentScanSize = currentScanSize + len;
                        if (currentScanSize > maxBatchScanSize) {
                            throw new MaxGetRequestSizeExceededException("Max get request size of " + maxBatchScanSize + " exceeded. "
                                    + "Configure the " + WarehausConstants.BATCH_SCANNER_MAX_MEMORY_KEY + " property appropriately and re-try");
                        }
                        results.add(forReturn);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Could not deserialize value from Accumulo", e);
            throw new TException("Could not retrieve data for request", e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return results;
    }

    protected void resetTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TException {
        connector.tableOperations().delete(WarehausConstants.TABLE_NAME);
        connector.tableOperations().delete(WarehausConstants.PURGE_TABLE_NAME);
        ensureTable();
    }

    /**
     * Start a purge of the given items. This method will begin purging items
     * that match the given a list of ids to purge and will call back to the
     * purgeCallbackService when the purge has completed. The return of this
     * function without exception indicates that the application has taken
     * responsibility of purging documents matching purgeIds from its data sets.
     * It does not indicate completion of the purge.
     * <p/>
     * Returns the state of the new purge request.
     *
     * @param purgeCallbackService ezDiscovery path of the purge service to call
     *                             back.
     * @param purgeId              Unique id to use for this purge request.d should
     *                             not take any action based on that fact. Required.
     * @param idsToPurge           A set containing all the items to purge. This should
     *                             be sent to the data access layer to perform the purge.
     * @param initiatorToken       Security token for the service or user that
     *                             initiated the purge.
     * @throws PurgeException If the purgeId is null or empty.
     * @throws TException     If an error occurred during the processing of the
     *                        purge.
     */
    @Override
    public PurgeState beginPurge(String purgeCallbackService, long purgeId, Set<Long> idsToPurge, EzSecurityToken initiatorToken)
            throws PurgeException, EzSecurityTokenException, TException {

        if (initiatorToken == null) {
            throw new TException("The security token was not provided when requesting the warehaus purge.");
        }

        if (!isPurgeAppSecurityId(initiatorToken)) {
            throw new TException("A warehaus purge may only be initiated by the Central Purge Service.");
        }

        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "beginPurge");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(initiatorToken, AuditEventType.FileObjectDelete, auditArgs);

        PurgeState purgeState = createDefaultPurgeState(purgeId);
        purgeState.setPurgeStatus(PurgeStatus.STARTING);
        insertPurgeStatus(purgeState);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new WarehousePurger(purgeId, idsToPurge, initiatorToken));
        executorService.shutdown();

        return purgeState;
    }

    @Override
    public PurgeState beginVirusPurge(String purgeCallbackService, long purgeId, Set<Long> idsToPurge, EzSecurityToken initiatorToken)
            throws PurgeException, EzSecurityTokenException, TException {

        return this.beginPurge(purgeCallbackService, purgeId, idsToPurge, initiatorToken);
    }

    /**
     * <p>
     * Returns the most recent state of a given purge request.
     * </p>
     *
     * @param purgeId Unique id to use for this purge request
     * @returns Status of the given purge, UNKNOWN_ID if it was not found
     */
    @Override
    public PurgeState purgeStatus(EzSecurityToken token, long purgeId)
            throws EzSecurityTokenException, TException {

        if (token == null) {
            throw new TException("The security token was not provided when requesting the warehaus purge status.");
        }

        if (!isPurgeAppSecurityId(token)) {
            throw new TException("A warehaus purge status may only be initiated by the Central Purge Service.");
        }

        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "purgeStatus");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(token, AuditEventType.FileObjectAccess, auditArgs);

        PurgeState state = createDefaultPurgeState(purgeId);
        state.setPurgeStatus(PurgeStatus.UNKNOWN_ID);

        Scanner scanner = null;
        try {
            scanner = connector.createScanner(
                    WarehausConstants.PURGE_TABLE_NAME,
                    WarehausUtils.getAuthsFromString(WarehausUtils.getAuthsListFromToken(token)));

            IteratorSetting is = new IteratorSetting(10, VersioningIterator.class);
            VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            scanner.addScanIterator(is);

            scanner.setRange(new Range(new Text(String.valueOf(purgeId))));

            int entriesFoundCount = 0;
            for (Entry<Key, Value> entry : scanner) {
                entriesFoundCount++;
                state = ThriftUtils.deserialize(PurgeState.class, entry.getValue().get());
            }
            if (entriesFoundCount > 1) {
                logger.warn("A total of {} entries were found in the warehaus purge db when searching for the purge id #{}. Expected only the return of the most recent record.",
                        entriesFoundCount, purgeId);
            }
        } catch (TableNotFoundException e) {
            throw new TException(e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return state;
    }

    /**
     * <p>
     * Cancelling a purge from the warehouse may not occur. As a result, the
     * cancel status will be set to {@link CancelStatus#CANNOT_CANCEL}.
     * </p>
     */
    @Override
    public PurgeState cancelPurge(EzSecurityToken token, long purgeId)
            throws EzSecurityTokenException, TException {

        if (token == null) {
            throw new TException("The security token was not provided when requesting the warehaus purge cancellation.");
        }

        if (!isPurgeAppSecurityId(token)) {
            throw new TException("A warehaus purge cancellation may only be initiated by the Central Purge Service.");
        }

        HashMap<String, String> auditArgs = Maps.newHashMap();
        auditArgs.put("action", "cancelPurge");
        auditArgs.put("purgeId", Long.toString(purgeId));
        auditLog(token, AuditEventType.FileObjectModify, auditArgs);

        PurgeState state = this.purgeStatus(token, purgeId);
        if (this.mayCancelPurgeProceed(state)) {
            state.setCancelStatus(CancelStatus.CANCELED);
            state.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
        } else {
            state.setCancelStatus(CancelStatus.CANNOT_CANCEL);
        }
        state.setTimeStamp(TimeUtil.getCurrentThriftDateTime());
        this.insertPurgeStatus(state);

        return state;
    }

    /**
     * <p>
     * Answers true if the give PurgeState is in a state where it may proceed
     * with a cancellation and false if it is not.
     * </p>
     *
     * @param state The PurgeState which is evaluated to determine if purge
     *              cancellation is permitted. If this is null the false is returned.
     */
    boolean mayCancelPurgeProceed(PurgeState state) {
        return state != null &&
                CancelStatus.NOT_CANCELED.equals(state.getCancelStatus()) &&
                (PurgeStatus.WAITING_TO_START.equals(state.getPurgeStatus()) ||
                        PurgeStatus.STARTING.equals(state.getPurgeStatus()));
    }

    /**
     * <p/>
     * Creates a PurgeState record with default values. By default, the
     * purge status is set to {@link PurgeStatus#WAITING_TO_START} and the
     * cancel status is set to {@link CancelStatus#NOT_CANCELED}.
     *
     * @param purgeId The purge id value to which the purgeId attribute
     *                is set.
     * @return A new instance of a PurgeState containing the given purgeId
     * and the default values.
     */
    PurgeState createDefaultPurgeState(long purgeId) {
        PurgeState state = new PurgeState();
        state.setCancelStatus(CancelStatus.NOT_CANCELED);
        state.setNotPurged(new TreeSet<Long>());
        state.setPurged(new TreeSet<Long>());
        state.setPurgeId(purgeId);
        state.setSuggestedPollPeriod(2000);
        state.setTimeStamp(TimeUtil.getCurrentThriftDateTime());
        state.setPurgeStatus(PurgeStatus.WAITING_TO_START);
        return state;
    }

    /**
     * <p>
     * Inserts a purge status record for the given collection of URIs. The state
     * of the purge is determined by the status given in the purgeState parameter.
     * </p>
     *
     * @param purgeState The state of the purge. This will be persisted for each
     *                   URI given in the uris parameter. Required. The purgeId is required.
     * @throws TException If an error occurs while writing to the purge table.
     *                    If either purgeId or purgeStatus are empty or null.
     */
    void insertPurgeStatus(PurgeState purgeState) throws TException {

        if (purgeState == null) {
            throw new TException("The purge state is required for inserting a warehaus purge record.");
        }

        Visibility visibility = new Visibility();
        visibility.setFormalVisibility(this.purgeVisibility);

        BatchWriter writer = null;
        writer = createPurgeWriter();

        Mutation m = new Mutation(new Text(String.valueOf(purgeState.getPurgeId())));
        try {
            m.put(new Text(""),
                    new Text(""),
                    new ColumnVisibility(visibility.getFormalVisibility()),
                    Calendar.getInstance().getTimeInMillis(),
                    new Value(ThriftUtils.serialize(purgeState)));
            writer.addMutation(m);
        } catch (MutationsRejectedException e) {
            logger.error("The write to the warehaus purge table failed for Purge Id '" + purgeState.getPurgeId() + "'.", e);
            throw new TException(e);
        } finally {
            try {
                flushWriter(writer);
            } finally {
                closeWriter(writer);
            }
        }
    }

    /**
     * <p>
     * Removes the warehaus entries identified by the given list of URIs.
     * </p>
     *
     * @param uriList        A collection of URIs that uniquely identify the warehaus
     *                       entries to remove. If this is null or empty then no processing
     *                       occurs.
     * @param initiatorToken The security token. Required.
     * @throws Exception If an error occurs while deleting the warehaus entries.
     */
    public void remove(Collection<String> uriList, EzSecurityToken initiatorToken) throws Exception {

        if (uriList == null || uriList.isEmpty()) return;

        BatchDeleter deleter = null;
        List<Range> ranges = Lists.newArrayList();

        try {
            for (String uri : uriList) {
                ranges.add(Range.exact(WarehausUtils.getKey(uri)));
            }
            deleter = createDeleter(WarehausUtils.getAuthsListFromToken(initiatorToken));
            deleter.setRanges(ranges);
            deleter.delete();
        } finally {
            if (deleter != null) {
                deleter.close();
            }
        }
    }

    /**
     * <p>
     * Given a set of purge bit ids, return the corresponding URI for each
     * purge bit id as one collection. To see the mapping of purge bit id
     * to URI then call #getUriMapping.
     * </p>
     *
     * @param idsToPurge    All of he id of URIs used by the purge service that
     *                      are referenced in a single purge request. Required.
     * @param securityToken The security token. Required.
     * @return
     * @throws TException If an error occurs when translating the bitvector
     *                    from the provenance service.
     */
    private Collection<String> getUris(Set<Long> idsToPurge, EzSecurityToken securityToken) throws TException {

        Map<Long, String> map = this.getUriMapping(idsToPurge, securityToken);
        Collection<String> uris = map.values();

        return uris == null ? new ArrayList<String>() : uris;
    }

    /**
     * <p>
     * Given a set of ids, return a mapping of ids to URIs.
     * </p>
     *
     * @param idsToPurge    All of he id of URIs used by the purge service that
     *                      are referenced in a single purge request. Required.
     * @param securityToken The security token. Required.
     * @return A map where the value is the URI.
     * @throws TException If an error occurs when translating the bitvector
     *                    from the provenance service.
     */
    private Map<Long, String> getUriMapping(Set<Long> idsToPurge, EzSecurityToken securityToken) throws TException {

        ThriftClientPool pool = new ThriftClientPool(this.getConfigurationProperties());
        ProvenanceService.Client client = null;
        try {
            client = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
        } finally {
            if (pool != null) pool.close();
        }
        ArrayList<Long> idsToPurgeList = new ArrayList<>();
        idsToPurgeList.addAll(idsToPurge);
        EzSecurityTokenWrapper chainedToken = securityClient.fetchDerivedTokenForApp(securityToken, pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME));
        PositionsToUris uriPositions = client.getDocumentUriFromId(chainedToken, idsToPurgeList);

        return uriPositions.getMapping();
    }

    /**
     * <p>
     * Answers true if the given security token has an application security id
     * that is equal to the application security id from the purge service. If
     * they are not equivalent then false is returned.
     * </p>
     *
     * @param securityToken The security token that is checked to determine if
     *                      it is from the purge service. Required.
     * @return True if the token has an application security id that matches
     * purge service's application security id and false if not.
     */
    private boolean isPurgeAppSecurityId(EzSecurityToken securityToken) throws EzSecurityTokenException {
        EzSecurityTokenWrapper securityWrapper = new EzSecurityTokenWrapper(securityToken);
        securityClient.validateReceivedToken(securityToken);
        return securityWrapper.getSecurityId().equals(this.getPurgeAppSecurityId());
    }

    /**
     * <p>
     * Returns the application securityId for the purge service.
     * </p>
     * <p/>
     * This can be moved to the {@link #getThriftProcessor()} method.
     *
     * @return The application security id for the purge service.
     */
    private String getPurgeAppSecurityId() {

        if (this.purgeAppSecurityId == null) {
            ThriftClientPool pool = new ThriftClientPool(this.getConfigurationProperties());
            purgeAppSecurityId = pool.getSecurityId(ezCentralPurgeServiceConstants.SERVICE_NAME);
            pool.close();
        }
        return purgeAppSecurityId;
    }

    /**
     * <p>
     * Create a writer instance for the purge table.
     * </p>
     *
     * @return An accumulo batch writer.
     * @throws TException If the purge table could not be found.
     */
    private BatchWriter createPurgeWriter() throws TException {
        try {
            EzProperties properties = new EzProperties(getConfigurationProperties(), false);
            BatchWriterConfig writerConfig = new BatchWriterConfig()
                    .setMaxLatency(properties.getLong(WarehausConstants.BATCH_WRITER_LATENCY_MS_KEY, WarehausConstants.DEFAULT_LATENCY), TimeUnit.MILLISECONDS)
                    .setMaxMemory(properties.getLong(WarehausConstants.BATCH_WRITER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_WRITER_MAX_MEMORY))
                    .setMaxWriteThreads(properties.getInteger(WarehausConstants.BATCH_WRITER_WRITE_THREADS_KEY, WarehausConstants.DEFAULT_WRITE_THREADS));
            return connector.createBatchWriter(WarehausConstants.PURGE_TABLE_NAME, writerConfig);
        } catch (TableNotFoundException e) {
            logger.error("A batch writer could not be initialized for the '" + WarehausConstants.PURGE_TABLE_NAME + "' table because it is missing.", e);
            throw new RuntimeException(e);
        }
    }

    private class WarehousePurger implements Runnable {

        private long purgeId;
        private EzSecurityToken initiatorToken;
        private Set<Long> idsToPurge;
        private Visibility visibility;

        WarehousePurger(long purgeId, Set<Long> idsToPurge, EzSecurityToken initiatorToken) {
            this.purgeId = purgeId;
            this.idsToPurge = idsToPurge;
            this.initiatorToken = initiatorToken;
            this.visibility = new Visibility();
            this.visibility.setFormalVisibility(purgeVisibility);
        }

        /**
         * <p>
         * Answers true if, based on the given purge state, the purge may proceed
         * and false if it may not.
         * </p>
         *
         * @param state The purge state which is evaluated.
         */
        private boolean mayPurgeProceed(PurgeState state) {
            return !(CancelStatus.CANCELED.equals(state.getCancelStatus()) ||
                    CancelStatus.CANCEL_IN_PROGRESS.equals(state.getCancelStatus()) ||
                    PurgeStatus.FINISHED_COMPLETE.equals(state.getPurgeStatus()) ||
                    PurgeStatus.FINISHED_INCOMPLETE.equals(state.getPurgeStatus()));
        }

        /**
         * <p>
         * Executes the warehouse purge.
         * </p>
         */
        @Override
        public void run() {

            try {
                PurgeState state = purgeStatus(initiatorToken, this.purgeId);
                if (this.mayPurgeProceed(state)) {

                    if (idsToPurge == null || idsToPurge.size() == 0) {
                        logger.info("No warehouse entries were given for purge request #{}. Marking the purge as finished.", this.purgeId);
                        state.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
                    } else {
                        try {
                            Collection<String> uriList = getUris(idsToPurge, initiatorToken);
                            remove(uriList, initiatorToken);
                            state.setPurged(idsToPurge);
                            state.setPurgeStatus(PurgeStatus.FINISHED_COMPLETE);
                        } catch (Exception e) {
                            logger.error("The delete of the URIs from the warehouse table failed for purge request #{}.", this.purgeId, e);
                            state.setNotPurged(idsToPurge);
                            state.setPurgeStatus(PurgeStatus.ERROR);
                        }
                    }
                    state.setTimeStamp(TimeUtil.getCurrentThriftDateTime());
                    insertPurgeStatus(state);
                } else {
                    logger.info("The purge request #{} was skipped for warehouse because the state is not valid for a purge. The purge and cancel statuses, respectively, are: {} and {}.",
                            this.purgeId, state.getPurgeStatus(), state.getCancelStatus());
                }
            } catch (TException e) {
                logger.error("The purge request #{} encountered an error that prevented the warehouse purge from completing properly.",
                        this.purgeId, e);
            }
        }
    }

}
