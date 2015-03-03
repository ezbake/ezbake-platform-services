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

package ezbake.warehaus.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ezbake.thrift.ThriftUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.ExportFile;
import ezbake.warehaus.VersionControl;
import ezbake.warehaus.WarehausConstants;
import ezbake.warehaus.WarehausUtils;
import ezbakehelpers.accumulo.AccumuloHelper;

public class WarehausImport extends AbstractWarehausAction {

    @Option(name="-f", aliases="--file", required=true, usage="The export tar file to import")
    private String tarFile;

    public static void main(String[] args) throws IOException, TException, EzConfigurationLoaderException {

        WarehausImport importer = new WarehausImport();
        CmdLineParser parser = new CmdLineParser(importer);

        try {
            parser.parseArgument(args);
            importer.process();
            System.out.println("WarehausImport started");
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    public void process() throws IOException, TException, EzConfigurationLoaderException {
        EzSecurityToken token = ToolHelper.importToken();
        FileInputStream file = new FileInputStream(tarFile);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int read = file.read();
        while (read > -1) {
            baos.write(read);
            read = file.read();
        }
        file.close();

        WarehausImport importer = new WarehausImport();
        importer.importTestData(ByteBuffer.wrap(baos.toByteArray()), token);
    }

    public WarehausImport() {
        super();
    }

    public void importTestData(ByteBuffer tArchive, EzSecurityToken security) throws TException {
        TDeserializer deserializer = new TDeserializer();
        TarInputStream tar = new TarInputStream(new ByteArrayInputStream(tArchive.array()));
        TarEntry entry;

        List<String> uriList = Lists.newArrayList();
        Map<String, VersionControl> parsed = Maps.newHashMap();
        Map<String, VersionControl> raw = Maps.newHashMap();
        Map<String, ColumnVisibility> visibilityMap = Maps.newHashMap();

        try {
            entry = tar.getNextEntry();
            while (entry != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int value = tar.read();
                while (value > -1) {
                    baos.write(value);
                    value = tar.read();
                }
                ExportFile file = new ExportFile();
                deserializer.deserialize(file, baos.toByteArray());

                String uri = file.getData().getUri();
                uriList.add(uri);
                raw.put(uri, new VersionControl(ByteBuffer.wrap(file.getData().getRawData()), file.getName()));
                parsed.put(uri, new VersionControl(ByteBuffer.wrap(file.getData().getParsedData()), file.getName()));
                visibilityMap.put(uri, new ColumnVisibility(file.getVisibility().getFormalVisibility()));

                entry = tar.getNextEntry();
            }

            insertData(uriList, parsed, raw, visibilityMap, security);
        } catch (IOException e) {
            throw new TException(e);
        } finally {
            try {
                tar.close();
            } catch (IOException e) {
                throw new TException(e);
            }
        }
    }

    private void insertData(List<String> uriList, 
            Map<String, VersionControl> parsedMap,
            Map<String, VersionControl> rawMap, 
            Map<String, ColumnVisibility> visibilityMap, EzSecurityToken security) throws TException {

        String id = ExportImportHelper.confirmToken(security, securityClient);
        long timestamp = Calendar.getInstance().getTimeInMillis();
        Map<String, Mutation> mutationMap = Maps.newHashMap();
        BatchWriter writer = null;

        try {
            writer = createWriter();
            // Add the parsed/raw data
            for (String uri : uriList) {
                ColumnVisibility visibilityForUpdate = visibilityMap.get(uri);
                Mutation mutation = mutationMap.get(uri);
                if (mutation == null) {
                    mutation = new Mutation(WarehausUtils.getKey(uri));
                }

                if (parsedMap.containsKey(uri)) {
                    mutation.put(new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(ExportImportHelper.DataType.PARSED.toString()),
                            visibilityForUpdate, timestamp,
                            new Value(ThriftUtils.serialize(
                                    new VersionControl(ByteBuffer.wrap(parsedMap.get(uri).getPacket()), id))));
                }
                if (rawMap.containsKey(uri)) {
                    mutation.put(new Text(WarehausUtils.getUriPrefixFromUri(uri)), new Text(ExportImportHelper.DataType.RAW.toString()),
                            visibilityForUpdate, timestamp,
                            new Value(ThriftUtils.serialize(
                                    new VersionControl(ByteBuffer.wrap(rawMap.get(uri).getPacket()), id))));
                }

                try {
                    writer.addMutation(mutation);
                } catch (MutationsRejectedException e) {
                    throw new TException(e);
                }
            }

            try {
                if (writer != null) { // shouldn't normally be null, but anyway
                    writer.flush();
                }
            } catch (MutationsRejectedException e) {
                throw new TException(e);
            }
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (MutationsRejectedException e) {
                throw new TException(e);
            }
        }
    }

    private BatchWriter createWriter() throws TException {
        try {
            EzProperties properties = new EzProperties(this.config, false);
            long maxMemory = properties.getLong(WarehausConstants.BATCH_WRITER_MAX_MEMORY_KEY, WarehausConstants.DEFAULT_WRITER_MAX_MEMORY);
            long latency = properties.getLong(WarehausConstants.BATCH_WRITER_LATENCY_MS_KEY, WarehausConstants.DEFAULT_LATENCY);
            int threads = properties.getInteger(WarehausConstants.BATCH_WRITER_WRITE_THREADS_KEY, WarehausConstants.DEFAULT_WRITE_THREADS);

            Connector connector = new AccumuloHelper(config).getConnector();

            if (!connector.tableOperations().exists(WarehausConstants.TABLE_NAME)) {
                throw new TException(WarehausConstants.TABLE_NAME + "does not exist");
            }

            BatchWriter writer = connector.createBatchWriter(WarehausConstants.TABLE_NAME, maxMemory, latency, threads);
            return writer;
        } catch (IOException e) {
            throw new TException(e);
        } catch (TableNotFoundException e) {
            throw new TException(e);
        }
    }

}
