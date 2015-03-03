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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import ezbake.base.thrift.Visibility;
import ezbake.data.common.TimeUtil;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.DocumentClassification;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.ExportFile;
import ezbake.warehaus.Repository;
import ezbake.warehaus.VersionControl;
import ezbake.warehaus.WarehausConstants;
import ezbake.warehaus.WarehausUtils;
import ezbakehelpers.accumulo.AccumuloHelper;

public class WarehausExport extends AbstractWarehausAction {
    
    @Option(name="-f", aliases="--file", required=true, usage="The export tar file to import")
    private String file;

    @Option(name="-n", aliases="--name", required=true, usage="The feed name")
    private String name;

    @Option(name="-s", aliases="--start", usage="The start time")
    private Long start;

    @Option(name="-e", aliases="--end", usage="The end time")
    private Long end;
    
	public static void main(String[] args) throws TException, IOException, EzConfigurationLoaderException {
		
		WarehausExport exporter = new WarehausExport();       
        CmdLineParser parser = new CmdLineParser(exporter);

        try {
            parser.parseArgument(args);
            exporter.process();
            System.out.println("WarehausExport started");
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
	}

    public WarehausExport() {
       super();
    }
    
    public void process() throws TException, IOException, EzConfigurationLoaderException {
        WarehausExport export = new WarehausExport();
        EzSecurityToken token = ToolHelper.importToken();
        
        DateTime startTime = null;
        DateTime finishTime = null;
        if (start != null) {
            startTime = TimeUtil.convertToThriftDateTime(start);
        }
        if (end != null) {
            finishTime = TimeUtil.convertToThriftDateTime(end);
        }
        
        ByteBuffer data = export.exportTestData(name, startTime, finishTime, new DocumentClassification("U"), token);
        FileOutputStream fileStream = new FileOutputStream(file);
        fileStream.write(data.array());
        fileStream.flush();
        fileStream.close();
    }
    
	public ByteBuffer exportTestData(String urn, DateTime start, DateTime finish, 
	        DocumentClassification classification, EzSecurityToken security) throws TException {
        if (urn == null || "".equals(urn.trim())) {
            throw new TException("Cannot export a null or empty urn");
        }
        
        ExportImportHelper.confirmToken(security, securityClient);
        String clearance = classification.getClassification();
        ByteBuffer forReturn;
        HashMap<String, Map<Long, ExportFile>> uriTimestamps = Maps.newHashMap();
        BatchScanner scanner = null;
        try {
            scanner = createScanner(clearance);

            long startTime = 0, endTime = Long.MAX_VALUE;
            if (start != null) {
                startTime = TimeUtil.convertFromThriftDateTime(start);
            }
            if (finish != null) {
                endTime = TimeUtil.convertFromThriftDateTime(finish);
            }
            scanner.setRanges(Lists.newArrayList(new Range())); // empty range
            
            IteratorSetting iterator = new IteratorSetting(10, "regex", RegExFilter.class);
            RegExFilter.setRegexs(iterator,
                    WarehausUtils.getPatternForURI(urn).toString(), null, null, null, false);
            scanner.addScanIterator(iterator);

            IteratorSetting is = new IteratorSetting(5, "version", VersioningIterator.class);
            VersioningIterator.setMaxVersions(is, Integer.MAX_VALUE);
            scanner.addScanIterator(is);
            
            for (Entry<Key, Value> entry : scanner) {

                long ts = entry.getKey().getTimestamp();
                if (startTime <= ts && ts <= endTime) {
                    String uri = WarehausUtils.getUriFromKey(entry.getKey());
                    Map<Long, ExportFile> repoMap = uriTimestamps.get(uri);
                    if (repoMap == null) {
                        repoMap = Maps.newHashMap();
                    }
                    ExportFile ef = new ExportFile();
                    Repository data = new Repository();
                    VersionControl versionData = new VersionControl();
                    new TDeserializer().deserialize(versionData, entry.getValue().get());
                    
                    if (repoMap.containsKey(ts) ) {
                        ef = repoMap.get(ts);
                        data = ef.getData();
                    } else {
                        data.setUri(uri);
                        ef.setVisibility(new Visibility().setFormalVisibility(
                                entry.getKey().getColumnVisibility().toString()));
                        ef.setName(versionData.getName());
                        ef.setTimestamp(entry.getKey().getTimestamp());
                    }

                    if (entry.getKey().getColumnQualifier().toString().equals(
                            ExportImportHelper.DataType.PARSED.toString())) {
                        data.setParsedData(versionData.getPacket());
                    } else if (entry.getKey().getColumnQualifier().toString().equals(
                            ExportImportHelper.DataType.RAW.toString())) {
                        data.setRawData(versionData.getPacket());
                    }
                    versionData.clear();
                    ef.setData(data);
                    repoMap.put(ts, ef);
                    uriTimestamps.put(uri, repoMap);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        int count = 1;        
        TSerializer serializer = new TSerializer();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TarOutputStream tar = new TarOutputStream(new BufferedOutputStream(baos));

        try {
            for (Map<Long, ExportFile> files : uriTimestamps.values()) {
                for (ExportFile ef : files.values()) {
    
                    byte[] bytes = serializer.serialize(ef);
                    TarEntry tarFile = new TarEntry(Integer.toString(count)+".whe");
                    tarFile.setSize(bytes.length);
                    tar.putNextEntry(tarFile);
                    tar.write(bytes);
                    tar.closeEntry();
                    tar.flush();
                    baos.flush();
                    if (count == 100) {
                        break;
                    }
                    count++;
                }
            }
        } catch (IOException e) {
            throw new TException(e);
        } finally {
            try {
                tar.finish();
                tar.close();
            } catch (IOException e) {
                throw new TException(e);
            }
            forReturn = ByteBuffer.wrap(baos.toByteArray());
        }
    
        return forReturn;
    }

    private static BatchScanner createScanner(String auths) throws TException {
        Connector connector;
        try {
            try {
               Properties config = new EzConfiguration().getProperties();
                connector = new AccumuloHelper(config).getConnector(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return connector.createBatchScanner(WarehausConstants.TABLE_NAME, 
                    WarehausUtils.getAuthsFromString(auths), WarehausConstants.QUERY_THREADS);
        } catch (TableNotFoundException e) {
            throw new TException(e);
        }
    }
    
}
