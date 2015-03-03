/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.persist;

import ezbake.persist.exception.EzPKeyError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 8:21 AM
 */
public class FilePersist extends EzPersist {
    private static final Logger logger = LoggerFactory.getLogger(FilePersist.class);
    private String directory;
    private List<String> tables;

    public FilePersist(String directory) {
        this(directory, Arrays.asList(""));
    }
    public FilePersist(String directory, List<String> subdirs) {
        this.directory = directory;
        this.tables = new ArrayList<String>(subdirs);
    }

    public static String joinPath(String... paths) {
        return StringUtils.join(paths, File.separatorChar);
    }

    @Override
    public String read(String row) throws EzPKeyError {
        return read(row, "", "", tables.get(0));
    }

    @Override
    public String read(String row, String colf) throws EzPKeyError {
        return read(row, colf, "", tables.get(0));
    }

    @Override
    public String read(String row, String colf, String colq) throws EzPKeyError {
        return read(row, colf, colq, tables.get(0));
    }

    @Override
    public String read(String row, String colf, String colq, String table) throws EzPKeyError {
        File rf = new File(joinPath(directory, table, row, colf, colq, "data"));
        String data;
        try {
            data = FileUtils.readFileToString(rf);
        } catch (IOException e) {
            throw new EzPKeyError("Cell not found", e);
        }
        return data;
    }

    @Override
    public Map<String, String> row(String row) throws EzPKeyError {
        return this.row(row, tables.get(0));
    }

    @Override
    public Map<String, String> row(String row, String table) throws EzPKeyError {
        Map<String, String> vals = new HashMap<String, String>();
        File rowRoot = new File(joinPath(directory, table, row));

        String keyRoot = directory.replaceAll("\\/$", "");
        if (table != null && !table.isEmpty()) {
            keyRoot = joinPath(keyRoot, table);
        }
        logger.debug("Key root: {}", keyRoot);
        List<String> keys = getFiles(rowRoot.toString(), keyRoot);
        logger.debug("keys: {}", keys);
        if (keys.isEmpty()) {
            throw new EzPKeyError("No row found with id: " + row);
        }
        for (String rowKey : keys) {
            if (!rowKey.endsWith("/data")) {
                continue;
            }
            String keyPath = rowKey.split("/data")[0];
            String key = EzPersist.key(keyPath.split(File.separator));

            logger.debug("Key path: {}, key: {}", keyPath, key);

            // Add the element to the array
            try {
                vals.put(key, FileUtils.readFileToString(new File(keyRoot, rowKey)));
            } catch (IOException e) {
                logger.debug("Error reading file", e);
            }
        }

        return vals;
    }

    @Override
    public List<Map<String, String>> all() {
        return all(tables.get(0));
    }

    @Override
    public List<Map<String, String>> all(String table) {
        List<Map<String, String>> vals = new ArrayList<Map<String, String>>();

        List<String> keys = getFiles(directory);
        int index = 0;
        String cRow = null;
        for (String rowKey : keys) {
            if (!rowKey.endsWith("/data")) {
                continue;
            }

            String keyPath = rowKey.split("/data")[0];
            String key = EzPersist.key(keyPath.split(File.separator));
            // Add different rows as different hashmaps
            String[] keyParts = EzPersist.keyParts(key);
            if (keyParts == null) {
                continue;
            } else if (cRow != null && !cRow.endsWith(keyParts[0])) {
                cRow = keyParts[0];
                index += 1;
            } else if (cRow == null) {
                cRow = keyParts[0];
            }

            // Add a new element if the list is full
            if (index >= vals.size()) {
                vals.add(new HashMap<String, String>());
            }


            // Add the element to the array
            try {
                vals.get(index).put(key, FileUtils.readFileToString(new File(directory, rowKey)));
            } catch (IOException e) {
                continue;
            }
        }
        return vals;
    }

    private List<String> getFiles(String path) {
        return getFiles(path, path, new ArrayList<String>());
    }
    private List<String> getFiles(String path, String root) {
        return getFiles(path, root, new ArrayList<String>());
    }
    private List<String> getFiles(String path, String root, List<String> data) {
        File file = new File(path);
        File[] files = file.listFiles();
        if (file.exists()){
            data.add(path.replaceFirst(joinPath(root, ""), ""));
        }
        if (files != null) {
            for (File f : files) {
                getFiles(f.toString(), root, data);
            }
        }
        return data;
    }
}
