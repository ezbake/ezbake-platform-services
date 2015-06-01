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

import com.google.common.base.Charsets;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Helper class to get version numbers from deployed artifacts.
 * Currently supports a version.txt file in a tar.gz, manifest in jar/war, or a pom.properties file in jar/war
 */
public class VersionHelper {
    public static final String VERSION_FILE = "version.txt";
    private static Logger logger = LoggerFactory.getLogger(VersionHelper.class);

    public static String getVersionFromJavaArtifact(String fileName, File artifact) {
        String versionNumber = null;
        try {
            versionNumber = getVersionFromManifest(artifact);
        } catch (IOException ex) {
            logger.error("Failed to get version number from the manifest", ex);
        }
        if (versionNumber == null) {
            try {
                logger.debug("Attempting to get version number from the pom.properties");
                versionNumber = getVersionFromPomProperties(artifact);
            } catch (IOException ex) {
                logger.error("Failed to get version number from the properties files", ex);
            }
        }
        if (versionNumber == null) {
            logger.debug("Attempting to get version number from the file name");
            versionNumber = getVersionFromFileName(fileName);
        }
        return versionNumber;
    }

    public static ArtifactDataEntry buildVersionFile(String version) {
        return new ArtifactDataEntry(new TarArchiveEntry(VERSION_FILE), version.getBytes(Charsets.UTF_8));
    }

    public static String getVersionFromArtifact(ByteBuffer artifact) throws IOException {
        String versionNumber = null;
        try (CompressorInputStream uncompressedInput = new GzipCompressorInputStream(new ByteArrayInputStream(artifact.array()))) {
            ArchiveInputStream input = new TarArchiveInputStream(uncompressedInput);
            TarArchiveEntry entry;
            while ((entry = (TarArchiveEntry) input.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().endsWith(VERSION_FILE)) {
                    versionNumber = IOUtils.toString(input, Charsets.UTF_8).trim();
                    break;
                }
            }
        }
        return versionNumber;
    }

    private static String getVersionFromManifest(File artifact) throws IOException {
        String versionNumber = null;
        try (JarFile jar = new JarFile(artifact)) {
            Manifest manifest = jar.getManifest();
            Attributes attributes = manifest.getMainAttributes();
            if (attributes != null) {
                for (Object o : attributes.keySet()) {
                    Attributes.Name key = (Attributes.Name) o;
                    String keyword = key.toString();
                    if (keyword.equals("Implementation-Version") || keyword.equals("Bundle-Version")) {
                        versionNumber = (String) attributes.get(key);
                        break;
                    }
                }
            }
        }
        return versionNumber;
    }

    private static String getVersionFromFileName(String fullFileName) {
        String versionNumber = null;
        String fileName = fullFileName.substring(0, fullFileName.lastIndexOf("."));
        if (fileName.contains(".")) {
            String majorVersion = fileName.substring(0, fileName.indexOf("."));
            String minorVersion = fileName.substring(fileName.indexOf("."));
            int delimiter = majorVersion.lastIndexOf("-");
            if (majorVersion.indexOf("_") > delimiter) {
                delimiter = majorVersion.indexOf("_");
            }
            majorVersion = majorVersion.substring(delimiter + 1, fileName.indexOf("."));
            versionNumber = majorVersion + minorVersion;
        } else {
            logger.debug("File name {} doesn't appear to contain a version number", fullFileName);
        }
        return versionNumber;
    }

    private static String getVersionFromPomProperties(File artifact) throws IOException {
        List<JarEntry> pomPropertiesFiles = new ArrayList<>();
        String versionNumber = null;
        try (JarFile jar = new JarFile(artifact)) {
            JarEntry entry;
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                entry = entries.nextElement();
                if (!entry.isDirectory() && entry.getName().endsWith("pom.properties")) {
                    pomPropertiesFiles.add(entry);
                }
            }

            if (pomPropertiesFiles.size() == 1) {
                Properties pomProperties = new Properties();
                pomProperties.load(jar.getInputStream(pomPropertiesFiles.get(0)));
                versionNumber = pomProperties.getProperty("version", null);
            } else {
                logger.debug("Found {} pom.properties files. Cannot use that for version", pomPropertiesFiles.size());
            }
        }
        return versionNumber;
    }
}
