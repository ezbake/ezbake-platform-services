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

package ezbake.deployer.publishers.artifact;

import com.google.common.collect.Lists;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.services.deploy.thrift.DeploymentArtifact;
import ezbake.services.deploy.thrift.DeploymentException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class PythonRequirementsPublisher extends ArtifactContentsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(PythonAppsArtifactContentsPublisher.class);

    public String[] extractPackageInfo(String filename) {
        String[] tokens = filename.split("-");

        if (tokens.length >= 2) {
            String[] p = new String[2];
            p[0] = tokens[0]; // package name
            p[1] = tokens[1]; // package version
            return p;
        }

        logger.error("Unable to parse the package name to extract valuable information: " + filename);
        return null;
    }

    @Override
    public Collection<ArtifactDataEntry> generateEntries(DeploymentArtifact artifact) throws DeploymentException {

        // load the tarball
        TarArchiveInputStream tarIn = null;
        try {
            byte[] a = artifact.getArtifact();
            tarIn = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(a)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // collect the packages
        // (pip fails if there are duplicate package names, so we're using map
        // to avoid repeated packages)
        TarArchiveEntry entry = null;
        Map<String, String> requirements = new HashMap<String, String>();
        do {
            try {
                if (tarIn != null) {
                    entry = tarIn.getNextTarEntry();
                }
            } catch (IOException e) {
                entry = null;
            }

            // get the name of the current entry
            String name = null;
            if (entry != null) {
                name = entry.getName();
            }

            if ((entry != null) && name.startsWith("./wheels/")) {
                String[] parts = name.split("/");
                String basename = parts[parts.length - 1];

                String[] p = extractPackageInfo(basename);

                if ((p != null) && (p.length == 2)) {
                    requirements.put(p[0], p[1]);
                }
            }
        } while (entry != null);

        // create requirements.txt output
        StringBuffer out = new StringBuffer();
        out.append("# commands for pip\n");
        out.append("--no-index\n");
        out.append("--use-wheels\n");
        out.append("--find-links ./wheels/\n\n");

        out.append("# requirements\n");
        for (Map.Entry<String, String> p : requirements.entrySet()) {
            out.append(p.getKey()); // package name
            out.append("==");
            out.append(p.getValue()); // package version
            out.append("\n");
        }

        return Lists.newArrayList(new ArtifactDataEntry(new TarArchiveEntry("requirements.txt"), out.toString().getBytes()));
    }
}
