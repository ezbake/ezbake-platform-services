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

package deployer.utilities;

import com.google.common.io.Files;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.deployer.utilities.VersionHelper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test class for the VersionHelper class
 */
public class VersionHelperTest {
    private static final String RESOURCE_BASE = "versionHelper/";

    @Test
    public void testVersionFromJarManifest() throws Exception {
        File f = getResourceFile("guava-manifest.jar");
        String version = VersionHelper.getVersionFromJavaArtifact("guava-manifest.jar", f);
        assertEquals("18.0.0", version);
    }

    @Test
    public void testVersionFromJarPom() {
        File f = getResourceFile("guava-pom.jar");
        String version = VersionHelper.getVersionFromJavaArtifact("guava-pom.jar", f);
        assertEquals("18.0", version);
    }

    @Test
    public void testVersionFromJarFileName() {
        File f = getResourceFile("guava-18.0.jar");
        String version = VersionHelper.getVersionFromJavaArtifact("guava-18.0.jar", f);
        assertEquals("18.0", version);
    }

    @Test
    public void testVersionFromJarMissing() {
        File f = getResourceFile("guava-nothing.jar");
        String version = VersionHelper.getVersionFromJavaArtifact("guava-nothing.jar", f);
        assertNull(version);
    }

    @Test
    public void testVersionFromWarManifest() {
        File f = getResourceFile("quarantine-rest-manifest.war");
        String version = VersionHelper.getVersionFromJavaArtifact("quarantine-rest-manifest.war", f);
        assertEquals("2.0", version);
    }

    @Test
    public void testVersionFromWarPom() {
        File f = getResourceFile("quarantine-rest-pom.war");
        String version = VersionHelper.getVersionFromJavaArtifact("quarantine-rest-pom.war", f);
        assertEquals("2.0", version);
    }

    @Test
    public void testVersionFromWarFileName() {
        File f = getResourceFile("quarantine-rest-2.0-SNAPSHOT.war");
        String version = VersionHelper.getVersionFromJavaArtifact("quarantine-rest-2.0-SNAPSHOT.war", f);
        assertEquals("2.0-SNAPSHOT", version);
    }

    @Test
    public void testVersionFromWarMissing() {
        File f = getResourceFile("quarantine-rest-nothing.war");
        String version = VersionHelper.getVersionFromJavaArtifact("quarantine-rest-nothing.war", f);
        assertNull(version);
    }

    @Test
    public void testVersionFromArtifact() throws IOException {
        File f = getResourceFile("test-with-version.tar.gz");
        String version = VersionHelper.getVersionFromArtifact(ByteBuffer.wrap(Files.toByteArray(f)));
        assertEquals("2.15.1-b2", version);
    }

    @Test
    public void testVersionFromArtifactMissing() throws IOException {
        File f = getResourceFile("test.tar.gz");
        String version = VersionHelper.getVersionFromArtifact(ByteBuffer.wrap(Files.toByteArray(f)));
        assertNull(version);
    }

    @Test
    public void testBuildVersionFile() {
        ArtifactDataEntry entry = VersionHelper.buildVersionFile("12.10.1");
        assertEquals(VersionHelper.VERSION_FILE, entry.getEntry().getName());
    }

    private File getResourceFile(String filename) {
        return new File(this.getClass().getClassLoader().getResource(RESOURCE_BASE + filename).getFile());
    }
}
