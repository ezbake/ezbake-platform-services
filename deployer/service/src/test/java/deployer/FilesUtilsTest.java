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

package deployer;

import com.google.common.collect.ImmutableSet;
import ezbake.deployer.impl.Files;
import ezbake.deployer.impl.PosixFilePermission;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FilesUtilsTest {

    @Test
    public void testGetRelativePathsUnixy() {
        assertEquals("stuff/xyz.dat", Files.relativize(
                new File("/var/data/"), new File("/var/data/stuff/xyz.dat")).toString());
        assertEquals("../../b/c", Files.relativize(
                new File("/a/x/y/"), new File("/a/b/c")).toString());
        assertEquals("../../b/c", Files.relativize(
                new File("/m/n/o/a/x/y/"), new File("/m/n/o/a/b/c")).toString());
    }

    @Test
    public void testPosixPermissionConversions() {
        Set<PosixFilePermission> expectedPermissions = ImmutableSet.<PosixFilePermission>builder()
                .add(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_EXECUTE, PosixFilePermission.OWNER_WRITE)
                .build();
        assertEquals(expectedPermissions, Files.convertTarArchiveEntryModeToPosixFilePermissions(0241));

        assertEquals(0444,
                Files.convertPosixFilePermissionsToTarArchiveEntryMode(ImmutableSet.<PosixFilePermission>builder()
                        .add(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ)
                        .build()));

        assertEquals(0777,
                Files.convertPosixFilePermissionsToTarArchiveEntryMode(
                        new HashSet<>(Arrays.asList(PosixFilePermission.values()))));

        assertEquals(new HashSet<>(Arrays.asList(PosixFilePermission.values())),
                Files.convertTarArchiveEntryModeToPosixFilePermissions(0777));

        assertEquals(0755, Files.convertPosixFilePermissionsToTarArchiveEntryMode(
                Files.convertTarArchiveEntryModeToPosixFilePermissions(0755)));
    }

}
