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

import com.google.common.collect.ImmutableSet;
import ezbake.deployer.impl.Files;
import ezbake.deployer.impl.PosixFilePermission;
import ezbake.deployer.utilities.ArtifactDataEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * This interface provides a way to create ArtifactDataEntries
 */
public class ArtifactDataEntryResourceCreator {

    public final static Set<PosixFilePermission> DEFAULT_FILE_PERMISSIONS = ImmutableSet.<PosixFilePermission>builder()
            .add(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.OTHERS_READ)
            .build();

    public final static Set<PosixFilePermission> DEFAULT_EXECUTABLE_FILE_PERMISSIONS =
            ImmutableSet.<PosixFilePermission>builder()
                    .addAll(DEFAULT_FILE_PERMISSIONS)
                    .add(PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_EXECUTE,
                            PosixFilePermission.OTHERS_EXECUTE)
                    .build();

    private Set<PosixFilePermission> permissions;

    /**
     * An artifact data entry resource creator with default permissions
     */
    ArtifactDataEntryResourceCreator() {
        this(DEFAULT_FILE_PERMISSIONS);
    }

    /**
     * An artifact data entry resource creator with custom permissions
     * @param permissions specified custom permissions
     */
    ArtifactDataEntryResourceCreator(Set<PosixFilePermission> permissions) {
        this.permissions = permissions;
    }

    /**
     * Create an ArtifactDataEntry using the specified resource
     * @param artifactPath name path of the artifact archive entry
     * @param resourcePath path to the resource used in creating the artifact data
     * @return created artifact data entry
     * @throws IOException
     */
    public ArtifactDataEntry createFromClassPathResource(String artifactPath, String resourcePath) throws IOException {
        return new ArtifactDataEntry(createTarArchiveEntry(artifactPath),
                IOUtils.toByteArray(JavaWebAppArtifactContentsPublisher.class
                        .getResourceAsStream(Files.get("/", resourcePath).toString())));
    }

    /**
     * Create an artifact data entry with blank data
     * @param artifactPath name path of the artifact archive entry
     * @return created artifact data entry
     */
    public ArtifactDataEntry createEmptyArtifact(String artifactPath) {
        return new ArtifactDataEntry(createTarArchiveEntry(artifactPath), new byte[]{});
    }

    /**
     * Update the set of permissions with a new set
     * @param newPermissions new permissions
     */
    public void updatePermissions(Set<PosixFilePermission> newPermissions) {
        permissions = newPermissions;
    }

    /**
     * Add a permission to the current set of permissions
     * @param permission new permission
     */
    public void addPermission(PosixFilePermission permission) {
        permissions.add(permission);
    }

    private TarArchiveEntry createTarArchiveEntry(String artifactPath) {
        TarArchiveEntry archiveEntry = new TarArchiveEntry(new File(artifactPath));
        archiveEntry.setMode(archiveEntry.getMode() |
                Files.convertPosixFilePermissionsToTarArchiveEntryMode(permissions));
        return archiveEntry;
    }
}
