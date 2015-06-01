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

package ezbake.deployer.publishers.openShift.inject;

import ezbake.deployer.impl.PosixFilePermission;

import java.io.File;
import java.io.InputStream;
import java.util.Set;

/**
 * This serves as a wrapper structure for a resource that can be installed on the filesystem.
 *
 * It has a path, input stream of data to read, and permissions
 */
public class ArtifactResource {

    private File path;
    private InputStream stream;
    private Set<PosixFilePermission> permissions;

    public ArtifactResource(File path, InputStream stream) {
        this(path, stream, null);
    }

    public ArtifactResource(File path, InputStream stream, Set<PosixFilePermission> permissions) {
        this.path = path;
        this.stream = stream;
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        return "{ path: " + path + ", stream: " + stream + ", permissions: " + permissions + "}";
    }

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }

    public InputStream getStream() {
        return stream;
    }

    public void setStream(InputStream stream) {
        this.stream = stream;
    }

    public Set<PosixFilePermission> getPermissions() {
        return permissions;
    }

    public void setPermissions(Set<PosixFilePermission> permissions) {
        this.permissions = permissions;
    }
}
