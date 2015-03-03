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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

/**
 * This class is a wrapper around the TarArchiveEntry that allows us to combine the archive entry
 * and some blob of data that will be written to the archive
 */
public class ArtifactDataEntry {
    TarArchiveEntry entry;
    byte[] data;

    public ArtifactDataEntry(TarArchiveEntry entry, byte[] data) {
        this.entry = entry;
        this.data = data;
        this.entry.setSize(data.length);
    }

    public TarArchiveEntry getEntry() {
        return entry;
    }

    public void setEntry(TarArchiveEntry entry) {
        this.entry = entry;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
