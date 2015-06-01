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

package deployer.publishers;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public abstract class TarFileChecker {
    public void check(byte[] bytes) throws Exception {
        check(new ByteArrayInputStream(bytes));
    }

    public void check(InputStream is) throws Exception {
        GZIPInputStream gzis = new GZIPInputStream(is);
        TarArchiveInputStream tarIs = new TarArchiveInputStream(gzis);
        check(tarIs);
    }

    public void check(TarArchiveInputStream inputStream) throws Exception {
        // copy the existing entries
        TarArchiveEntry nextEntry;
        while ((nextEntry = inputStream.getNextTarEntry()) != null) {
            verify(nextEntry, inputStream);
        }
    }

    public abstract void verify(TarArchiveEntry entry, InputStream inputStream) throws Exception;
}
