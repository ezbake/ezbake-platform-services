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

package deployer.publishers.openshift;

import com.google.common.collect.Sets;
import ezbake.deployer.impl.BasicFileAttributes;
import ezbake.deployer.impl.FileVisitResult;
import ezbake.deployer.impl.FileVisitor;
import ezbake.deployer.impl.Files;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class FileCollector {
    public File testDir;
    public File gitDbDir;
    public final Set<File> expectedFiles;
    public final Set<File> foundUnexpectedFiles = Sets.newHashSet();
    public final Set<File> foundExpectedFiles = Sets.newHashSet();

    public FileCollector(File testDir, File gitDbDir, Set<File> expectedFiles) throws IOException {
        this.testDir = testDir;
        this.gitDbDir = gitDbDir;
        this.expectedFiles = expectedFiles;
        call();
    }

    public FileCollector(File testDir, File gitDbDir, File... expectedFiles) throws IOException {
        this.testDir = testDir;
        this.gitDbDir = gitDbDir;
        this.expectedFiles = Sets.newHashSet(expectedFiles);
        call();
    }

    private void call() throws IOException {
        System.out.println("TestDir: " + testDir);
        Files.walkFileTree(testDir, new FileVisitor<File>() {
            @Override
            public FileVisitResult preVisitDirectory(File dir, BasicFileAttributes attrs) throws IOException {
                if (dir.equals(gitDbDir))
                    return FileVisitResult.SKIP_SUBTREE;
                else
                    return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(File file, BasicFileAttributes attrs) throws IOException {
                File relFile = Files.relativize(testDir, file);
                if (!expectedFiles.contains(relFile)) {
                    foundUnexpectedFiles.add(relFile);
                } else {
                    foundExpectedFiles.add(relFile);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(File file, IOException exc) throws IOException {
                System.out.println("visitFileFailed(" + file + ")");
                return FileVisitResult.CONTINUE;

            }

            @Override
            public FileVisitResult postVisitDirectory(File dir, IOException exc) throws IOException {
                if (dir.equals(gitDbDir))
                    return FileVisitResult.SKIP_SUBTREE;
                else
                    return FileVisitResult.CONTINUE;
            }
        });
    }
}