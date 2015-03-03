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

package ezbake.deployer.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

public class Files {

    public static File get(String first, String... sub) {
        return get(new File(first), sub);
    }

    public static File get(File first, String... sub) {
        File base = first;
        for (String s : sub)
            base = new File(base, s);
        return base;
    }

    public static File get(File first, File... sub) {
        File base = first;
        for (File s : sub)
            base = new File(base, s.getPath());
        return base;
    }

    public static File resolve(File base, File... child) {
        File resolved = base;
        for (File c : child) {
            if (c.isAbsolute()) {
                resolved = c;
            } else {
                resolved = new File(resolved, c.getPath());
            }
        }
        return resolved;
    }

    public static File resolve(File base, String... child) {
        File[] fChildren = new File[child.length];
        for (int i = 0; i < child.length; i++) {
            fChildren[i] = new File(child[i]);
        }
        return resolve(base, fChildren);
    }

    public static File relativize(File base, File target) {
        String[] baseComponents;
        String[] targetComponents;
        try {
            baseComponents = base.getCanonicalPath().split(Pattern.quote(File.separator));
            targetComponents = target.getCanonicalPath().split(Pattern.quote(File.separator));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // skip common components
        int index = 0;
        for (; index < targetComponents.length && index < baseComponents.length; ++index) {
            if (!targetComponents[index].equals(baseComponents[index]))
                break;
        }

        StringBuilder result = new StringBuilder();
        if (index != baseComponents.length) {
            // backtrack to base directory
            for (int i = index; i < baseComponents.length; ++i)
                result.append("..").append(File.separator);
        }
        for (; index < targetComponents.length; ++index)
            result.append(targetComponents[index]).append(File.separator);
        if (!target.getPath().endsWith("/") && !target.getPath().endsWith("\\")) {
            // remove final path separator
            result.delete(result.length() - File.separator.length(), result.length());
        }
        return new File(result.toString());
    }

    public static boolean isDirectory(File file) {
        return file.isDirectory();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createDirectories(File parent) {
        parent.mkdirs();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createDirectories(String parent) {
        new File(parent).mkdirs();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void setPosixFilePermissions(File file, Set<PosixFilePermission> filePermissions) {
        if (filePermissions.contains(PosixFilePermission.OWNER_EXECUTE))
            file.setExecutable(true, filePermissions.contains(PosixFilePermission.GROUP_EXECUTE)
                    || filePermissions.contains(PosixFilePermission.OTHERS_EXECUTE));
        if (filePermissions.contains(PosixFilePermission.OWNER_READ))
            file.setReadable(true, filePermissions.contains(PosixFilePermission.GROUP_READ)
                    || filePermissions.contains(PosixFilePermission.OTHERS_READ));
        if (filePermissions.contains(PosixFilePermission.OWNER_WRITE))
            file.setWritable(true, filePermissions.contains(PosixFilePermission.GROUP_WRITE)
                    || filePermissions.contains(PosixFilePermission.OTHERS_WRITE));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void setPosixFilePermissions(File file, int mode) {
        setPosixFilePermissions(file, convertTarArchiveEntryModeToPosixFilePermissions(mode));
    }

    public static Set<PosixFilePermission> convertTarArchiveEntryModeToPosixFilePermissions(int mode) {
        Set<PosixFilePermission> permissions = Sets.newHashSet();
        BitSet bs = BitSet.valueOf(new long[]{mode});

        if (bs.get(PosixFilePermission.OTHERS_EXECUTE.ordinal())) {
            permissions.add(PosixFilePermission.OTHERS_EXECUTE);
        }
        if (bs.get(PosixFilePermission.OTHERS_WRITE.ordinal())) {
            permissions.add(PosixFilePermission.OTHERS_WRITE);
        }
        if (bs.get(PosixFilePermission.OTHERS_READ.ordinal())) {
            permissions.add(PosixFilePermission.OTHERS_READ);
        }
        if (bs.get(PosixFilePermission.GROUP_EXECUTE.ordinal())) {
            permissions.add(PosixFilePermission.GROUP_EXECUTE);
        }
        if (bs.get(PosixFilePermission.GROUP_WRITE.ordinal())) {
            permissions.add(PosixFilePermission.GROUP_WRITE);
        }
        if (bs.get(PosixFilePermission.GROUP_READ.ordinal())) {
            permissions.add(PosixFilePermission.GROUP_READ);
        }
        if (bs.get(PosixFilePermission.OWNER_EXECUTE.ordinal())) {
            permissions.add(PosixFilePermission.OWNER_EXECUTE);
        }
        if (bs.get(PosixFilePermission.OWNER_WRITE.ordinal())) {
            permissions.add(PosixFilePermission.OWNER_WRITE);
        }
        if (bs.get(PosixFilePermission.OWNER_READ.ordinal())) {
            permissions.add(PosixFilePermission.OWNER_READ);
        }

        return permissions;
    }

    public static int convertPosixFilePermissionsToTarArchiveEntryMode(Set<PosixFilePermission> permissions) {
        BitSet mode = new BitSet();

        if (permissions.contains(PosixFilePermission.OTHERS_EXECUTE)) {
            mode.set(PosixFilePermission.OTHERS_EXECUTE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.OTHERS_WRITE)) {
            mode.set(PosixFilePermission.OTHERS_WRITE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.OTHERS_READ)) {
            mode.set(PosixFilePermission.OTHERS_READ.ordinal());
        }
        if (permissions.contains(PosixFilePermission.GROUP_EXECUTE)) {
            mode.set(PosixFilePermission.GROUP_EXECUTE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.GROUP_WRITE)) {
            mode.set(PosixFilePermission.GROUP_WRITE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.GROUP_READ)) {
            mode.set(PosixFilePermission.GROUP_READ.ordinal());
        }
        if (permissions.contains(PosixFilePermission.OWNER_EXECUTE)) {
            mode.set(PosixFilePermission.OWNER_EXECUTE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.OWNER_WRITE)) {
            mode.set(PosixFilePermission.OWNER_WRITE.ordinal());
        }
        if (permissions.contains(PosixFilePermission.OWNER_READ)) {
            mode.set(PosixFilePermission.OWNER_READ.ordinal());
        }

        return (int)mode.toLongArray()[0];
    }

    public static void walkFileTree(final File gitRepoPath, final FileVisitor<File> simpleFileVisitor) throws IOException {
        new DirectoryWalker<File>() {
            @Override
            protected boolean handleDirectory(File directory, int depth, Collection<File> results) throws IOException {
                FileVisitResult x = simpleFileVisitor.preVisitDirectory(directory, new BasicFileAttributes(directory));
                if (x == FileVisitResult.CONTINUE)
                    return true;
                else if (x == FileVisitResult.SKIP_SUBTREE || x == FileVisitResult.SKIP_SIBLINGS)
                    return false;
                else
                    throw new CancelException(directory, depth);
            }

            @Override
            protected void handleFile(File file, int depth, Collection<File> results) throws IOException {
                simpleFileVisitor.visitFile(file, new BasicFileAttributes(file));
                super.handleFile(file, depth, results);
            }

            @Override
            protected void handleDirectoryEnd(File directory, int depth, Collection<File> results) throws IOException {
                simpleFileVisitor.postVisitDirectory(directory, null);
                super.handleDirectoryEnd(directory, depth, results);
            }

            protected void perform() throws IOException {
                walk(gitRepoPath, Lists.<File>newArrayList());
            }
        }.perform();
    }

    public static boolean isReadable(File file) {
        return file.canRead();
    }

    public static boolean exists(File file) {
        return file.exists();
    }

    private static final int TEMP_DIR_ATTEMPTS = 10000;

    public static File createTempDirectory(File baseDir, String prefix) {
        String baseName = prefix + "-" + System.currentTimeMillis() + "-";

        for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
            File tempDir = new File(baseDir, baseName + counter);
            if (tempDir.mkdir()) {
                return tempDir;
            }
        }
        throw new IllegalStateException("Failed to create directory within "
                + TEMP_DIR_ATTEMPTS + " attempts (tried "
                + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }

    public static File createTempDirectory(String prefix) {
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        return createTempDirectory(baseDir, prefix);
    }

    public static boolean isExecutable(File file) {
        return file.canExecute();
    }

    public static void deleteRecursively(File directory) throws IOException {
        FileUtils.deleteDirectory(directory);
    }
}
