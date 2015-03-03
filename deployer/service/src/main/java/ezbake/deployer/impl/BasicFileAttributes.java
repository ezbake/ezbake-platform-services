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


import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Basic attributes associated with a file in a file system.
 * <p/>
 * <p> Basic file attributes are attributes that are common to many file systems
 * and consist of mandatory and optional file attributes as defined by this
 * interface.
 * <p/>
 * <p> <b>Usage Example:</b>
 * <pre>
 *    Path file = ...
 *    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
 * </pre>
 *
 * @since 1.7
 * See java 1.7 java.nio.file.attribute.BasicFileAttributeView
 */

public class BasicFileAttributes {

    File file;

    public BasicFileAttributes(File file) {
        this.file = file;
    }

    Date lastModifiedTime() {
        return new Date(file.lastModified());
    }

    Date lastAccessTime() {
        return lastModifiedTime();
    }

    Date creationTime() {
        return lastModifiedTime();
    }

    boolean isRegularFile() {
        return file.isFile();
    }

    boolean isDirectory() {
        return file.isDirectory();
    }

    boolean isSymbolicLink() {
        try {
            return FileUtils.isSymlink(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isOther() {
        return !isRegularFile() && !isDirectory() && !isSymbolicLink();
    }

    long size() {
        return FileUtils.sizeOf(file);
    }

}