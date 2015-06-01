/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.persist;

import ezbake.persist.exception.EzPKeyError;

import java.util.List;
import java.util.Map;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 8:03 AM
 */
public abstract class EzPersist {
    public static final String KEY_SEP = ":";
    public static final String ESCAPE_SEP = "_:_";

    public static String escapeKeyPart(String part) {
        return part.replaceAll(KEY_SEP, ESCAPE_SEP);
    }
    public static String unescapeKeyPart(String part) {
        return part.replaceAll(ESCAPE_SEP, KEY_SEP);
    }

    public static String key(String... parts) {
        if (parts.length > 3) {
            return null;
        }
        String row = (parts.length > 0)? parts[0] : "None";
        String colf = (parts.length > 1)? parts[1] : "None";
        String colq = (parts.length > 2)? parts[2] : "None";
        return key(row, colf, colq);
    }
    public static String key(String row, String colf, String colq) {
        return String.format("%s%s%s%s%s", new String[]{row, KEY_SEP, colf, KEY_SEP, colq});
    }

    public static String[] keyParts(String key) {
        if (key == null) {
            return null;
        }
        String[] parts = key.split(KEY_SEP);
        if (parts.length != 3) {
            return null;
        }
        String row = unescapeKeyPart(parts[0]);
        String colf = unescapeKeyPart(parts[1]);
        String colq = unescapeKeyPart(parts[2]);

        return new String[]{row, colf, colq};
    }

    public abstract String read(String row) throws EzPKeyError;
    public abstract String read(String row, String colf) throws EzPKeyError;
    public abstract String read(String row, String colf, String colq) throws EzPKeyError;
    public abstract String read(String row, String colf, String colq, String table) throws EzPKeyError;

    public abstract Map<String, String> row(String row) throws EzPKeyError;
    public abstract Map<String, String> row(String row, String table) throws EzPKeyError;

    public abstract List<Map<String, String>> all();
    public abstract List<Map<String, String>> all(String table);
}
