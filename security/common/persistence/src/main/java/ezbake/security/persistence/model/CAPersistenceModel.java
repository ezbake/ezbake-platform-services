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

package ezbake.security.persistence.model;

import com.google.common.collect.ImmutableMap;
import ezbake.persist.EzPersist;
import ezbake.persist.EzPersistBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.List;
import java.util.Map;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 10:27 AM
 */
public class CAPersistenceModel extends EzPersistBase {
    String id;
    String certificate;
    String privateKey;
    long serial;

    public static CAPersistenceModel fromRows(List<Map.Entry<Key, Value>> rows) {
        CAPersistenceModel model = new CAPersistenceModel();

        for (Map.Entry<Key, Value> row : rows) {
            String attribute = row.getKey().getColumnFamily().toString();
            String value = new String(row.getValue().get());

            if (model.id == null) {
                model.id = row.getKey().getRow().toString();
            }

            if (attribute.equals("certificate")) {
                model.certificate = value;
            } else if (attribute.equals("private_key")) {
                model.privateKey = value;
            } else if (attribute.equals("serial")) {
                model.serial = Long.valueOf(value);
            }
        }
        return model;
    }

    public static CAPersistenceModel fromEzPersist(Map<String, String> rows) {
        CAPersistenceModel model = new CAPersistenceModel();
        return model.populateEzPersist(rows);
    }

    @Override
    public CAPersistenceModel populateEzPersist(Map<String, String> rows) {
        for (Map.Entry<String,String> entry : rows.entrySet()) {
            String[] parts = EzPersist.keyParts(entry.getKey());
            if (parts == null || parts.length < 2) {
                continue;
            }
            String attribute = parts[1];
            String value = entry.getValue();

            if (id == null) {
                id = parts[0];
            }

            if (attribute.equals("certificate")) {
                certificate = value;
            } else if (attribute.equals("private_key")) {
                privateKey = value;
            } else if (attribute.equals("serial")) {
                serial = Long.valueOf(value);
            }
        }
        return this;
    }

    @Override
    public Map<String, Object> ezPersistRows() {
        String certRow = EzPersist.key(id, "certificate");
        String pkRow = EzPersist.key(id, "private_key");
        String serialRow = EzPersist.key(id, "serial");

        return ImmutableMap.<String, Object>of(
                certRow, certificate,
                pkRow, privateKey,
                serialRow, String.valueOf(serial));
    }

    public String getId() {
        return id;
    }

    public String getCertificate() {
        return certificate;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public long getSerial() {
        return serial;
    }
}
