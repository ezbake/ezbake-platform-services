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

package ezbake.quarantine.service.util;

import com.google.common.base.Joiner;
import ezbake.quarantine.thrift.QuarantinedObject;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class IDGenerationUtility {
    public static String getId(QuarantinedObject qo) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        return getHash(qo.getPipelineId().getBytes(), qo.getPipeId().getBytes(), qo.getContent());
    }

    public static String getHash(byte[]... bytesToHash) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.reset();
        for (byte[] bytes : bytesToHash) {
            digest.update(bytes);
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }

    public static String getAggregateId(String toAggregate) {
        return getSeparatedString(toAggregate, "aggregate");
    }

    public static String getPipelineAndPipeId(String pipelineId, String pipeId) {
        return getSeparatedString(pipelineId, pipeId);
    }

    public static String getSeparatedString(String... strings) {
        return Joiner.on("::").join(strings);
    }

    public static String[] getIdParts(String id) {
        return id.split("::");
    }
}
