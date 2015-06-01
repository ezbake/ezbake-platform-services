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

package ezbake.warehaus;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;

/**
 * Contains utility methods for the AccumuloWarehaus.
 */
public class WarehausUtils {

    private static final Logger logger = LoggerFactory
            .getLogger(WarehausUtils.class);

    public static Authorizations getAuthsFromString(final String auths) {
        Authorizations retVal;
        if (auths == null || auths.trim().equals("")) {
            retVal = Authorizations.EMPTY;
        } else {
            retVal = new Authorizations(auths.split(","));
        }
        return retVal;
    }

    public static String getAuthsListFromToken(EzSecurityToken token) {
        ezbake.base.thrift.Authorizations auths = token.getAuthorizations();
        Set<String> formalAuths = auths.isSetFormalAuthorizations() ? auths.getFormalAuthorizations() : Sets.<String>newHashSet();
        Set<String> externalCommunities = auths.isSetExternalCommunityAuthorizations() ? auths.getExternalCommunityAuthorizations() : Sets.<String>newHashSet();
        Set<String> allAuths = Sets.union(formalAuths, externalCommunities);
        return StringUtils.join(allAuths, ",");
    }

    public static Text getKey(String uri) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(uri.getBytes());
            byte[] hash = messageDigest.digest();
            BigInteger bigInt = new BigInteger(1, hash);
            String hashtext = bigInt.toString(16);
            return new Text(hashtext + ":" + uri);
        } catch (NoSuchAlgorithmException e) {
            // We hopefully should never end up here
            logger.error("NoSuchAlgorithmException thrown while attempting "
                    + "to hash key.", e);
            throw new RuntimeException(e);
        }
    }

    public static String getUriPrefixFromUri(String uri) {
        // Assuming the uri schema is of the form - CATEGORY://FEED_NAME/ID
        String uriPrefix = uri;
        int idx = StringUtils.ordinalIndexOf(uri, "/", 3);
        if (idx > -1) {
            uriPrefix = uri.substring(0, idx);
        }
        return uriPrefix;
    }

    public static String getPatternForURI(String uri) {
        return ".*\\:" + Pattern.quote(uri) + ".*";
    }

    public static String getUriFromKey(Key key) {
        String row = key.getRow().toString();
        return getUriFromComputed(row);
    }

    public static String getUriFromComputed(String computed) {
        return computed.substring(computed.indexOf(":") + 1, computed.length());
    }

    public static String visibilityToString(ColumnVisibility visibility) {
        return new String(visibility.getExpression());
    }

}
