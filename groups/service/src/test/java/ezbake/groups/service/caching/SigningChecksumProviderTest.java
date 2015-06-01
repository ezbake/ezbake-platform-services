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

 package ezbake.groups.service.caching;

import com.google.common.collect.Sets;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.RSAKeyCrypto;
import org.junit.Assert;
import org.junit.Test;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Set;

public class SigningChecksumProviderTest {

    @Test
    public void testChecksums() throws PKeyCryptoException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        String key = "key!";
        long[] valueArray = new long[] {
                0l, 6l, 8l, 29l, 44l, 46l, 53l, 77l, 96l, 115l, 117l, 121l, 125l, 127l, 133l, 148l, 188l, 295l, 332l,
                342l, 567l, 575l, 577l, 715l, 2600l, 2601l, 9623l, 9624l, 9625l, 9626l, 9858l, 9859l, 9860l, 9861l,
                9862l, 9863l, 9864l, 10091l, 10092l, 10093l, 10094l, 10595l, 10944l, 23327l, 23836l, 30974l, 33343l,
                33344l, 33565l, 33571l, 34047l, 34048l, 34049l, 34051l, 34052l, 34435l, 34436
        };

        Set<Long> valueSet = Sets.newHashSet();
        for (Long l : valueArray) {
            valueSet.add(l);
        }

        SigningChecksumProvider provider = new SigningChecksumProvider(new SetChecksumProvider(), new RSAKeyCrypto());

        byte[] signature = provider.getChecksumSignature(valueSet, key);
        Assert.assertTrue(provider.verifyChecksumSignature(valueSet, key, signature));


    }
}
