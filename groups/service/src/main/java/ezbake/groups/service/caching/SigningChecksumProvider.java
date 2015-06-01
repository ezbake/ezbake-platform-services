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

import ezbake.crypto.PKeyCrypto;
import ezbake.crypto.PKeyCryptoException;
import ezbake.groups.service.GroupsServiceModule;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Set;

/**
 * This class wraps the SetChecksumProvider with methods that generate signatures based on the result
 * of the checksum and the cache keys being used to store the objects.
 */
public class SigningChecksumProvider {
    private SetChecksumProvider baseProvider;
    private PKeyCrypto signer;

    @Inject
    public SigningChecksumProvider(SetChecksumProvider baseProvider, @Named(GroupsServiceModule.CRYPTO_NAME) PKeyCrypto signer) {
        this.baseProvider = baseProvider;
        this.signer = signer;
    }

    public byte[] getChecksumSignature(String data, String key) throws PKeyCryptoException {
        return signer.sign((data + key).getBytes());
    }

    /**
     * Generate a signature given the data and key
     * @param data data being checksummed and signed
     * @param key key being associated with the data. Will be factored into the signature
     * @return signature bytes
     * @throws PKeyCryptoException
     */
    public byte[] getChecksumSignature(Set<Long> data, String key) throws PKeyCryptoException {
        return signer.sign(serializeChecksum(baseProvider.getChecksum(data), key));
    }

    /**
     * Verify a checksum signature
     * @param data data being checksummed and signed
     * @param key key being associated with the data
     * @param signature signature bytes
     * @return true if valid signature
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     */
    public boolean verifyChecksumSignature(Set<Long> data, String key, byte[] signature)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        return signature != null && signer.verify(serializeChecksum(baseProvider.getChecksum(data), key), signature);
    }

    private byte[] serializeChecksum(Long checksum, String key) {
        ByteBuffer buffer =  ByteBuffer.allocate(Long.SIZE+key.length())
                .putLong(checksum)
                .put(key.getBytes());
        buffer.flip();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        return serialized;
    }
}
