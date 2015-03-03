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

import org.apache.thrift.TException;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by eperry on 4/24/14.
 */
public class EncryptionUtilityTest {

    @Test
    public void testEncryptAndDecrypt() throws UnsupportedEncodingException, TException {
        String message = "hey there!";
        String password = "thisisthepassword!@#@!@#adf23r234234";
        final Random r = new SecureRandom();
        byte[] salt = new byte[32];
        byte[] iv = new byte[16];
        r.nextBytes(salt);
        byte[] result = EncryptionUtility.encryptData(message.getBytes("UTF-8"), iv, salt, password);

        byte[] decrypted = EncryptionUtility.decryptData(result, password, salt, iv);
        assertEquals("Decrypted data equals data before encryption", message, new String(decrypted, "UTF-8"));
    }
}
