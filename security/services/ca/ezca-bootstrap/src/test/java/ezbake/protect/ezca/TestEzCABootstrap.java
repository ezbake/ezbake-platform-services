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

package ezbake.protect.ezca;

import static org.junit.Assert.assertTrue;
import ezbake.base.thrift.EzBakeBaseService;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.crypto.PBECrypto;
import ezbake.persist.FilePersist;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistenceModel;

import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.SecurityIDNotFoundException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;

/**
 * User: jhastings
 * Date: 5/28/14
 * Time: 11:19 AM
 */
public class TestEzCABootstrap {
    Logger logger = LoggerFactory.getLogger(TestEzCABootstrap.class);
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setUpDirectory() {
        System.setProperty("EZCONFIGURATION_DIR", "src/test/resources");
    }

    private final String cert = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDFjCCAf6gAwIBAgIBATANBgkqhkiG9w0BAQUFADAqMRUwEwYDVQQLEwxUaGVT\n" +
            "b3VyY2VEZXYxETAPBgNVBAMTCEV6QmFrZUNBMCAXDTE0MTEyNTE4MTk0OVoYDzIw\n" +
            "NDQxMTI1MTgxOTQ5WjAqMRUwEwYDVQQLEwxUaGVTb3VyY2VEZXYxETAPBgNVBAMT\n" +
            "CEV6QmFrZUNBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA19H/DmXI\n" +
            "aYMAXcU2v5Wyj9nlX7U4z8U5NUHNAEdJSez6//rTvck/bopwCvvVlpLTE4QImPHf\n" +
            "Rccye/zuCUEA7ZC7hOyvQAcjLdYkdeo40vkhHJa74DH463+Iz/uw6TzPWdK8okRJ\n" +
            "MW1jc8QlqXnSjFCTuTz+7h7o+QNErGHSyWWqx8nYyStCPhPgPEfFZlN8GJ5t4Z3V\n" +
            "q4jUki6XGMPjDvL6wF60MbGL7d9Q1+65rvBO7JD6NarIrATAtMCuMIqeH38OwqVj\n" +
            "ss1FBzAI1QK6iaFicO4g6kb9sfrImJcpNvLzR2PlVe67x3/gsedi099sSQfv3Tk6\n" +
            "LGvCwENj0LWYgwIDAQABo0UwQzASBgNVHRMBAf8ECDAGAQH/AgEAMA4GA1UdDwEB\n" +
            "/wQEAwIBBjAdBgNVHQ4EFgQUaEb00k4OIFiPw679dCo0AqTWTAIwDQYJKoZIhvcN\n" +
            "AQEFBQADggEBAIwj69Z6LYLdqHOBqK5rxZW0oOzgv05i5WFbN31ArNO/7fVY3Hc9\n" +
            "tqAO2CupJjF5drJSiinLy+6/dWpcfaZ7phlYdtOG7+OlL8uFAnz8wSciQwC0093A\n" +
            "LqPW3cvmwo9ECT+kGf1GOigBjETuloikOrAlZRqEC6c9bpyfuLML56cWbDkRyqvC\n" +
            "TYLb3DBEZVb2mvlGxgBpcSgViDZGqvK3TRyaVSHiGcE1coB76gXse7uR2ibbNSIS\n" +
            "1FIpSi6tqeQqdogiNywkjjf5AxHgGwGcqhNFTRSURoQI+P8HASBFSzfu+hx4Od83\n" +
            "PVuworkkw7nZT9pDUc6S7tjSVLHgxJc3aKk=\n" +
            "-----END CERTIFICATE-----\n";
    private final String privateKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
            "MIIBOQIBAAJBAMjTl3+eqVXBfyR2jxcYgWGp3pBoQCoxNCtUDrTwqemYyvsJqLWY\n" +
            "gRb1epdRnUnryn8CJViaXPdzEP0GOE96s0kCAwEAAQJAMwulTGj1vbhrKsd/42z1\n" +
            "Je/ZhHcbKB5Nll3NRyyM47Tgzzy/q2wUGT+Ei6oUrlnrBXNnOmNMZqvgjerGAfST\n" +
            "AQIhAPjV0StwbdjFmde4A2iWxGBvKoSa+DDiR1RTMnEaBix5AiEAzpvmKhiSlU11\n" +
            "23Bmg0EdQBYTPsnT3DNJr4rTMgFRaVECIE9qThuDAkvQpMzIGa5jj8EHOZagrt1L\n" +
            "GmC7PLoECDxhAiBBLDpJcyDiCeIwi1867hJNAemmN8IlxuPqhM8kCfhlUQIgPml1\n" +
            "2T2/FC9lFH0bpry0VNWlVrS/aHnoBS5oWaZcf6U=\n" +
            "-----END RSA PRIVATE KEY-----";
    private final String publicKey = "-----BEGIN PUBLIC KEY-----\n" +
            "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAMjTl3+eqVXBfyR2jxcYgWGp3pBoQCox\n" +
            "NCtUDrTwqemYyvsJqLWYgRb1epdRnUnryn8CJViaXPdzEP0GOE96s0kCAwEAAQ==\n" +
            "-----END PUBLIC KEY-----";

    @Before
    public void setUpFileSystem() throws IOException {
        
        
        String root = folder.getRoot().toString();
        logger.debug("Root {}", root);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "ezbakeca", "x509_cert", "", "data")), cert);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "ezbakeca", "private_key", "", "data")), "ezbake ca private key");

        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "name", "", "data")), "_Ez_Security");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "status", "", "data")), "ACTIVE");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "private_key", "", "data")), privateKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "public_key", "", "data")), publicKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "x509_cert", "", "data")), cert);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "owner", "", "data")), "");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Security", "admins", "", "data")), "[]");

        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "name", "", "data")), "100");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "status", "", "data")), "ACTIVE");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "private_key", "", "data")), privateKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "public_key", "", "data")), publicKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "x509_cert", "", "data")), cert);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "owner", "", "data")), "");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "100", "admins", "", "data")), "[]");
        
        //TODO: Add some stuff for testing the EzRegistration
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "name", "", "data")), "_Ez_Registration");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "status", "", "data")), "ACTIVE");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "private_key", "", "data")), privateKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "public_key", "", "data")), publicKey);
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecuriyt_reg", "_Ez_Registration", "x509_cert", "", "data")), "my cert");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "owner", "", "data")), "");
        FileUtils.writeStringToFile(new File(FilePersist.joinPath(root, "ezsecurity_reg", "_Ez_Registration", "admins", "data")), "[]");

    }

    @Test
    public void testMain() throws IOException, DataLengthException, IllegalStateException, InvalidCipherTextException,
            EzConfigurationLoaderException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException,
            InvalidKeySpecException, AccumuloSecurityException, AccumuloException, RegistrationException,
            SecurityIDNotFoundException {
        Properties ezConfig = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        String[] args = new String[] {
            "-d", folder.getRoot().toString(),
            "-n", "_Ez_Security,100,_Ez_Registration"
        };

        EzCABootstrap booter = new EzCABootstrap(ezConfig, args);
        booter.run();

        // Make sure we can get registrations out
        AccumuloRegistrationManager registrationManager = new AccumuloRegistrationManager(ezConfig);
        AppPersistenceModel ezSecurity = registrationManager.getRegistration(new String[]{"U"}, "_Ez_Security", null, null);
        AppPersistenceModel ezbakeca = registrationManager.getRegistration(new String[]{"U"}, "ezbakeca", null, null);


        List<AppPersistenceModel> list = booter.getAppModels();
        AppPersistenceModel appModel = list.get(0);
        
        String passcode = appModel.getPasscode();
        PBECrypto pbeCrypto = new PBECrypto(passcode);
        pbeCrypto.setSalt(ezSecurity.getSalt());
        byte[] data = pbeCrypto.decrypt(pbeCrypto.generateKey(), ezSecurity.getEncryptedPk());
        Assert.assertArrayEquals(privateKey.getBytes(), data);
    }
    
    @Ignore("Doesn't verify anything")
    @Test
    public void testWriteTar() {
        AppPersistenceModel apm = new AppPersistenceModel();
        apm.setPublicKey(publicKey);

        EzCABootstrap.createAndWriteTarball("test", EzCABootstrap.certsForApp(apm, null, null, null), folder.getRoot().toString());
    }
    

    @Test
    public void testArgParser() {
        Parameters p = new Parameters(new String[]{"-d", "test", "--dry-run"});
        Assert.assertTrue(p.dryRun);
    }
}
