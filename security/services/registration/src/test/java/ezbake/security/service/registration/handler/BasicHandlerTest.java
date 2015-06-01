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

package ezbake.security.service.registration.handler;

import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.SecurityIDNotFoundException;
import org.apache.thrift.TException;
import org.junit.*;

/**
 * User: jhastings
 * Date: 4/8/14
 * Time: 11:41 AM
 */
public class BasicHandlerTest extends HandlerBaseTest {



    private static final String cert = "-----BEGIN CERTIFICATE-----\n" +
            "MIICvDCCAaSgAwIBAgIBAjANBgkqhkiG9w0BAQUFADAqMRUwEwYDVQQLEwxUaGVT\n" +
            "b3VyY2VEZXYxETAPBgNVBAMTCEV6QmFrZUNBMCAXDTE0MTEyNTE4MTk1MFoYDzIw\n" +
            "NDQxMTI1MTgxOTUwWjAXMRUwEwYDVQQDFAxfRXpfU2VjdXJpdHkwggEiMA0GCSqG\n" +
            "SIb3DQEBAQUAA4IBDwAwggEKAoIBAQC8cDrwjzIA5duFy4GSxdNEGH8p19oVy6ER\n" +
            "f3v95jCjNeoDZxJsDTi5Za5NRqBurIlg33vYNWc3mVviO1p1uPaZtr+Jz7gD8+76\n" +
            "aNWHhnqIL7LluvEs3lfAcZtFuFfazmOEjAZ3vL4G4iH5zNeloX1oFNwhu1zfUdh1\n" +
            "KqozOUYRhYuXr7VW0nrUAaUcWwjJdVRIfeoB9S+e386F8fMwbS/Oh5NfatVDz96d\n" +
            "TBgI/UGehbDtRyirCy9hFOjTu6c+vMqVMoKPIaVG1n1GWFqap9JMDBjiKRrzSOZv\n" +
            "bxtkJDxM4dkb//ATkp6cV4g7VLo9c6DpOLj8AzKxL8AbOtTodIlZAgMBAAEwDQYJ\n" +
            "KoZIhvcNAQEFBQADggEBAIrpIDo8Ob+svzyF7xCVwLH7C5afK8A+8n8tI78UheTe\n" +
            "bR7SBrwP/0wK7Xq+uKHkAKKmyMlree95gCYJyHf1sJOFDGt1TEvxuG7oykkgHWfY\n" +
            "TimHoDvvp7QwJTlSkEDZpiTAn94S6Y9LYAygvvK9guDNFqO1DoU0aTeNElS6rq+w\n" +
            "3esV+iGBPyzjmATPhMuZhm6wOiu8QEjyKRBuOqaE54GpRY4GvSIdr1lzamKUoUl/\n" +
            "EixLzFBu11hkqIaM81/wvtWszmntTHt3uVhc39azZqWIknRUaocrRw2/1sUOOE5t\n" +
            "6eGyUd1V2s6w+yCezGhRmKzjTVmuu575yB3Gse7/nGg=\n" +
            "-----END CERTIFICATE-----\n";

    private static final String pk = "-----BEGIN RSA PRIVATE KEY-----\n" +
            "MIIEowIBAAKCAQEAvHA68I8yAOXbhcuBksXTRBh/KdfaFcuhEX97/eYwozXqA2cS\n" +
            "bA04uWWuTUagbqyJYN972DVnN5lb4jtadbj2mba/ic+4A/Pu+mjVh4Z6iC+y5brx\n" +
            "LN5XwHGbRbhX2s5jhIwGd7y+BuIh+czXpaF9aBTcIbtc31HYdSqqMzlGEYWLl6+1\n" +
            "VtJ61AGlHFsIyXVUSH3qAfUvnt/OhfHzMG0vzoeTX2rVQ8/enUwYCP1BnoWw7Uco\n" +
            "qwsvYRTo07unPrzKlTKCjyGlRtZ9RlhamqfSTAwY4ika80jmb28bZCQ8TOHZG//w\n" +
            "E5KenFeIO1S6PXOg6Ti4/AMysS/AGzrU6HSJWQIDAQABAoIBAEz96gcJ6ttVDzl+\n" +
            "acWnUGedPq/BAtku5vN4TBf0KmE1ERUs0ukVCd0uP2ZRehFeK49KIJa5Ux/zaAhq\n" +
            "Sc6ZsSAi++V52my7CSSFGuGRv5TPMGAO3qV/fwkhIdj9td+vvheVArt/gYDcehdP\n" +
            "a7i/37Zb94lMvWh9T1yn/vyI5SkY3K99+L3M1y6DdpXmQbKXE8GfLRwPwPbfDyk7\n" +
            "V2YGzcvFeCxvwjM/YQqJbQLwKed8kXSBr9+UPPw8pk40hzhfxYdemv6a62UQHepE\n" +
            "LOL1j6jFL4d9j2HeMd1eOPD2QCSDp7hK3HNllsxchzQ+8IdcxiVCPm348oN+un1L\n" +
            "C7sxREUCgYEA0ByzXIWyNcTRc7ZgX2i9GbRpDmk6N5xA40k2kxrphasJ/D1wSVO3\n" +
            "INKKuGp1HfESEJVTrc8uZOg6cBRiFsCDt0ywbq5ZNsHHLwyUloTjy1V5UnAhRyuv\n" +
            "016NDZbf8EIULzEO6rGf0wDHw3ye0ghpmoR4cE2QHjd1GcnJlSCmlsMCgYEA58yb\n" +
            "RJlHJvqR3IvDdqbMwudYEf5F64cjSuCLmvaafLE9oatTHjjf8KUSvLmWD5W31zXR\n" +
            "psR1Gj1o+XYKvnrPWOkc25WLBmWq/2L1tVAW97rv0IZ13BCCiwZnaNqN0Nuuj7aL\n" +
            "cZq3y8VKMRh7VRaTohXHMve1JcaS2YYO1W2GdbMCgYEAuNx1ur8MEVUWlMmxC683\n" +
            "IqkuFN4GF7XVsc+sCboDK3hGM2jD4G7boe1DyhLOm90zJcXvgdoipQHgPwTsKLez\n" +
            "iNQ3eOmoV8qDy1hKePXsfwca8M6n0NeOpJw9gY++tmWMFmtmi7ViegUcbZq6XWmZ\n" +
            "nOcFMQTE+wJaI6EqTiylrg8CgYAIyDy9vZzvgiDSnUz7ithJLiCtFdgqU0VoCdfg\n" +
            "OCWkQcbXADm29GqvoGF0WwevcXm0oqpdyiWxp8/5W5qOmvKOKM7aFvFcfa+b23D5\n" +
            "vJ4SJrf9S4rdmpaHk+eJFna3Cgu0EDN6S2VZSBFGiOnrUF6pjm+so6vuUXaw3R5k\n" +
            "wbCNdwKBgDl6bl8bvZyKHWOUR6x9ZtVEzIugvtXImicuhem+iPuJ5MXKXPc2Ng/n\n" +
            "sCX2oIJiMYEFgcrzEk10CZ915kwbPXorpiX7+JLqyCE/6QbTFf7jf1uaxKbatQNk\n" +
            "uZHE7gKYMmTF+FMqqQ5gluPHhVmum5fGqSL7pVbGX1kbWYnmJGmz\n" +
            "-----END RSA PRIVATE KEY-----";

    @Test
    public void testGenerateIdNotNegative() {
        for (int i=0; i < 1000; ++i) {
            String id = handler.generateSecurtyId();
            long idL = Long.parseLong(id);
            Assert.assertTrue(idL >= 0);
        }
    }

    @Test
    public void testPing() throws TException {
        Assert.assertTrue(handler.ping());
    }

    @Test(expected=SecurityIDNotFoundException.class)
    public void testStatusNoRegistration() throws RegistrationException, SecurityIDNotFoundException, TException {
        handler.getStatus(getTestEzSecurityToken(false), "DoesNotExist");
    }

}
