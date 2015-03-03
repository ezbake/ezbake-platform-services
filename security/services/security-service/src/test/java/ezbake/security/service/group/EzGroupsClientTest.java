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

package ezbake.security.service.group;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.TokenType;
import ezbake.groups.thrift.EzGroups;
import ezbake.groups.thrift.EzGroupsConstants;
import ezbake.security.common.core.EzSecurityTokenProvider;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * User: jhastings
 * Date: 7/31/14
 * Time: 9:27 PM
 */
public class EzGroupsClientTest {

    @Before
    public void setUp() {
    }

    @Test
    public void test() throws TException {
        String ezGroupSid = "12345";
        List<String> chain = Lists.newArrayList("test");

        EzGroups.Client mockClient = EasyMock.createNiceMock(EzGroups.Client.class);
        EasyMock.expect(mockClient.createUserAndGetAuthorizations(
                EasyMock.anyObject(EzSecurityToken.class),
                EasyMock.eq(chain),
                EasyMock.eq("JEFF"),
                EasyMock.eq("JEFF H")
        )).andReturn(Sets.newHashSet(1l,2l,3l));

        EzSecurityTokenProvider tokenProvider = EasyMock.createMock(EzSecurityTokenProvider.class);
        EasyMock.expect(tokenProvider.get(ezGroupSid))
                .andReturn(MockEzSecurityToken.getMockAppToken("01"))
                .anyTimes();

        ThriftClientPool pool = EasyMock.createNiceMock(ThriftClientPool.class);
        EasyMock.expect(pool.getClient(EzGroupsConstants.SERVICE_NAME, EzGroups.Client.class)).andReturn(mockClient);
        EasyMock.expect(pool.getSecurityId(EzGroupsConstants.SERVICE_NAME)).andReturn(ezGroupSid).anyTimes();

        EasyMock.replay(tokenProvider, mockClient, pool);

        EzGroupsClient client = new EzGroupsClient(tokenProvider, pool);

        Set<Long> authorizations = client.getAuthorizations(chain, TokenType.USER, "JEFF", "JEFF H");
        Assert.assertNotNull(authorizations);

    }

}
