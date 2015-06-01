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

package ezbake.publisher.service;

import java.nio.ByteBuffer;
import java.util.Properties;

import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.warehaus.*;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.ezbroadcast.core.thrift.SecureMessage;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.publisher.thrift.PublishData;
import ezbake.security.test.MockEzSecurityToken;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;

import static org.mockito.Mockito.*;

public class ContentPublisherServiceTest {
    private final TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    private static final Logger logger = LoggerFactory.getLogger(ContentPublisherServiceTest.class);

    private ContentPublisherService contentPublisher;
    private EzSecurityToken mockToken;
    private ThriftClientPool mockPool;

    @Before
    public void setup() throws Exception {
        // Mock the client pool
        mockPool = mock(ThriftClientPool.class);
        when(mockPool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME)).thenReturn("provenance");
        when(mockPool.getSecurityId(WarehausServiceConstants.SERVICE_NAME)).thenReturn("warehaus");
        when(mockPool.getSecurityId(InternalNameServiceConstants.SERVICE_NAME)).thenReturn("ins");

        // Mock INS
        InternalNameService.Client mockIns = mock(InternalNameService.Client.class);
        when(mockIns.getTopicsForFeed("content", "publishContent")).thenReturn(Sets.newHashSet("someTopic"));
        when(mockIns.ping()).thenReturn(true);
        when(mockPool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class)).thenReturn(mockIns);

        // Get test props
        Properties props = new ClasspathConfigurationLoader().loadConfiguration();
        EzProperties ezProps = new EzProperties(props, true);
        ezProps.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "app");

        // Mock the security stuff
        mockToken = MockEzSecurityToken.getMockUserToken("Jon Doe", "TS", Sets.newHashSet("U", "FOUO"), null, false);
        MockEzSecurityToken.populateAppInfo(mockToken, "content", "whatever");
        EzbakeSecurityClient mockSecurity = mock(EzbakeSecurityClient.class);
        when(mockSecurity.fetchDerivedTokenForApp(mockToken, "warehaus")).thenReturn(new EzSecurityTokenWrapper(mockToken));
        when(mockSecurity.fetchDerivedTokenForApp(mockToken, "ins")).thenReturn(new EzSecurityTokenWrapper(mockToken));
        when(mockSecurity.fetchDerivedTokenForApp(mockToken, "provenance")).thenReturn(new EzSecurityTokenWrapper(mockToken));

        contentPublisher = new ContentPublisherService(mockPool, mockSecurity, ezProps);
    }

    @Test
    public void testPing() throws TException {
        // Mock Provenance
        ProvenanceService.Client mockProvenance = mock(ProvenanceService.Client.class);
        when(mockProvenance.ping()).thenReturn(true);
        when(mockPool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class)).thenReturn(mockProvenance);

        // Mock Locksmith
        EzLocksmith.Client mockLocksmith = mock(EzLocksmith.Client.class);
        when(mockLocksmith.ping()).thenReturn(true);
        when(mockPool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class)).thenReturn(mockLocksmith);

        // Mock Warehaus
        WarehausService.Client mockClient = mock(WarehausService.Client.class);
        when(mockClient.ping()).thenReturn(true);
        when(mockPool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class)).thenReturn(mockClient);

        Assert.assertEquals("Ping should be true", true, contentPublisher.ping());
    }

    @Test
    public void putAndGet() throws TException {
        final String result[] = new String[1];
        Visibility visibility = new Visibility().setFormalVisibility("U");

        String parsedData = "this is parsed data";
        UpdateEntry entry = new UpdateEntry("DEV://contentpublisher/test");
        entry.setRawData(ByteBuffer.wrap("this is raw data".getBytes()));
        entry.setParsedData(ByteBuffer.wrap(parsedData.getBytes()));

        // Mock Warehaus
        WarehausService.Client mockClient = mock(WarehausService.Client.class);
        PutRequest request = new PutRequest();
        request.addToEntries(new PutUpdateEntry(entry, visibility));
        when(mockClient.put(request, mockToken)).then(new Answer<IngestStatus>() {
            @Override
            public IngestStatus answer(InvocationOnMock invocationOnMock) throws Throwable {
                result[0] = new String(((PutRequest)invocationOnMock.getArguments()[0]).getEntries().get(0).getEntry().getParsedData());
                return new IngestStatus().setTimestamp(12345).setStatus(IngestStatusEnum.SUCCESS);
            }
        });
        when(mockClient.ping()).thenReturn(true);
        when(mockPool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class)).thenReturn(mockClient);

        PublishData data = new PublishData();

        SSRJSON ssrjson = new SSRJSON();
        SSR ssr = new SSR();
        ssr.setUri(entry.getUri());
        ssr.setVisibility(visibility);
        ssrjson.setSsr(ssr);
        ssrjson.setJsonString(serializer.toString(ssr));

        data.setEntry(entry);
        data.setSsrjson(ssrjson);
        data.setFeedname("publishContent");

        logger.info("Sending data");
        logger.info(data.toString());
        contentPublisher.publish(data, visibility, mockToken);

        Multimap<String, byte[]> broadcasted = PublisherBroadcaster.getBroadcasted();
        for (String key: broadcasted.keys()) {
            SecureMessage message = ThriftUtils.deserialize(SecureMessage.class,
                    broadcasted.get(key).iterator().next());

            if (!key.equals(ContentPublisherService.SSR_TOPIC)) {
                Assert.assertEquals("Error for key " + key, parsedData, new String(message.getContent()));
            } else {
                SSRJSON publishedSSRJSON = ThriftUtils.deserialize(SSRJSON.class, message.getContent());
                Assert.assertEquals("Error for key " + key, ssrjson, publishedSSRJSON);
            }
        }

        logger.info("Starting fetch");
        Assert.assertEquals(parsedData, result[0]);
    }
}
