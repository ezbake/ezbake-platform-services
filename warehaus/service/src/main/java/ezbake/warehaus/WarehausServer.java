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

import java.util.Properties;

import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbake.security.client.EzbakeSecurityClient;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class WarehausServer {
	private static AccumuloWarehaus handler;

	@SuppressWarnings("rawtypes")
   private static WarehausService.Processor processor;

	@SuppressWarnings("rawtypes")
   public static void main(String[] args) {
		try {
			Properties config = new Properties();
			config.put(EzBakePropertyConstants.ACCUMULO_USE_MOCK, "true");
			config.put(EzBakePropertyConstants.ACCUMULO_USERNAME, "root");
			config.put(EzBakePropertyConstants.ACCUMULO_PASSWORD, "");
			config.put(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true");
			config.put(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "test");
			handler = new AccumuloWarehaus();
			handler.setConfigurationProperties(config);
			processor = (WarehausService.Processor) handler.getThriftProcessor();
			final int port = Integer.parseInt(args[0]);
			Runnable simple = new Runnable() {
				public void run() {
					TServerTransport serverTransport;
					try {
						serverTransport = new TServerSocket(port);
						TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
						System.out.println("Starting the simple server...");
						server.serve();
					} catch (TTransportException e) {
						System.err.println("Transport Exception: " + e.getMessage());
					}
				}
			};
			new Thread(simple).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
