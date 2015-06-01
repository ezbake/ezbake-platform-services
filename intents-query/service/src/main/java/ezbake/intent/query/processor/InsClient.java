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

package ezbake.intent.query.processor;

import org.apache.thrift.TException;

import ezbake.configuration.EzConfiguration;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.thrift.ThriftClientPool;

public class InsClient {

    private final ThriftClientPool pool;
    private final Class<InternalNameService.Client> insclass;

    public InsClient(final EzConfiguration configuration) {
        pool = new ThriftClientPool(configuration.getProperties());
        insclass = InternalNameService.Client.class;
    }

    public void close() {
        pool.close();
    }

    public InternalNameService.Client getThriftClient() {
        try {
            return pool.getClient(InternalNameServiceConstants.SERVICE_NAME, insclass);
        } catch (final TException ex) {
            final String message = "An error occurred getting a Thrift client from the thread pool!";
            System.err.println(message);
            throw new RuntimeException(message, ex);
        }
    }

    public void returnToPool(InternalNameService.Client client) {
        try {
            pool.returnToPool(client);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
