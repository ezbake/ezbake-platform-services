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

package ezbake.warehaus.tools;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;

import org.apache.thrift.TException;

import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.base.thrift.Date;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Time;
import ezbake.base.thrift.TimeZone;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.warehaus.WarehausService;
import ezbake.warehaus.WarehausServiceConstants;

public class ToolHelper {
	public static WarehausService.Client createClient(ThriftClientPool pool) throws TException {
		return pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
	}

	public static byte[] importFile(String filename) throws IOException {
		FileInputStream file = new FileInputStream(filename);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int read = file.read();
		while (read > -1) {
			baos.write(read);
			read = file.read();
		}
		file.close();
		return baos.toByteArray();
	}
	
	public static EzSecurityToken importToken() throws IOException, TException, EzConfigurationLoaderException { 
	    ThriftClientPool pool = null;
	    try {
            EzProperties properties = new EzProperties(new EzConfiguration().getProperties(), true);
            pool = new ThriftClientPool(properties);  
            return importToken(pool);
	    } finally {
	        if (pool != null) {
	            pool.close();
	        }        
        }
    }

	public static EzSecurityToken importToken(ThriftClientPool pool) throws IOException, TException, EzConfigurationLoaderException {    
	    EzProperties properties = new EzProperties(new EzConfiguration().getProperties(), true);
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(properties);
        EzSecurityToken token = securityClient.fetchAppToken(pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME));
        securityClient.close();
		return token;
	}

	public static void exportFile(String filename, byte[] content) throws IOException {
		FileOutputStream file = new FileOutputStream(filename);
		file.write(content);
		file.close();
	}
}
