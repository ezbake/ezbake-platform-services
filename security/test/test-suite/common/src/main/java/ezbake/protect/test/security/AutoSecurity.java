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

package ezbake.protect.test.security;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.persist.EzPersistUtil;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.SecurityID;
import ezbake.security.persistence.impl.AccumuloRegistrationManager;
import ezbake.security.persistence.model.AppPersistenceModel;
import ezbake.security.thrift.AppCerts;
import ezbake.security.thrift.ApplicationRegistration;
import ezbake.security.thrift.EzSecurityRegistration;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.security.thrift.RegistrationException;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.security.thrift.SecurityIDNotFoundException;
import ezbake.thrift.ThriftClientPool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoSecurity {

    private Logger log = LoggerFactory.getLogger(AutoSecurity.class);
    private Properties ezConfig;
   
    private String ezSecurityRegistrationId;
    
    public static final String tmpRoot = "/tmp";
    public static final String pkiSub = "pki";
    
    private AccumuloRegistrationManager arm;
    
    private boolean deleteTmpFiles;
    
    public AutoSecurity(Properties ezConfig, boolean deleteTmpFiles) throws AccumuloException, AccumuloSecurityException {
        this.ezConfig = ezConfig;
    
     
        this.arm = new AccumuloRegistrationManager(ezConfig);
        this.deleteTmpFiles = deleteTmpFiles;
    }
   
    
    public Map<String, Path> downloadCerts(String[] auths, String owner, RegistrationStatus status) throws IOException, RegistrationException, SecurityIDNotFoundException {
        EzSecurityRegistration.Client client = null;
        FileOutputStream fos = null;
        
        Map<String, Path> appCertLocMap = new HashMap<String, Path>();
        
        try {
          
            List<AppPersistenceModel> apps = arm.all(auths, owner, status);
            
            log.debug("Found [{}] Apps.", apps.size());
            
            
            AppPersistenceModel security = arm.getRegistration(auths, SecurityID.ReservedSecurityId.EzSecurity.getId(), owner, status);
            AppPersistenceModel ca = arm.getRegistration(auths, SecurityID.ReservedSecurityId.CA.getId(), owner, status);
            
            Path tmpDir = this.mkdir(pkiSub);
            log.debug("Temp Dir {}", tmpDir.toString());
            File tmpFile = tmpDir.toFile();
            String currDir = tmpFile.getAbsolutePath();
            
            for(AppPersistenceModel app : apps) {
                File appTmpFile = new File(tmpFile.getAbsolutePath() + "/" + app.getAppName());
                appTmpFile.mkdir();
                appCertLocMap.put(app.getAppName(), appTmpFile.toPath());
                
                if(isDeletingTempFiles()) 
                    appTmpFile.deleteOnExit();
                
                
                AppCerts appCerts = EzPersistUtil.getAppCerts(app, ca, security);
                
                for(AppCerts._Fields f : AppCerts._Fields.values()) {
                    String fName = f.getFieldName().replace("_", ".");

                    log.debug("Creating File {}", fName);
                    byte[] data = (byte[])appCerts.getFieldValue(f);
                    
                   // log.debug("Writing Data {}", data);
                    
                    File file = new File(appTmpFile.getAbsolutePath() + "/" + fName);
                    
                    if(isDeletingTempFiles()) {
                        file.deleteOnExit();
                    }
                    
                    fos = new FileOutputStream(file);
                    fos.write(data);
                    
                    fos.close();
                }
            }
            
        }
        catch(IOException e) {
            log.error("Error {}" , e);
        }
        finally {
            log.debug("Fin!");
        }
        
        return appCertLocMap;
    }
    
    protected Path mkdir(String dir) throws IOException {
      
        
        log.debug("Generated Path {}", dir);
        Path tmpPath = Files.createTempDirectory(dir);
        
        if(this.isDeletingTempFiles())
           tmpPath.toFile().deleteOnExit();
        
        tmpPath.toFile().mkdir();
        
        return tmpPath;
    }
    
    protected boolean isDeletingTempFiles() {
        return this.deleteTmpFiles;
    }
    
    protected void setIsDeletingTempFiles(boolean deleteTmpFiles) {
        this.deleteTmpFiles = deleteTmpFiles;
    }
    
    
}
