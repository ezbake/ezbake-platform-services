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

package ezbake.deployer.cli.commands;

import com.google.common.base.Supplier;
import ezbake.deployer.utilities.ArtifactDataEntry;
import ezbake.security.thrift.AppCerts;
import ezbake.security.thrift.EzSecurityRegistration;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.services.deploy.thrift.DeploymentException;
import ezbake.thrift.ThriftClientPool;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static ezbake.deployer.utilities.Utilities.SSL_CONFIG_DIRECTORY;

/**
 *
 */
public class SSLCertsCommand extends BaseCommand {

    private final Supplier<ThriftClientPool> poolSupplier;

    public SSLCertsCommand(Supplier<ThriftClientPool> poolSupplier) {
        super();
        this.poolSupplier = poolSupplier;
    }

    @Override
    public void call() throws IOException, TException {
        String[] args = globalParameters.unparsedArgs;
        minExpectedArgs(2, args, this);
        String securityId = args[0];
        String filePath = args[1];

        List<ArtifactDataEntry> certs = new ArrayList<>();
        EzSecurityRegistration.Client client = null;
        ThriftClientPool pool = poolSupplier.get();
        try {
            client = pool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);

            AppCerts s = client.getAppCerts(
                    getSecurityToken(pool.getSecurityId(EzSecurityRegistrationConstants.SERVICE_NAME)), securityId);
            for (AppCerts._Fields fields : AppCerts._Fields.values()) {
                Object o = s.getFieldValue(fields);
                if (o instanceof byte[]) {
                    String fieldName = fields.getFieldName().replace("_", ".");
                    TarArchiveEntry tae = new TarArchiveEntry(new File(
                            new File(SSL_CONFIG_DIRECTORY, securityId), fieldName));
                    certs.add(new ArtifactDataEntry(tae, (byte[]) o));
                }
            }

            ArchiveStreamFactory asf = new ArchiveStreamFactory();
            FileOutputStream fos = new FileOutputStream(filePath);
            GZIPOutputStream gzs = new GZIPOutputStream(fos);
            try (TarArchiveOutputStream aos = (TarArchiveOutputStream) asf.createArchiveOutputStream(ArchiveStreamFactory.TAR, gzs)) {
                aos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

                for (ArtifactDataEntry entry : certs) {
                    aos.putArchiveEntry(entry.getEntry());
                    IOUtils.write(entry.getData(), aos);
                    aos.closeArchiveEntry();
                }
                aos.finish();
                gzs.finish();
            } catch (ArchiveException ex) {
                throw new DeploymentException(ex.getMessage());
            } finally {
                IOUtils.closeQuietly(fos);
            }
        } finally {
            pool.returnToPool(client);
        }
    }

    @Override
    public String getName() {
        return "sslcerts";
    }

    @Override
    public void displayHelp() {
        System.out.println(getName() + " <securityId> <output file name>");
        System.out.println("\tDownload the ssl certs for the given application");
    }

    @Override
    public String quickUsage() {
        return getName() + " - Download the ssl certs for the given application";
    }
}
