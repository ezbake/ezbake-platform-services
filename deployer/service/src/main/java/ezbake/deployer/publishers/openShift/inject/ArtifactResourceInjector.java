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

package ezbake.deployer.publishers.openShift.inject;

import com.google.common.collect.ImmutableSet;
import ezbake.deployer.impl.Files;
import ezbake.deployer.impl.PosixFilePermission;
import ezbake.services.deploy.thrift.DeploymentException;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * This interface defines the methods required to add new files to the openshift
 * deployed artifact. It will return a list of resources including:
 *   1) the path to where the file should go (in the repo)
 *   2) an open InputStream to the data
 *   3) any required file permissions
 */
public interface ArtifactResourceInjector {
    public static final File OPENSHIFT_ACTION_HOOKS_PATH = Files.get(".openshift", "action_hooks");

    public final static Set<String> executableScripts = ImmutableSet.<String>builder()
            .add("start", "stop", "pre_build", "pre_start_jbossas", "pre_restart_jbossas", "deploy")
            .build();

    public static final Set<PosixFilePermission> executablePerms = ImmutableSet.<PosixFilePermission>builder()
            .add(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE).build();

    /**
     * This will return a list of files that should be injected into the artifact
     *
     * @return list of resources to inject
     */
    public List<ArtifactResource> getInjectableResources() throws DeploymentException;
}
