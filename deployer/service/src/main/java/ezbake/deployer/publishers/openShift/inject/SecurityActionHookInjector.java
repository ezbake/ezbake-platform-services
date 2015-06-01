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

import ezbake.deployer.utilities.Utilities;

import java.io.File;
import java.util.List;

/**
 * This class gets the files that need to be  injected as action hooks for security
 */
public class SecurityActionHookInjector extends ClasspathResourceInjector {

    public static final String SECURITY_ACTION_HOOKS = "ezdeploy.openshift.security.action_hooks";
    private static final List<String> securityActionHooks = Utilities.getResourcesFromClassPath(
            SecurityActionHookInjector.class, SECURITY_ACTION_HOOKS);

    @Override
    public List<String> getResources() {
        return securityActionHooks;
    }

    @Override
    public File getBasePath() {
        return OPENSHIFT_ACTION_HOOKS_PATH;
    }
}
