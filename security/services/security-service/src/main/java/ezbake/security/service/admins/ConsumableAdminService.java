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

package ezbake.security.service.admins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.InputStream;
import java.util.Set;

/**
 * User: jhastings
 * Date: 8/18/14
 * Time: 10:54 AM
 */
public class ConsumableAdminService extends AdministratorService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumableAdminService.class);

    protected boolean waitForAdmins = true;

    @Inject
    public ConsumableAdminService(@Named("Admin File") String adminFile) {
        super(adminFile);
    }

    @Override
    public synchronized boolean loadUpdate(InputStream is) {
        while(!waitForAdmins) {
            logger.info("Admin service waiting to load further updates until previous update is consumed");
            try {
                wait();
                logger.info("Previous update was consumed. Loading updates will continue now");
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        boolean keepGoing = super.loadUpdate(is);
        waitForAdmins = false;
        notifyAll();
        return keepGoing;
    }

    @Override
    public synchronized Set<String> getAdmins() {
        while(waitForAdmins) {
            logger.info("Admin service waiting for any updates on admins file");
            try {
                wait();
                logger.info("Admin update received. Will consume update");
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        waitForAdmins = true;
        notifyAll();
        return admins;
    }
}
