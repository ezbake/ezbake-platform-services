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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Set;

/**
 * User: jhastings
 * Date: 7/25/14
 * Time: 3:24 PM
 */
public class PublishingAdminService extends ConsumableAdminService {
    private static final Logger logger = LoggerFactory.getLogger(PublishingAdminService.class);
    private static final String ADMIN_SYNC_PATH = "/ezsecurity/sync/admins";

    private AdminSyncer adminSyncer;
    private LeaderSelector leaderSelector;

    @Inject
    public PublishingAdminService(@Named("Admin File") String adminFile, CuratorFramework curator, AdminSyncer syncer) {
        super(adminFile);

        if (curator.getState() == CuratorFrameworkState.LATENT) {
            curator.start();
        }

        adminSyncer = syncer;

        leaderSelector = new LeaderSelector(curator, ADMIN_SYNC_PATH, new AdminLeaderSelector());
        if (hasAdmins()) {
            logger.info("Had admins file, will attempt to gain leadership");
            if (leaderSelector != null && !leaderSelector.hasLeadership()) {
                leaderSelector.start();
            }
        } else {
            logger.info("This instance has no admins. Will not attempt to gain leadership");
        }
    }

    @Override
    public void close() {
        super.close();
        if (leaderSelector != null && leaderSelector.hasLeadership()) {
            leaderSelector.close();
        }
    }

    private class AdminLeaderSelector extends LeaderSelectorListenerAdapter {

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            logger.info("Taking leadership as Admin updater");
            while(watchThread.isRunning()) {
                adminSyncer.sendUpdates(getAdmins());
            }
            logger.info("Relinquishing leadership");
        }
    }

}
