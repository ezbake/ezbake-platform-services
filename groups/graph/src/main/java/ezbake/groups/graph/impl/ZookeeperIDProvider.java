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

package ezbake.groups.graph.impl;

import com.google.common.base.Joiner;
import com.google.inject.name.Named;
import ezbake.groups.graph.api.GroupIDProvider;
import ezbake.groups.graph.api.GroupIDPublisher;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * User: jhastings
 * Date: 9/18/14
 * Time: 12:41 PM
 */
public class ZookeeperIDProvider implements GroupIDProvider {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperIDProvider.class);

    public static String getNamespacedZkPath(String appNamespace, String path) {
        String namespacedPath = Joiner.on("/").skipNulls().join(zookeeper_namespace, appNamespace, path);
        logger.info("Using zookeeper path: {}", path);
        return namespacedPath;
    }

    private CuratorFramework curator;
    private DistributedAtomicLong index;
    private InterProcessReadWriteLock counterLock;
    private String indexZkPath;
    private String lockZkPath;
    private String validZkPath;
    private GroupIDPublisher idGetter;

    public ZookeeperIDProvider(CuratorFramework curator) {
        this(curator, null, null);
    }

    public ZookeeperIDProvider(CuratorFramework curator, String prefix) {
        this(curator, null, prefix);
    }

    @Inject
    public ZookeeperIDProvider(CuratorFramework curator, GroupIDPublisher idGetter,
                               @Named("APP_NAME") @Nullable String prefix) {
        this.idGetter = idGetter;
        this.curator = curator;
        if (curator.getState() == CuratorFrameworkState.LATENT) {
            curator.start();
        }

        // Get the namespaced path to each zookeeper node
        indexZkPath = getNamespacedZkPath(prefix, index_counter);
        lockZkPath = getNamespacedZkPath(prefix, counter_lock);
        validZkPath = getNamespacedZkPath(prefix, index_valid);

        // Set up the counter
        index = new DistributedAtomicLong(curator, indexZkPath, curator.getZookeeperClient().getRetryPolicy());

        // Set up the validity lock
        counterLock = new InterProcessReadWriteLock(curator, lockZkPath);

        try {
            setCurrentID();
        } catch (Exception e) {
            throw new RuntimeException("Unable to set the current group Id, fix errors and restart", e);
        }
    }

    public String getIndexZkPath() {
        return indexZkPath;
    }

    public String getLockZkPath() {
        return lockZkPath;
    }

    public void setCurrentID(long id) throws Exception {
        InterProcessMutex wlock = counterLock.writeLock();
        acquireLock(wlock, 250, TimeUnit.MILLISECONDS);
        try {
            if (isIdValid()) {
                return;
            }

            AtomicValue<Long> current = index.trySet(id);
            if (!current.succeeded()) {
                setIdValid(false);
                throw new Exception("Failed to set current id. Can only set value to 0, but current value is: " +
                        current.postValue());
            }
            setIdValid(true);
        } finally {
            wlock.release();
        }
    }

    /**
     * Zookeeper ID Provider only allows the setting of the current id to 0, and only if the value is not present in
     * zookeeper
     *
     * @throws Exception
     */
    @Override
    public void setCurrentID() throws Exception {
        InterProcessMutex wlock = counterLock.writeLock();
        acquireLock(wlock, 250, TimeUnit.MILLISECONDS);
        try {
            if (isIdValid()) {
                return;
            }
            long id = idGetter.getCurrentId();

            AtomicValue<Long> current = index.trySet(id);
            if (!current.succeeded()) {
                setIdValid(false);
                throw new Exception("Failed to set current id. Can only set value to 0, but current value is: " +
                        current.postValue());
            }
            setIdValid(true);
        } finally {
            wlock.release();
        }
    }

    @Override
    public long currentID() throws Exception {
        InterProcessMutex rlock = counterLock.readLock();
        acquireLock(rlock, 250, TimeUnit.MILLISECONDS);
        try {
            AtomicValue<Long> current = index.get();
            if (!current.succeeded()) {
                setIdValid(false);
                throw new Exception("Failed to get id from zookeeper");
            }
            return current.postValue();
        } finally {
            rlock.release();
        }
    }

    @Override
    public long nextID() throws Exception {
        InterProcessMutex wlock = counterLock.writeLock();
        acquireLock(wlock, 250, TimeUnit.MILLISECONDS);
        try {
            AtomicValue<Long> current = index.increment();
            if (!current.succeeded()) {
                setIdValid(false);
                throw new Exception("Failed to increment id in zookeeper");
            }
            return current.postValue();
        } finally {
            wlock.release();
        }
    }

    /**
     * This updates the value of the 'valid' flag in zookeeper
     * @param valid
     * @throws Exception
     */
    private void setIdValid(boolean valid) throws Exception {
        byte[] data = valid ? "1".getBytes() : "0".getBytes();

        InterProcessMutex wlock = counterLock.writeLock();
        acquireLock(wlock, 250, TimeUnit.MILLISECONDS);
        try {
            curator.inTransaction()
                    .delete().forPath(validZkPath)
                    .and()
                    .create().forPath(validZkPath, data)
                    .and().commit();
        } catch (KeeperException.NoNodeException e) {
            // create the node
            curator.create().forPath(validZkPath, data);
        } catch (KeeperException.NodeExistsException e) {
            // delete and try again
            curator.inTransaction()
                    .delete().forPath(validZkPath)
                    .and()
                    .create().forPath(validZkPath, data)
                    .and().commit();
        } finally {
            wlock.release();
        }
    }

    private boolean isIdValid() throws Exception {
        InterProcessMutex rlock = counterLock.readLock();
        acquireLock(rlock, 250, TimeUnit.MILLISECONDS);
        boolean valid = false;
        try {
            byte[] data = curator.getData().forPath(validZkPath);
            if (Arrays.equals(data, "1".getBytes())) {
                valid = true;
            }
        } catch (KeeperException.NoNodeException e) {
            setIdValid(false);
        } finally {
            rlock.release();
        }
        return valid;
    }

    private void acquireLock(InterProcessMutex lockToAcquire, int timeout, TimeUnit unit) throws Exception {
        if (!lockToAcquire.acquire(timeout, unit)) {
            logger.error("Failed to acquire read lock in {} {}", timeout, unit);
            throw new IllegalStateException("Failed to acquire read lock in " + timeout + " " + unit.toString());
        }
    }


}
