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

package ezbake.services.provenance.idgenerator;

import ezbake.services.provenance.graph.GraphDb;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ZookeeperIdProvider implements IdProvider {
    private final CuratorFramework curator;

    private DistributedAtomicLong document;
    private DistributedAtomicLong rule;
    private DistributedAtomicLong purge;

    public final String documentZkPath;
    public final String documentLockZkPath;

    public final String ruleZkPath;
    public final String ruleLockZkPath;

    public final String purgeZkPath;
    public final String purgeLockZkPath;

    public ZookeeperIdProvider(final Properties properties) {
        String indexName = GraphDb.getElasticIndexName(properties);

        documentZkPath = String.format("/ezbake/%s/document/counter", indexName);
        documentLockZkPath = String.format("/ezbake/%s/document/lock", indexName);
        ruleZkPath = String.format("/ezbake/%s/rule/counter", indexName);
        ruleLockZkPath =  String.format("/ezbake/%s/rule/lock", indexName);
        purgeZkPath = String.format("/ezbake/%s/purge/counter", indexName);
        purgeLockZkPath = String.format("/ezbake/%s/purge/lock", indexName);

        ZookeeperConfigurationHelper zc = new ZookeeperConfigurationHelper(properties);
        curator = CuratorFrameworkFactory.builder()
                .connectString(zc.getZookeeperConnectionString())
                .retryPolicy(new RetryNTimes(5, 1000))
                .build();
        if (curator.getState() == CuratorFrameworkState.LATENT) {
            curator.start();
        }
        document = new DistributedAtomicLong(curator, documentZkPath, curator.getZookeeperClient().getRetryPolicy(),
                PromotedToLock.builder()
                        .lockPath(documentLockZkPath)
                        .timeout(250, TimeUnit.MILLISECONDS)
                        .build());
        rule = new DistributedAtomicLong(curator, ruleZkPath, curator.getZookeeperClient().getRetryPolicy(),
                PromotedToLock.builder()
                        .lockPath(ruleLockZkPath)
                        .timeout(250, TimeUnit.MILLISECONDS)
                        .build());
        purge = new DistributedAtomicLong(curator, purgeZkPath, curator.getZookeeperClient().getRetryPolicy(),
                PromotedToLock.builder()
                        .lockPath(purgeLockZkPath)
                        .timeout(250, TimeUnit.MILLISECONDS)
                        .build());
    }

    @Override
    public void shutdown() {
        if (curator != null) {
            curator.close();
        }
    }

    @Override
    public long getNextId(ID_GENERATOR_TYPE type) throws IdGeneratorException {
        DistributedAtomicLong atomic = null;
        switch (type) {
            case DocumentType:
                atomic = document;
                break;
            case AgeOffRule:
                atomic = rule;
                break;
            case PurgeEvent:
                atomic = purge;
                break;
            default:
                throw new IdGeneratorException("Not supported ID Generator Type: " + type);
        }

        try {
            AtomicValue<Long> current = atomic.increment();
            if (!current.succeeded()) {
                throw new IdGeneratorException("Failed to increment id in zookeeper for type " + type);
            }
            return current.postValue();

        }catch (Exception ex) {
            throw new IdGeneratorException(ex);
        }
    }

    @Override
    public long getNextNId(ID_GENERATOR_TYPE type, long delta) throws IdGeneratorException {
        DistributedAtomicLong atomic = null;
        switch (type) {
            case DocumentType:
                atomic = document;
                break;
            case AgeOffRule:
                atomic = rule;
                break;
            case PurgeEvent:
                atomic = purge;
                break;
            default:
                throw new IdGeneratorException("Not supported ID Generator Type: " + type);
        }

        try {
            AtomicValue<Long> current = atomic.add(delta);
            if (!current.succeeded()) {
                throw new IdGeneratorException(String.format("Failed to increment id in zookeeper for type %s by %d", type, delta));
            }
            return current.postValue();
        }catch (Exception ex) {
            throw new IdGeneratorException(ex);
        }
    }

    @Override
    public long getCurrentValue(ID_GENERATOR_TYPE type) throws IdGeneratorException {
        DistributedAtomicLong atomic = null;
        switch (type) {
            case DocumentType:
                atomic = document;
                break;
            case AgeOffRule:
                atomic = rule;
                break;
            case PurgeEvent:
                atomic = purge;
                break;
            default:
                throw new IdGeneratorException("Not supported ID Generator Type: " + type);
        }

        try {
            AtomicValue<Long> current = atomic.get();
            if (!current.succeeded()) {
                throw new IdGeneratorException("Failed to get id in zookeeper for type " + type);
            }
            return current.postValue();
        }catch (Exception ex) {
            throw new IdGeneratorException(ex);
        }
    }

    @Override
    public void setCurrentValue(ID_GENERATOR_TYPE type, long value) throws IdGeneratorException {

        DistributedAtomicLong atomic = null;
        switch (type) {
            case DocumentType:
                atomic = document;
                break;
            case AgeOffRule:
                atomic = rule;
                break;
            case PurgeEvent:
                atomic = purge;
                break;
            default:
                throw new IdGeneratorException("Not supported ID Generator Type: " + type);
        }

        try {
            AtomicValue<Long> current = atomic.trySet(value);
            if (!current.succeeded()) {
                throw new IdGeneratorException("Failed to set current id for type " + type);
            }
        }catch (Exception ex) {
            throw new IdGeneratorException(ex);
        }
    }
}
