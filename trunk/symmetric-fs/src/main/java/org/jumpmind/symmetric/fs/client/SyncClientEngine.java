/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.fs.client;

import java.util.List;

import org.jumpmind.symmetric.fs.SyncParameterConstants;
import org.jumpmind.symmetric.fs.config.FileSystemSyncConfigCollectionPersister;
import org.jumpmind.symmetric.fs.config.ISyncConfigCollectionPersister;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.config.SyncConfigCollection;
import org.jumpmind.symmetric.fs.track.FileSystemDirectorySpecSnapshotPersister;
import org.jumpmind.symmetric.fs.track.IDirectorySpecSnapshotPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class SyncClientEngine {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected SyncConfigCollection config;
    protected ISyncConfigCollectionPersister syncConfigCollectionPersister;
    protected ISyncStatusPersister syncStatusPersister;
    protected IDirectorySpecSnapshotPersister directorySnapshotPersister;
    protected IServerNodeLocker serverNodeLocker;
    protected ThreadPoolTaskScheduler taskScheduler;
    protected List<SyncJob> syncJobs;

    public SyncClientEngine() {
    }

    protected void init() {
        syncConfigCollectionPersister = createSyncConfigCollectionPersister();
        syncStatusPersister = createSyncStatusPersister();
        directorySnapshotPersister = createDirectorySpecSnapshotPersister();
        serverNodeLocker = createServerNodeLocker();
        config = syncConfigCollectionPersister.get();
        if (config == null) {
            config = new SyncConfigCollection();
            syncConfigCollectionPersister.save(config);
        }
        initTaskScheduler();

    }

    protected void initTaskScheduler() {
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix(String.format("%s-fs-client-", config.getProperties()
                .getProperty(SyncParameterConstants.ENGINE_NAME)));
        taskScheduler.setPoolSize(config.getProperties().getInt(
                SyncParameterConstants.CLIENT_WORKER_THREADS_NUMBER, 20));
    }

    protected void startTaskScheduler() {
        taskScheduler.initialize();
    }

    protected void stopTaskScheduler() {
        try {
            if (taskScheduler != null && !taskScheduler.getScheduledExecutor().isShutdown()) {
                taskScheduler.destroy();
            }
        } catch (Exception ex) {
        }
    }

    public void start() {
        startTaskScheduler();
    }

    protected void initJobs() {
        List<SyncConfig> syncConfigs = config.getSyncConfigs();
        for (SyncConfig syncConfig : syncConfigs) {
            List<Node> nodes = config.getServerNodesForGroup(syncConfig.getGroupLink()
                    .getServerGroupId());
            for (Node node : nodes) {
                SyncJob job = addSyncJob(node, syncConfig);
                job.start();
            }
        }
    }

    protected SyncJob addSyncJob(Node node, SyncConfig syncConfig) {
        SyncJob job = new SyncJob(syncStatusPersister, directorySnapshotPersister,
                serverNodeLocker, taskScheduler, node, syncConfig, this.config.getProperties());
        syncJobs.add(job);
        return job;
    }

    public void stop() {
        for (SyncJob job : syncJobs) {
            job.stop();
        }
        stopTaskScheduler();
    }

    protected String getStatusDirectory() {
        return System.getProperty("org.jumpmind.symmetric.fs.status.dir", "../status");
    }

    protected String getConfigDirectory() {
        return System.getProperty("org.jumpmind.symmetric.fs.conf.dir", "../conf");
    }

    protected ISyncStatusPersister createSyncStatusPersister() {
        return new FileSystemSyncStatusPersister(getStatusDirectory());
    }

    protected ISyncConfigCollectionPersister createSyncConfigCollectionPersister() {
        return new FileSystemSyncConfigCollectionPersister(getConfigDirectory());
    }

    protected IDirectorySpecSnapshotPersister createDirectorySpecSnapshotPersister() {
        return new FileSystemDirectorySpecSnapshotPersister(getStatusDirectory());
    }

    protected IServerNodeLocker createServerNodeLocker() {
        return new IServerNodeLocker() {

            public boolean unlock(Node serverNode) {
                return true;
            }

            public boolean lock(Node serverNode) {
                return true;
            }
        };
    }

}
