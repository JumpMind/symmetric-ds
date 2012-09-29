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

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.fs.SyncParameterConstants;
import org.jumpmind.symmetric.fs.client.connector.TransportConnectorFactory;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.ScriptAPI;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.config.SyncConfigCollection;
import org.jumpmind.symmetric.fs.service.IPersisterServices;
import org.jumpmind.symmetric.fs.service.filesystem.FileSystemPersisterServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class SyncClientEngine {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IPersisterServices persisterServices;
    protected SyncConfigCollection config;
    protected IServerNodeLocker serverNodeLocker;
    protected ThreadPoolTaskScheduler taskScheduler;
    protected List<SyncJob> syncJobs;
    protected ScriptAPI scriptApi;
    protected ISyncClientListener syncClientListener;
    protected TransportConnectorFactory transportConnectorFactory;

    public SyncClientEngine() {
        syncJobs = new ArrayList<SyncJob>();
        scriptApi = new ScriptAPI();
    }

    protected void init() {
        persisterServices = createPersisterServices();
        serverNodeLocker = createServerNodeLocker();
        transportConnectorFactory = createTransportConnectorFactory(persisterServices);
        config = persisterServices.getSyncConfigCollectionPersister().get();
        if (config == null) {
            config = new SyncConfigCollection();
            persisterServices.getSyncConfigCollectionPersister().save(config);
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

    public void setScriptApi(ScriptAPI api) {
        this.scriptApi = api;
    }

    public ScriptAPI getScriptApi() {
        return scriptApi;
    }

    public void setSyncClientListener(ISyncClientListener clientListener) {
        this.syncClientListener = clientListener;
    }

    public ISyncClientListener getSyncClientListener() {
        return syncClientListener;
    }

    protected SyncJob addSyncJob(Node node, SyncConfig syncConfig) {
        SyncJob job = new SyncJob(transportConnectorFactory, persisterServices, serverNodeLocker,
                taskScheduler, node, syncConfig, this.config.getProperties(), syncClientListener,
                scriptApi);
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

    protected TransportConnectorFactory createTransportConnectorFactory(IPersisterServices persisterServices) {
        return new TransportConnectorFactory(persisterServices);
    }

    protected IPersisterServices createPersisterServices() {
        return new FileSystemPersisterServices(getStatusDirectory(), getConfigDirectory());
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
