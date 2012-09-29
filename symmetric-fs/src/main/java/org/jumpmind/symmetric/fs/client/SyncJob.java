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

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.fs.SyncParameterConstants;
import org.jumpmind.symmetric.fs.client.SyncStatus.Stage;
import org.jumpmind.symmetric.fs.client.connector.ConnectorException;
import org.jumpmind.symmetric.fs.client.connector.ITransportConnector;
import org.jumpmind.symmetric.fs.client.connector.TransportConnectorFactory;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.NodeDirectorySpecKey;
import org.jumpmind.symmetric.fs.config.ScriptAPI;
import org.jumpmind.symmetric.fs.config.ScriptIdentifier;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.service.IPersisterServices;
import org.jumpmind.symmetric.fs.track.DirectoryChangeTracker;
import org.jumpmind.util.RandomTimeSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

public class SyncJob implements Runnable {

    final Logger log = LoggerFactory.getLogger(getClass());

    protected IPersisterServices persisterServices;
    protected IServerNodeLocker serverNodeLocker;
    protected TaskScheduler taskScheduler;
    protected Node serverNode;
    protected SyncConfig config;
    protected DirectoryChangeTracker directoryChangeTracker;
    protected TransportConnectorFactory transportConnectorFactory;
    protected TypedProperties properties;
    protected NodeDirectorySpecKey key;
    protected RandomTimeSlot randomTimeSlot;
    protected ISyncClientListener syncClientListener;
    protected ScriptAPI scriptApi;

    private boolean paused = false;

    private Date lastFinishTime;

    private boolean running = false;

    private long lastExecutionTimeInMs;

    private long totalExecutionTimeInMs;

    private long numberOfRuns;

    private boolean started;

    private ScheduledFuture<?> scheduledJob;

    public SyncJob(TransportConnectorFactory transportConnectorFactory,
            IPersisterServices persisterServices, IServerNodeLocker serverNodeLocker,
            TaskScheduler taskScheduler, Node node, SyncConfig config, TypedProperties properties,
            ISyncClientListener syncClientListener, ScriptAPI api) {
        this.transportConnectorFactory = transportConnectorFactory;
        this.persisterServices = persisterServices;
        this.serverNodeLocker = serverNodeLocker;
        this.syncClientListener = syncClientListener;
        this.taskScheduler = taskScheduler;
        this.serverNode = node;
        this.config = config;
        this.properties = properties;
        this.key = new NodeDirectorySpecKey(node, config.getDirectorySpec());
        this.randomTimeSlot = new RandomTimeSlot(serverNode.getNodeId(),
                properties.getInt(SyncParameterConstants.JOB_RANDOM_MAX_START_TIME_MS));
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean isRunning() {
        return running;
    }

    public long getAverageExecutionTimeInMs() {
        if (numberOfRuns > 0) {
            return totalExecutionTimeInMs / numberOfRuns;
        } else {
            return 0;
        }
    }

    public long getLastExecutionTimeInMs() {
        return lastExecutionTimeInMs;
    }

    public Date getLastFinishTime() {
        return lastFinishTime;
    }

    public long getTotalExecutionTimeInMs() {
        return totalExecutionTimeInMs;
    }

    public long getNumberOfRuns() {
        return numberOfRuns;
    }

    public void pause() {
        paused = true;
    }

    public void unpause() {
        paused = false;
    }

    public void start() {
        if (this.scheduledJob == null) {
            if (!StringUtils.isBlank(config.getFrequency())) {
                String frequency = config.getFrequency();
                try {
                    long timeBetweenRunsInMs = Long.parseLong(frequency);
                    int startDelay = randomTimeSlot.getRandomValueSeededByExternalId();
                    if (timeBetweenRunsInMs > 0) {
                        this.scheduledJob = taskScheduler.scheduleWithFixedDelay(this, new Date(
                                System.currentTimeMillis() + startDelay), timeBetweenRunsInMs);
                        log.info("Started {} for node {} on periodic schedule: every {}ms",
                                new Object[] { config.getConfigId(), serverNode.getNodeId(),
                                        timeBetweenRunsInMs });
                        started = true;
                    } else {
                        log.error("Failed to schedule the {} job for node {}",
                                config.getConfigId(), serverNode.getNodeId());
                    }

                } catch (NumberFormatException ex) {
                    this.scheduledJob = taskScheduler.schedule(this, new CronTrigger(frequency));
                    log.info("Started {} for node {} with cron expression: {}", new Object[] {
                            config.getConfigId(), serverNode.getNodeId(), frequency });
                    started = true;
                }
            }
        }
    }

    public boolean stop() {
        boolean success = false;
        if (this.scheduledJob != null) {
            success = this.scheduledJob.cancel(true);
            this.scheduledJob = null;
            if (success) {
                log.info("{} for node {} has been cancelled.", config.getConfigId(),
                        serverNode.getNodeId());
                started = false;
            } else {
                log.warn("Failed to cancel this {} for node {}", config.getConfigId(),
                        serverNode.getNodeId());
            }
        }
        return success;
    }

    protected String getEngineName() {
        return properties.getProperty(SyncParameterConstants.ENGINE_NAME);
    }

    /*
     * This method is called from the job
     */
    public void run() {
        invoke(false);
    }

    public boolean invoke() {
        return invoke(true);
    }

    public boolean invoke(boolean force) {
        boolean ran = false;
        try {
            if (!Thread.interrupted()) {
                MDC.put("engineName", getEngineName());
                if (!paused || force) {
                    if (!running) {
                        running = true;
                        synchronized (this) {
                            ran = true;
                            long startTime = System.currentTimeMillis();
                            try {
                                doSync();
                            } finally {
                                lastFinishTime = new Date();
                                long endTime = System.currentTimeMillis();
                                lastExecutionTimeInMs = endTime - startTime;
                                totalExecutionTimeInMs += lastExecutionTimeInMs;
                                numberOfRuns++;
                                running = false;
                            }
                        }
                    }
                }
            } else {
                log.warn("This thread was interrupted.  Not executing the job until the interrupted status has cleared");
            }
        } catch (final Throwable ex) {
            log.error(ex.getMessage(), ex);
        }

        return ran;
    }

    protected void doSync() {
        if (serverNodeLocker.lock(serverNode)) {
            ITransportConnector connector = null;
            try {
                connector = transportConnectorFactory.createTransportConnector(config, serverNode);
                connector.connect();
                SyncStatus syncStatus = persisterServices.getSyncStatusPersister().get(
                        SyncStatus.class, key);
                if (syncStatus == null) {
                    syncStatus = new SyncStatus(serverNode);
                    persisterServices.getSyncStatusPersister().save(syncStatus, key);
                }

                initDirectoryChangeTracker();

                while (syncStatus.getStage() != Stage.DONE) {

                    switch (syncStatus.getStage()) {
                        case START:
                            runScript(ScriptIdentifier.PRECLIENT, syncStatus);
                            syncStatus.setStage(Stage.RAN_PRESCRIPT);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case RAN_PRESCRIPT:
                            syncStatus.setSnapshot(directoryChangeTracker.takeSnapshot());
                            syncStatus.setStage(Stage.RECORDED_FILES_TO_SEND);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case RECORDED_FILES_TO_SEND:
                            connector.prepare(syncStatus);
                            syncStatus.setStage(Stage.SEND_FILES);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case SEND_FILES:
                            connector.send(syncStatus);
                            syncStatus.setStage(Stage.RECEIVE_FILES);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case RECEIVE_FILES:
                            connector.receive(syncStatus);
                            syncStatus.setStage(Stage.RUN_POSTSCRIPT);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case RUN_POSTSCRIPT:
                            runScript(ScriptIdentifier.POSTCLIENT, syncStatus);
                            syncStatus.setStage(Stage.DONE);
                            persisterServices.getSyncStatusPersister().save(syncStatus, key);
                            break;
                        case DONE:
                            break;
                    }
                }
            } catch (ConnectorException ex) {
                log.warn("Connection issue: {}", ex.getMessage());
            } finally {
                serverNodeLocker.unlock(serverNode);

                if (connector != null) {
                    connector.close();
                }
            }
        }
    }

    protected boolean runScript(ScriptIdentifier identifier, SyncStatus syncStatus) {
        // TODO runScript
        return true;
    }

    protected void initDirectoryChangeTracker() {
        if (directoryChangeTracker == null) {
            long checkInterval = properties.getLong(
                    SyncParameterConstants.DIRECTORY_TRACKER_POLL_FOR_CHANGE_INTERVAL, 10000);
            directoryChangeTracker = new DirectoryChangeTracker(serverNode,
                    config.getDirectorySpec(),
                    persisterServices.getDirectorySpecSnapshotPersister(), checkInterval);
            directoryChangeTracker.start();
        }
    }

}
