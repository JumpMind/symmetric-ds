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
import org.jumpmind.symmetric.fs.config.ScriptAPI;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.NodeDirectorySpecKey;
import org.jumpmind.symmetric.fs.config.ScriptIdentifier;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.track.DirectoryChangeTracker;
import org.jumpmind.symmetric.fs.track.IDirectorySpecSnapshotPersister;
import org.jumpmind.util.Context;
import org.jumpmind.util.RandomTimeSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

public class SyncJob implements Runnable {

    final Logger log = LoggerFactory.getLogger(getClass());

    protected ISyncStatusPersister syncStatusPersister;
    protected IDirectorySpecSnapshotPersister directorySnapshotPersister;
    protected IServerNodeLocker serverNodeLocker;
    protected TaskScheduler taskScheduler;
    protected Node serverNode;
    protected SyncConfig config;
    protected DirectoryChangeTracker directoryChangeTracker;
    protected TypedProperties properties;
    protected NodeDirectorySpecKey key;
    protected RandomTimeSlot randomTimeSlot;
    protected ISyncClientListener syncClientListener;
    protected ScriptAPI api;

    private boolean paused = false;

    private Date lastFinishTime;

    private boolean running = false;

    private long lastExecutionTimeInMs;

    private long totalExecutionTimeInMs;

    private long numberOfRuns;

    private boolean started;

    private ScheduledFuture<?> scheduledJob;

    public SyncJob(ISyncStatusPersister syncStatusPersister,
            IDirectorySpecSnapshotPersister directorySnapshotPersister,
            IServerNodeLocker serverNodeLocker, TaskScheduler taskScheduler, Node node,
            SyncConfig config, TypedProperties properties, ISyncClientListener syncClientListener,
            ScriptAPI api) {
        this.syncStatusPersister = syncStatusPersister;
        this.directorySnapshotPersister = directorySnapshotPersister;
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

    public void stop() {

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
            try {
                if (isServerAvailable(serverNode)) {
                    SyncStatus syncStatus = syncStatusPersister.get(SyncStatus.class, key);
                    if (syncStatus == null) {
                        syncStatus = new SyncStatus(serverNode);
                        syncStatusPersister.save(syncStatus, key);
                    }

                    initDirectoryChangeTracker();

                    while (syncStatus.getStage() != Stage.DONE) {

                        switch (syncStatus.getStage()) {
                            case START:
                                runScript(ScriptIdentifier.PRECLIENT, syncStatus, api);
                                syncStatus.setStage(Stage.RAN_PRESCRIPT);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case RAN_PRESCRIPT:
                                syncStatus.setSnapshot(directoryChangeTracker.takeSnapshot());
                                syncStatus.setStage(Stage.RECORDED_FILES_TO_SEND);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case RECORDED_FILES_TO_SEND:
                                updateFilesToSendAndReceiveFromServer(syncStatus);
                                syncStatus.setStage(Stage.SEND_FILES);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case SEND_FILES:
                                sendFiles(syncStatus);
                                syncStatus.setStage(Stage.RECEIVE_FILES);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case RECEIVE_FILES:
                                receiveFiles(syncStatus);
                                syncStatus.setStage(Stage.RUN_POSTSCRIPT);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case RUN_POSTSCRIPT:
                                runScript(ScriptIdentifier.POSTCLIENT, syncStatus, api);
                                syncStatus.setStage(Stage.DONE);
                                syncStatusPersister.save(syncStatus, key);
                                break;
                            case DONE:
                                break;
                        }
                    }

                }
            } finally {
                serverNodeLocker.unlock(serverNode);
            }
        }
    }

    protected void sendFiles(SyncStatus syncStatus) {
        // TODO loop through and send files that have not yet been sent
    }

    protected void receiveFiles(SyncStatus syncStatus) {
        // TODO loop through and receive files that have not yet been sent
    }

    protected void updateFilesToSendAndReceiveFromServer(SyncStatus syncStatus) {
        // TODO contact server and update files to send and receive
    }

    protected boolean runScript(ScriptIdentifier identifier, SyncStatus syncStatus, Context context) {
        // TODO runScript
        return true;
    }

    protected boolean isServerAvailable(Node serverNode) {
        // TODO
        return true;
    }

    protected void initDirectoryChangeTracker() {
        if (directoryChangeTracker == null) {
            long checkInterval = properties.getLong(
                    SyncParameterConstants.DIRECTORY_TRACKER_POLL_FOR_CHANGE_INTERVAL, 10000);
            directoryChangeTracker = new DirectoryChangeTracker(serverNode,
                    config.getDirectorySpec(), directorySnapshotPersister, checkInterval);
            directoryChangeTracker.start();
        }
    }

}
