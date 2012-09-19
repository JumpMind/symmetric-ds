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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.fs.client.SyncStatus.Stage;
import org.jumpmind.symmetric.fs.config.Config;
import org.jumpmind.symmetric.fs.config.GroupConfig;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.ScriptIdentifier;
import org.jumpmind.symmetric.fs.track.DirectoryChangeTracker;
import org.jumpmind.symmetric.fs.track.IDirectorySpecSnapshotPersister;
import org.jumpmind.util.Context;

public class SyncClient {

    protected Config config;
    protected ISyncStatusPersister syncStatusPersister;
    protected IDirectorySpecSnapshotPersister directorySnapshotPersister;
    protected IServerNodeLocker serverNodeLocker;
    protected Map<String, DirectoryChangeTracker> changeTrackerByNodeId;
    protected long checkInterval = 10000;

    public SyncClient(Config config, ISyncStatusPersister syncStatusPersister,
            IDirectorySpecSnapshotPersister directorySnapshotPersister,
            IServerNodeLocker serverNodeLocker) {
        this.config = config;
        this.syncStatusPersister = syncStatusPersister;
        this.directorySnapshotPersister = directorySnapshotPersister;
        this.serverNodeLocker = serverNodeLocker;
        this.changeTrackerByNodeId = new HashMap<String, DirectoryChangeTracker>();
    }

    public void sync(Context context, ISyncClientListener listener) {
        List<Node> nodes = config.getServerNodes();
        for (Node node : nodes) {
            // TODO insert per node thread pool here
            sync(node, context, listener);
        }
    }

    public void sync(Node serverNode, Context context, ISyncClientListener listener) {
        if (serverNodeLocker.lock(serverNode)) {
            try {
                if (isServerAvailable(serverNode)) {
                    SyncStatus syncStatus = syncStatusPersister.get(serverNode);
                    if (syncStatus == null) {
                        syncStatus = new SyncStatus(serverNode);
                        syncStatusPersister.save(syncStatus);
                    }

                    while (syncStatus.getStage() != Stage.DONE) {

                        switch (syncStatus.getStage()) {
                            case START:
                                runScript(ScriptIdentifier.PRECLIENT, syncStatus, context);
                                syncStatus.setStage(Stage.RAN_PRESCRIPT);
                                syncStatusPersister.save(syncStatus);
                                break;
                            case RAN_PRESCRIPT:
                                DirectoryChangeTracker changeTracker = getDirectoryChangeTracker(serverNode);
                                syncStatus.setSnapshot(changeTracker.takeSnapshot());
                                syncStatus.setStage(Stage.RECORDED_FILES_TO_SEND);
                                syncStatusPersister.save(syncStatus);
                                break;
                            case RECORDED_FILES_TO_SEND:
                                updateFilesToSendAndReceiveFromServer(syncStatus);
                                syncStatus.setStage(Stage.SEND_FILES);
                                syncStatusPersister.save(syncStatus);
                                break;
                            case SEND_FILES:
                                sendFiles(syncStatus);
                                syncStatus.setStage(Stage.RECEIVE_FILES);
                                syncStatusPersister.save(syncStatus);
                                break;
                            case RECEIVE_FILES:
                                receiveFiles(syncStatus);
                                syncStatus.setStage(Stage.RUN_POSTSCRIPT);
                                syncStatusPersister.save(syncStatus);
                                break;
                            case RUN_POSTSCRIPT:
                                runScript(ScriptIdentifier.POSTCLIENT, syncStatus, context);
                                syncStatus.setStage(Stage.DONE);
                                syncStatusPersister.save(syncStatus);
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

    protected DirectoryChangeTracker getDirectoryChangeTracker(Node serverNode) {
        String nodeId = serverNode.getNodeId();
        DirectoryChangeTracker changeTracker = changeTrackerByNodeId.get(nodeId);
        if (changeTracker == null) {
            GroupConfig groupConfig = config.getGroupConfig(serverNode.getGroupId());
            changeTracker = new DirectoryChangeTracker(nodeId,
                    groupConfig.getClientDirectorySpec(), directorySnapshotPersister, checkInterval);
            changeTracker.start();
            changeTrackerByNodeId.put(nodeId, changeTracker);
        }
        return changeTracker;
    }

}
