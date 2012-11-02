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

import org.jumpmind.symmetric.fs.config.ConflictStrategy;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;
import org.jumpmind.symmetric.fs.track.FileChange;
import org.jumpmind.symmetric.fs.track.FileChangeType;

public class SyncStatus {

    public enum Stage {
        START, RAN_PRESCRIPT, RECORDED_FILES_TO_SEND, SEND_FILES, RECEIVE_FILES, RUN_POSTSCRIPT, DONE
    };

    protected Node node;
    protected Stage stage = Stage.START;
    protected SyncConfig syncConfig;
    protected DirectorySpecSnapshot clientSnapshot;
    protected DirectorySpecSnapshot serverSnapshot;
    protected List<String> deletesToSend;
    protected List<String> deletesSent;
    protected List<String> filesToSend;
    protected List<String> fileSent;
    protected List<String> filesToReceive;
    protected List<String> deletesToReceive;
    protected List<String> deletesReceived;
    protected List<String> filesReceived;
    protected List<String> filesInConflict;

    public SyncStatus() {
        deletesToSend = new ArrayList<String>();
        deletesSent = new ArrayList<String>();

        filesToSend = new ArrayList<String>();
        fileSent = new ArrayList<String>();

        deletesToReceive = new ArrayList<String>();
        deletesReceived = new ArrayList<String>();

        filesToReceive = new ArrayList<String>();
        filesReceived = new ArrayList<String>();

        filesInConflict = new ArrayList<String>();
    }

    public SyncStatus(Node node, SyncConfig syncConfig) {
        this();
        this.node = node;
        this.syncConfig = syncConfig;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Stage getStage() {
        return stage;
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public DirectorySpecSnapshot getClientSnapshot() {
        return clientSnapshot;
    }

    public void setClientSnapshot(DirectorySpecSnapshot clientSnapshot) {
        this.clientSnapshot = clientSnapshot;
        List<FileChange> changes = clientSnapshot.getFiles();
        for (FileChange fileChange : changes) {
            String fileName = fileChange.getFileName();
            if (fileChange.getFileChangeType() == FileChangeType.DELETE) {
                filesToSend.remove(fileName);
                deletesToSend.add(fileName);
            } else {
                deletesToSend.remove(fileName);
                filesToSend.add(fileName);
            }
        }
    }

    public void setServerSnapshot(DirectorySpecSnapshot serverSnapshot) {
        this.serverSnapshot = serverSnapshot;
        List<FileChange> changes = serverSnapshot.getFiles();
        for (FileChange fileChange : changes) {
            String fileName = fileChange.getFileName();
            if (fileChange.getFileChangeType() == FileChangeType.DELETE) {
                filesToReceive.remove(fileName);
                deletesToReceive.add(fileName);
            } else {
                deletesToReceive.remove(fileName);
                if (!filesToSend.contains(fileName)) {
                    filesToReceive.add(fileName);
                } else {
                    ConflictStrategy conflictStrategy = syncConfig.getConflictStrategy();
                    switch (conflictStrategy) {
                        case CLIENT_WINS:
                            // do nothing
                            break;
                        case REPORT_ERROR:
                            filesInConflict.add(fileName);
                            filesToSend.remove(fileName);
                            break;
                        case SERVER_WINS:
                            filesToReceive.add(fileName);
                            filesToSend.remove(fileName);
                            break;
                        default:
                            break;
                    }
                }
            }

        }
    }

    public List<String> getFilesInConflict() {
        return filesInConflict;
    }

    public List<String> getFilesToSend() {
        return filesToSend;
    }

    public List<String> getFileSent() {
        return fileSent;
    }

    public List<String> getFilesToReceive() {
        return filesToReceive;
    }

    public List<String> getFilesReceived() {
        return filesReceived;
    }

    public List<String> getDeletesReceived() {
        return deletesReceived;
    }

    public List<String> getDeletesSent() {
        return deletesSent;
    }

    public List<String> getDeletesToReceive() {
        return deletesToReceive;
    }

    public List<String> getDeletesToSend() {
        return deletesToSend;
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    public void setSyncConfig(SyncConfig syncConfig) {
        this.syncConfig = syncConfig;
    }

    public DirectorySpecSnapshot getServerSnapshot() {
        return serverSnapshot;
    }

}
