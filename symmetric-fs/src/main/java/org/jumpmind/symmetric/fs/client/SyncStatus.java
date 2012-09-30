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

import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;

public class SyncStatus {

    public enum Stage {
        START, RAN_PRESCRIPT, RECORDED_FILES_TO_SEND, SEND_FILES, RECEIVE_FILES, RUN_POSTSCRIPT, DONE
    };

    protected Node node;
    protected Stage stage = Stage.START;
    protected SyncConfig syncConfig;
    protected DirectorySpecSnapshot directorySpecSnapshot;
    protected List<String> filesToSend;
    protected List<String> fileSent;
    protected List<String> filesToReceive;
    protected List<String> filesReceived;
    
    public SyncStatus() {
        
    }
    
    public SyncStatus(Node node, SyncConfig syncConfig) {
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

    public DirectorySpecSnapshot getDirectorySpecSnapshot() {
        return directorySpecSnapshot;
    }

    public void setDirectorySpecSnapshot(DirectorySpecSnapshot directorySpecSnapshot) {
        this.directorySpecSnapshot = directorySpecSnapshot;
    }

    public List<String> getFilesToSend() {
        return filesToSend;
    }

    public void setFilesToSend(List<String> filesToSend) {
        this.filesToSend = filesToSend;
    }

    public List<String> getFileSent() {
        return fileSent;
    }

    public void setFileSent(List<String> fileSent) {
        this.fileSent = fileSent;
    }

    public List<String> getFilesToReceive() {
        return filesToReceive;
    }

    public void setFilesToReceive(List<String> filesToReceive) {
        this.filesToReceive = filesToReceive;
    }

    public List<String> getFilesReceived() {
        return filesReceived;
    }

    public void setFilesReceived(List<String> filesReceived) {
        this.filesReceived = filesReceived;
    }
    
    public SyncConfig getSyncConfig() {
        return syncConfig;
    }
    
    public void setSyncConfig(SyncConfig syncConfig) {
        this.syncConfig = syncConfig;
    }
    
}
