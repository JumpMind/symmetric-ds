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
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;

public class SyncStatus {

    public enum Stage {
        START, RAN_PRESCRIPT, RECORDED_FILES_TO_SEND, SEND_FILES, RECEIVE_FILES, RUN_POSTSCRIPT, DONE
    };

    protected Node node;
    protected Stage stage = Stage.START;
    protected DirectorySpecSnapshot snapshot;
    protected List<String> filesToSend;
    protected List<String> fileSent;
    protected List<String> filesToReceive;
    protected List<String> filesReceived;
    
    public SyncStatus() {     
    }
    
    public SyncStatus(Node node) {
        this.node = node;
    }
    
    public Node getNode() {
        return node;
    }   

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public Stage getStage() {
        return stage;
    }

    public void setSnapshot(DirectorySpecSnapshot snapshot) {
        this.snapshot = snapshot;
    }

}
