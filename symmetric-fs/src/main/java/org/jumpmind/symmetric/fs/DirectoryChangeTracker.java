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
package org.jumpmind.symmetric.fs;

public class DirectoryChangeTracker {

    protected String nodeId;
    protected DirectorySpec directorySpec;
    protected IDirectorySnapshotPersister directorySnapshotPersister;
    protected DirectorySpecSnapshot lastSnapshot;
    protected DirectorySpecSnapshot changesSinceLastSnapshot;

    public DirectoryChangeTracker(String nodeId, DirectorySpec directorySpec,
            IDirectorySnapshotPersister directorySnapshotPersister) {
        this.nodeId = nodeId;
        this.directorySpec = directorySpec;
        this.directorySnapshotPersister = directorySnapshotPersister;
    }
    
    protected void init() {
        changesSinceLastSnapshot = new DirectorySpecSnapshot(nodeId, directorySpec);
        startWatcher();
        lastSnapshot = directorySnapshotPersister.get(nodeId, directorySpec);
        if (lastSnapshot == null) {
            lastSnapshot = changesSinceLastSnapshot;
            takeFullSnapshot(lastSnapshot);
        }
    }
    
    protected void startWatcher() {
        
    }
    
    synchronized protected DirectorySpecSnapshot takeSnapshot() {
        DirectorySpecSnapshot changes = changesSinceLastSnapshot;
        lastSnapshot.merge(changesSinceLastSnapshot);
        changesSinceLastSnapshot = new DirectorySpecSnapshot(nodeId, directorySpec);
        directorySnapshotPersister.save(lastSnapshot);        
        return changes;
    }
    
    protected void takeFullSnapshot(DirectorySpecSnapshot snapshot) {
        // update the snapshot with every file in the directory spec
    }

}
