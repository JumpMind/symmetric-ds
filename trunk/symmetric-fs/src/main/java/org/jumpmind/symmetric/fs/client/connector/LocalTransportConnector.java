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
package org.jumpmind.symmetric.fs.client.connector;

import java.io.File;

import org.jumpmind.symmetric.fs.SyncParameterConstants;
import org.jumpmind.symmetric.fs.client.SyncStatus;
import org.jumpmind.symmetric.fs.config.ConflictStrategy;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.config.SyncDirection;
import org.jumpmind.symmetric.fs.track.DirectoryChangeTracker;

public class LocalTransportConnector extends AbstractTransportConnector implements
        ITransportConnector {

    DirectoryChangeTracker serverDirectoryChangeTracker;

    @Override
    public void connect(SyncStatus syncStatus) {
        if (serverDirectoryChangeTracker == null) {
            SyncConfig syncConfig = syncStatus.getSyncConfig();
            File serverDir = new File(syncConfig.getServerDir());
            if (!serverDir.exists()) {
                serverDir.mkdirs();
            }
            serverDirectoryChangeTracker = new DirectoryChangeTracker(
                    node,
                    syncConfig.getServerDir(),
                    syncConfig.getDirectorySpec(),
                    persisterSerivces.getDirectorySpecSnapshotPersister(),
                    properties
                            .getLong(SyncParameterConstants.DIRECTORY_TRACKER_POLL_FOR_CHANGE_INTERVAL));
            serverDirectoryChangeTracker.start();
        }
    }

    public void prepare(SyncStatus syncStatus) {
        SyncConfig syncConfig = syncStatus.getSyncConfig();
        SyncDirection direction = syncConfig.getSyncDirection();
        ConflictStrategy conflictStrategy = syncConfig.getConflictStrategy();
        syncStatus.setServerSnapshot(serverDirectoryChangeTracker.takeSnapshot());

    }

    public void send(SyncStatus syncStatus) {
    }

    public void receive(SyncStatus syncStatus) {
    }

}
