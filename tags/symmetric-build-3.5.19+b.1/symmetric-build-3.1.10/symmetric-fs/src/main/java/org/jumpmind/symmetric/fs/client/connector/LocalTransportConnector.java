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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.fs.client.SyncStatus;
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
                    persisterSerivces.getDirectorySpecSnapshotPersister());
            serverDirectoryChangeTracker.start();
        }
    }

    public void prepare(SyncStatus syncStatus) {
        SyncConfig syncConfig = syncStatus.getSyncConfig();
        SyncDirection direction = syncConfig.getSyncDirection();
        if (direction == SyncDirection.SERVER_TO_CLIENT || direction == SyncDirection.BIDIRECTIONAL) {
            syncStatus.setServerSnapshot(serverDirectoryChangeTracker.takeSnapshot());
        }

    }

    public void send(SyncStatus syncStatus) {
        List<String> toSend = syncStatus.getFilesToSend();
        List<String> sent = syncStatus.getFileSent();
        List<String> toDelete = syncStatus.getDeletesToSend();
        List<String> deleted = syncStatus.getDeletesSent();
        move(syncStatus.getSyncConfig().getClientDir(), syncStatus.getSyncConfig().getServerDir(),
                toSend, sent, toDelete, deleted);
        List<String> filesProcesssed = new ArrayList<String>(sent.size() + deleted.size());
        filesProcesssed.addAll(sent);
        filesProcesssed.addAll(deleted);
        serverDirectoryChangeTracker.pollForChanges();
        serverDirectoryChangeTracker.removeAndMergeChanges(filesProcesssed);
    }

    public void receive(SyncStatus syncStatus, DirectoryChangeTracker clientDirectoryChangeTracker) {
        List<String> toSend = syncStatus.getFilesToReceive();
        List<String> sent = syncStatus.getFilesReceived();
        List<String> toDelete = syncStatus.getDeletesToReceive();
        List<String> deleted = syncStatus.getDeletesReceived();
        move(syncStatus.getSyncConfig().getServerDir(), syncStatus.getSyncConfig().getClientDir(),
                toSend, sent, toDelete, deleted);
        List<String> filesProcesssed = new ArrayList<String>(sent.size() + deleted.size());
        filesProcesssed.addAll(sent);
        filesProcesssed.addAll(deleted);
        clientDirectoryChangeTracker.pollForChanges();
        clientDirectoryChangeTracker.removeAndMergeChanges(filesProcesssed);
        
    }

    protected void move(String sourceDir, String targetDir, List<String> toSend, List<String> sent,
            List<String> toDelete, List<String> deleted) {
        try {
            try {
                for (String fileName : toSend) {
                    try {
                        FileUtils.copyFile(new File(sourceDir, fileName), new File(targetDir,
                                fileName));
                        sent.add(fileName);
                    } catch (FileNotFoundException ex) {
                        log.info(ex.getMessage());
                    }
                }
            } finally {
                toSend.removeAll(sent);
            }

            try {
                for (String fileName : toDelete) {
                    FileUtils.deleteQuietly(new File(targetDir, fileName));
                    deleted.add(fileName);
                }
            } finally {
                toDelete.removeAll(deleted);
            }
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

}
