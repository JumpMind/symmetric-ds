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
package org.jumpmind.symmetric.fs.track;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;

public class DirectoryChangeTracker {

    protected Node node;
    protected DirectorySpec directorySpec;
    protected NodeDirectorySpecKey nodeDirectorySpecKey;
    protected IDirectorySpecSnapshotPersister directorySnapshotPersister;
    protected DirectorySpecSnapshot lastSnapshot;
    protected DirectorySpecSnapshot changesSinceLastSnapshot;
    protected FileAlterationMonitor fileMonitor;
    protected FileAlterationObserver fileObserver;
    protected DirectorySpecSnasphotUpdater currentListener;
    protected long checkInterval = 10000;

    public DirectoryChangeTracker(Node node, DirectorySpec directorySpec,
            IDirectorySpecSnapshotPersister directorySnapshotPersister, long checkInterval) {
        this.node = node;
        this.directorySpec = directorySpec;
        this.nodeDirectorySpecKey = new NodeDirectorySpecKey(node, directorySpec);
        this.directorySnapshotPersister = directorySnapshotPersister;
        this.checkInterval = checkInterval;
    }

    public void start() {
        changesSinceLastSnapshot = new DirectorySpecSnapshot(node, directorySpec);
        startWatcher();
        lastSnapshot = directorySnapshotPersister.get(DirectorySpecSnapshot.class,
                nodeDirectorySpecKey);
        if (lastSnapshot == null) {
            lastSnapshot = changesSinceLastSnapshot;
            takeFullSnapshot(lastSnapshot);
        } else {
            DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node, directorySpec);
            takeFullSnapshot(snapshot);
            changesSinceLastSnapshot.merge(lastSnapshot.diff(snapshot));
        }
    }

    public void stop() {
        if (fileMonitor != null) {
            try {
                fileMonitor.stop();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void startWatcher() {
        try {
            fileMonitor = new FileAlterationMonitor(checkInterval);
            fileObserver = new FileAlterationObserver(directorySpec.getDirectory(),
                    directorySpec.createIOFileFilter());
            currentListener = new DirectorySpecSnasphotUpdater(changesSinceLastSnapshot, false);
            fileObserver.addListener(currentListener);
            fileMonitor.addObserver(fileObserver);
            if (checkInterval > 0) {
                fileMonitor.start();
            } else {
                fileObserver.initialize();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void pollForChanges() {
        if (checkInterval <= 0 && fileObserver != null) {
            fileObserver.checkAndNotify();
        }
    }

    synchronized public DirectorySpecSnapshot takeSnapshot() {
        DirectorySpecSnapshot changes = changesSinceLastSnapshot;
        lastSnapshot.merge(changesSinceLastSnapshot);
        changesSinceLastSnapshot = new DirectorySpecSnapshot(node, directorySpec);
        DirectorySpecSnasphotUpdater newListener = new DirectorySpecSnasphotUpdater(
                changesSinceLastSnapshot, false);
        fileObserver.addListener(newListener);
        fileObserver.removeListener(currentListener);
        currentListener = newListener;
        directorySnapshotPersister.save(lastSnapshot, nodeDirectorySpecKey);
        return changes;
    }

    synchronized protected void takeFullSnapshot(DirectorySpecSnapshot snapshot) {
        // update the snapshot with every file in the directory spec
        FileAlterationObserver observer = new FileAlterationObserver(directorySpec.getDirectory(),
                directorySpec.createIOFileFilter());
        observer.addListener(new DirectorySpecSnasphotUpdater(snapshot, true));
        observer.checkAndNotify();
    }

    class DirectorySpecSnasphotUpdater extends FileAlterationListenerAdaptor {

        DirectorySpecSnapshot snapshot;
        boolean populateAll = false;

        DirectorySpecSnasphotUpdater(DirectorySpecSnapshot snapshot, boolean populateAll) {
            this.snapshot = snapshot;
            this.populateAll = populateAll;
        }

        public void onFileDelete(File file) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureDeletes()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(file, FileChangeType.DELETE));
                }
            }
        }

        public void onFileCreate(File file) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureCreates()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(file, FileChangeType.CREATE));
                }
            }
        }

        public void onFileChange(File file) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureUpdates()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(file, FileChangeType.UPDATE));
                }
            }
        }

        public void onDirectoryDelete(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureDeletes()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(directory, FileChangeType.DELETE));
                }
            }
        }

        public void onDirectoryCreate(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureCreates()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(directory, FileChangeType.CREATE));
                }
            }
        }

        public void onDirectoryChange(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureUpdates()) {
                synchronized (DirectoryChangeTracker.this) {
                    this.snapshot.addFileChange(new FileChange(directory, FileChangeType.UPDATE));
                }
            }
        }

    }

}
