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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.NodeDirectoryKey;
import org.jumpmind.symmetric.fs.service.IDirectorySpecSnapshotPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryChangeTracker {

    final protected Logger log = LoggerFactory.getLogger(getClass());
    protected Node node;
    protected File directory;
    protected DirectorySpec directorySpec;
    protected NodeDirectoryKey nodeDirectorySpecKey;
    protected IDirectorySpecSnapshotPersister directorySnapshotPersister;
    protected DirectorySpecSnapshot lastSnapshot;
    protected DirectorySpecSnapshot changesSinceLastSnapshot;
    protected FileAlterationObserver fileObserver;
    protected DirectorySpecSnasphotUpdater currentListener;

    public DirectoryChangeTracker(Node node, String directory, DirectorySpec directorySpec,
            IDirectorySpecSnapshotPersister directorySnapshotPersister) {
        this.node = node;
        this.directory = new File(directory);
        this.directorySpec = directorySpec;
        this.nodeDirectorySpecKey = new NodeDirectoryKey(node, directory);
        this.directorySnapshotPersister = directorySnapshotPersister;
    }

    public void start() {
        changesSinceLastSnapshot = new DirectorySpecSnapshot(node, directory.getAbsolutePath(),
                directorySpec);
        fileObserver = new FileAlterationObserver(directory, directorySpec.createIOFileFilter());
        currentListener = new DirectorySpecSnasphotUpdater(changesSinceLastSnapshot, false);
        fileObserver.addListener(currentListener);
        try {
            fileObserver.initialize();

            lastSnapshot = directorySnapshotPersister.get(DirectorySpecSnapshot.class,
                    nodeDirectorySpecKey);
            if (lastSnapshot == null) {
                lastSnapshot = changesSinceLastSnapshot;
                takeFullSnapshot(lastSnapshot);
            } else {
                DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node,
                        directory.getAbsolutePath(), directorySpec);
                takeFullSnapshot(snapshot);
                changesSinceLastSnapshot.merge(lastSnapshot.diff(snapshot));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new IoException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void pollForChanges() {
        if (fileObserver != null) {
            fileObserver.checkAndNotify();
        }
    }

    public List<FileChange> removeAndMergeChanges(List<String> files) {
        List<FileChange> removed = new ArrayList<FileChange>();
        List<FileChange> changes = changesSinceLastSnapshot.getFiles();
        for (String file : files) {
            for (FileChange fileChange : changes) {
                if (fileChange.getFileName().equals(file)) {
                    removed.add(fileChange);
                }
            }
        }
        changes.removeAll(removed);
        lastSnapshot.merge(changes);
        directorySnapshotPersister.save(lastSnapshot, nodeDirectorySpecKey);
        return removed;
    }

    synchronized public DirectorySpecSnapshot takeSnapshot() {
        pollForChanges();
        DirectorySpecSnapshot changes = changesSinceLastSnapshot;
        changesSinceLastSnapshot = new DirectorySpecSnapshot(node, directory.getAbsolutePath(),
                directorySpec);
        DirectorySpecSnasphotUpdater newListener = new DirectorySpecSnasphotUpdater(
                changesSinceLastSnapshot, false);
        fileObserver.addListener(newListener);
        fileObserver.removeListener(currentListener);
        currentListener = newListener;
        lastSnapshot.merge(changes);
        directorySnapshotPersister.save(lastSnapshot, nodeDirectorySpecKey);
        return changes;
    }

    synchronized protected void takeFullSnapshot(DirectorySpecSnapshot snapshot) {
        // update the snapshot with every file in the directory spec
        FileAlterationObserver observer = new FileAlterationObserver(directory,
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
                log.debug("File delete detected: {}", file.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(directory, file, FileChangeType.DELETE));
            }
        }

        public void onFileCreate(File file) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureCreates()) {
                log.debug("File create detected: {}", file.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(directory, file, FileChangeType.CREATE));
            }
        }

        public void onFileChange(File file) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureUpdates()) {
                log.debug("File change detected: {}", file.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(directory, file, FileChangeType.UPDATE));
            }
        }

        public void onDirectoryDelete(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureDeletes()) {
                log.debug("File delete detected: {}", directory.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(DirectoryChangeTracker.this.directory,
                        directory, FileChangeType.DELETE));
            }
        }

        public void onDirectoryCreate(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureCreates()) {
                log.debug("File create detected: {}", directory.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(DirectoryChangeTracker.this.directory,
                        directory, FileChangeType.CREATE));
            }
        }

        public void onDirectoryChange(File directory) {
            if (populateAll || snapshot.getDirectorySpec().isCaptureUpdates()) {
                log.debug("File change detected: {}", directory.getAbsolutePath());
                this.snapshot.addFileChange(new FileChange(DirectoryChangeTracker.this.directory,
                        directory, FileChangeType.UPDATE));
            }
        }

    }

}
