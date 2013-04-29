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
package org.jumpmind.symmetric.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTriggerTracker {

    final protected Logger log = LoggerFactory.getLogger(getClass());

    private FileTrigger fileTrigger;
    private FileAlterationObserver fileObserver;
    private DirectorySnapshot lastSnapshot;
    private DirectorySnapshot changesSinceLastSnapshot;
    private SnapshotUpdater currentListener;

    public FileTriggerTracker(FileTrigger fileTrigger, DirectorySnapshot lastSnapshot) {
        this.fileTrigger = fileTrigger;

        changesSinceLastSnapshot = new DirectorySnapshot(fileTrigger);
        fileObserver = new FileAlterationObserver(fileTrigger.getBaseDir(),
                fileTrigger.createIOFileFilter());
        currentListener = new SnapshotUpdater(changesSinceLastSnapshot, false);
        fileObserver.addListener(currentListener);
        try {
            fileObserver.initialize();
            this.lastSnapshot = lastSnapshot;
            if (lastSnapshot == null || lastSnapshot.size() == 0) {
                this.lastSnapshot = changesSinceLastSnapshot;
                takeFullSnapshot(this.lastSnapshot);
            } else {
                DirectorySnapshot snapshot = new DirectorySnapshot(fileTrigger);
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

    synchronized public DirectorySnapshot takeSnapshot() {
        pollForChanges();
        DirectorySnapshot changes = changesSinceLastSnapshot;
        changesSinceLastSnapshot = new DirectorySnapshot(fileTrigger);
        SnapshotUpdater newListener = new SnapshotUpdater(changesSinceLastSnapshot, false);
        fileObserver.addListener(newListener);
        fileObserver.removeListener(currentListener);
        currentListener = newListener;
        lastSnapshot.merge(changes);
        return changes;
    }

    synchronized protected void takeFullSnapshot(DirectorySnapshot snapshot) {
        // update the snapshot with every file in the directory spec
        FileAlterationObserver observer = new FileAlterationObserver(fileTrigger.getBaseDir(),
                fileTrigger.createIOFileFilter());
        observer.addListener(new SnapshotUpdater(snapshot, true));
        observer.checkAndNotify();
    }

    class SnapshotUpdater extends FileAlterationListenerAdaptor {

        DirectorySnapshot snapshot;
        boolean populateAll = false;

        SnapshotUpdater(DirectorySnapshot snapshot, boolean populateAll) {
            this.snapshot = snapshot;
            this.populateAll = populateAll;
        }

        public void onFileDelete(File file) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnDelete()) {
                log.debug("File delete detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), file,
                        LastEventType.DELETE));
            }
        }

        public void onFileCreate(File file) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnCreate()) {
                log.debug("File create detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), file,
                        LastEventType.CREATE));
            }
        }

        public void onFileChange(File file) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnModified()) {
                log.debug("File change detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), file,
                        LastEventType.MODIFY));
            }
        }

        public void onDirectoryDelete(File directory) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnDelete()) {
                log.debug("File delete detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), directory,
                        LastEventType.DELETE));
            }
        }

        public void onDirectoryCreate(File directory) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnCreate()) {
                log.debug("File create detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), directory,
                        LastEventType.CREATE));
            }
        }

        public void onDirectoryChange(File directory) {
            if (populateAll || snapshot.getFileTrigger().isSyncOnModified()) {
                log.debug("File change detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTrigger(), directory,
                        LastEventType.MODIFY));
            }
        }

    }

}
