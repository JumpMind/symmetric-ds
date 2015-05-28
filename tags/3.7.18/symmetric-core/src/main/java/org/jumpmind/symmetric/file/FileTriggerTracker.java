/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTriggerTracker {

    final protected Logger log = LoggerFactory.getLogger(getClass());

    private FileTriggerRouter fileTriggerRouter;
    private FileAlterationObserver fileObserver;
    private DirectorySnapshot lastSnapshot;
    private DirectorySnapshot changesSinceLastSnapshot;
    private SnapshotUpdater currentListener;

    public FileTriggerTracker(FileTriggerRouter fileTriggerRouter, DirectorySnapshot lastSnapshot) {
        this.fileTriggerRouter = fileTriggerRouter;

        changesSinceLastSnapshot = new DirectorySnapshot(fileTriggerRouter);
        fileObserver = new FileAlterationObserver(fileTriggerRouter.getFileTrigger().getBaseDir(),
                fileTriggerRouter.getFileTrigger().createIOFileFilter());
        currentListener = new SnapshotUpdater(changesSinceLastSnapshot);
        fileObserver.addListener(currentListener);
        try {
            fileObserver.initialize();
            if (lastSnapshot == null) {
                lastSnapshot = new DirectorySnapshot(fileTriggerRouter);
            }
            this.lastSnapshot = lastSnapshot;
            DirectorySnapshot currentSnapshot = new DirectorySnapshot(fileTriggerRouter);
            takeFullSnapshot(currentSnapshot);
            changesSinceLastSnapshot.addAll(lastSnapshot.diff(currentSnapshot));
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new IoException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected void pollForChanges() {
        if (fileObserver != null) {
            fileObserver.checkAndNotify();
        }
    }

    synchronized public DirectorySnapshot trackChanges() {
        pollForChanges();
        DirectorySnapshot changes = changesSinceLastSnapshot;
        changesSinceLastSnapshot = new DirectorySnapshot(fileTriggerRouter);
        SnapshotUpdater newListener = new SnapshotUpdater(changesSinceLastSnapshot);
        fileObserver.addListener(newListener);
        fileObserver.removeListener(currentListener);
        currentListener = newListener;
        lastSnapshot.merge(changes);
        return changes;
    }

    synchronized protected void takeFullSnapshot(DirectorySnapshot snapshot) {
        // update the snapshot with every file in the directory spec
        FileAlterationObserver observer = new FileAlterationObserver(fileTriggerRouter
                .getFileTrigger().getBaseDir(), fileTriggerRouter.getFileTrigger()
                .createIOFileFilter());
        observer.addListener(new SnapshotUpdater(snapshot));
        observer.checkAndNotify();
    }

    class SnapshotUpdater extends FileAlterationListenerAdaptor {

        DirectorySnapshot snapshot;

        SnapshotUpdater(DirectorySnapshot snapshot) {
            this.snapshot = snapshot;
        }

        public void onFileDelete(File file) {
                log.debug("File delete detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.DELETE));
        }

        public void onFileCreate(File file) {
            if (snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnCtlFile()){
                onCtlFile(file);
            } else {
                log.debug("File create detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.CREATE));
            }
        }
        
        public void onCtlFile(File file) {
          if (snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnCtlFile()){
              File ctlFile = new File(file.getAbsolutePath() + ".ctl");
              if (ctlFile.exists()) {
                  log.debug("Control file detected: {}", file.getAbsolutePath());
                  this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                          LastEventType.CREATE));
              }
          }
      }

        public void onFileChange(File file) {
                log.debug("File change detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.MODIFY));
        }

        public void onDirectoryDelete(File directory) {
                log.debug("File delete detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), directory,
                        LastEventType.DELETE));
        }

        public void onDirectoryCreate(File directory) {
                log.debug("File create detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), directory,
                        LastEventType.CREATE));
        }

        public void onDirectoryChange(File directory) {
        }

    }

}
