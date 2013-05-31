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
        currentListener = new SnapshotUpdater(changesSinceLastSnapshot, false);
        fileObserver.addListener(currentListener);
        try {
            fileObserver.initialize();
            this.lastSnapshot = lastSnapshot;
            if (lastSnapshot == null || lastSnapshot.size() == 0) {
                this.lastSnapshot = changesSinceLastSnapshot;
                takeFullSnapshot(this.lastSnapshot);
            } else {
                DirectorySnapshot currentSnapshot = new DirectorySnapshot(fileTriggerRouter);
                takeFullSnapshot(currentSnapshot);
                changesSinceLastSnapshot.addAll(lastSnapshot.diff(currentSnapshot));
            }
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
        SnapshotUpdater newListener = new SnapshotUpdater(changesSinceLastSnapshot, false);
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
            if (populateAll || snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnDelete()) {
                log.debug("File delete detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.DELETE));
            }
        }

        public void onFileCreate(File file) {
            if (populateAll || snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnCreate()) {
                log.debug("File create detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.CREATE));
            }
        }

        public void onFileChange(File file) {
            if (populateAll || snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnModified()) {
                log.debug("File change detected: {}", file.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), file,
                        LastEventType.MODIFY));
            }
        }

        public void onDirectoryDelete(File directory) {
            if (populateAll || snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnDelete()) {
                log.debug("File delete detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), directory,
                        LastEventType.DELETE));
            }
        }

        public void onDirectoryCreate(File directory) {
            if (populateAll || snapshot.getFileTriggerRouter().getFileTrigger().isSyncOnCreate()) {
                log.debug("File create detected: {}", directory.getAbsolutePath());
                this.snapshot.add(new FileSnapshot(snapshot.getFileTriggerRouter(), directory,
                        LastEventType.CREATE));
            }
        }

        public void onDirectoryChange(File directory) {
        }

    }

}
