package org.jumpmind.symmetric.file;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTriggerRouter;

public class DirectorySnapshot extends ArrayList<FileSnapshot> {

    private static final long serialVersionUID = 1L;

    private FileTriggerRouter fileTriggerRouter;

    public DirectorySnapshot(FileTriggerRouter fileTriggerRouter, List<FileSnapshot> snapshot) {
        this(fileTriggerRouter);
        addAll(snapshot);
    }

    public DirectorySnapshot(FileTriggerRouter fileTriggerRouter) {
        this.fileTriggerRouter = fileTriggerRouter;
    }

    protected void merge(DirectorySnapshot snapshot) {
        Set<FileSnapshot> toAdd = new HashSet<FileSnapshot>();
        Set<FileSnapshot> toRemove = new HashSet<FileSnapshot>();
        for (FileSnapshot fileChange : snapshot) {
            for (FileSnapshot file : this) {
                if (fileChange.getFileName().equals(file.getFileName())) {
                    toRemove.add(file);
                    if (fileChange.getLastEventType() == LastEventType.MODIFY) {
                        toAdd.add(fileChange);
                    }
                }
            }
        }

        for (FileSnapshot fileChange : toRemove) {
            if (fileChange.getLastEventType() == LastEventType.CREATE) {
                toAdd.add(fileChange);
            }
        }

        this.removeAll(toRemove);
        this.addAll(toAdd);
    }

    protected DirectorySnapshot diff(DirectorySnapshot anotherSnapshot) {
        DirectorySnapshot differences = new DirectorySnapshot(anotherSnapshot.getFileTriggerRouter());
        for (FileSnapshot anotherFile : anotherSnapshot) {
            boolean found = false;
            for (FileSnapshot file : this) {
                if (anotherFile.sameFile(file)) {
                    found = true;
                    if (file.getLastEventType() == LastEventType.MODIFY
                            && anotherFile.getLastEventType() == LastEventType.CREATE) {
                        anotherFile.setLastEventType(LastEventType.MODIFY);
                    }
                    if (!anotherFile.equals(file)) {
                        differences.add(anotherFile);
                    }
                }
            }
            if (!found) {
                differences.add(anotherFile);
            }
        }

        for (FileSnapshot file : this) {
            boolean found = false;
            for (FileSnapshot anotherFile : anotherSnapshot) {
                if (anotherFile.sameFile(file)) {
                    found = true;
                }
            }
            if (file.getLastEventType() != LastEventType.DELETE && !found) {
                FileSnapshot copy = new FileSnapshot(file);
                copy.setLastEventType(LastEventType.DELETE);
                differences.add(copy);
            }
        }
        return differences;
    }
    
    public FileTriggerRouter getFileTriggerRouter() {
        return fileTriggerRouter;
    }

}
