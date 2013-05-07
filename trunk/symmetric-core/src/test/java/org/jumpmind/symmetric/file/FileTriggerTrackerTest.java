package org.jumpmind.symmetric.file;

import java.io.File;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Router;
import org.junit.Before;
import org.junit.Test;

public class FileTriggerTrackerTest {

    File snapshotDirectory = new File("target/snapshots");
    File directory = new File("target/test");
    File subdirectory = new File (directory, "a");
    File fileInDirectory1 = new File(directory, "1.txt");
    File fileInDirectory2 = new File(directory, "2.csv");
    File fileInSubDirectory = new File(subdirectory, "3.doc");

    @Before
    public void setupTest() throws Exception {
        recreateDirectorySpecAndFiles();
    }
    
    @Test
    public void testTakeFullSnapshotRecursive() throws Exception {
        FileTrigger fileTrigger = new FileTrigger(directory.getAbsolutePath(), true, null, null);
        Router router = new Router();
        FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
        FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, null);
        DirectorySnapshot snapshot = new DirectorySnapshot(fileTriggerRouter);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(4, snapshot.size());        
    }
    
    @Test
    public void testTakeFullSnapshotNonRecursive() throws Exception {
        FileTrigger fileTrigger = new FileTrigger(directory.getAbsolutePath(), false, null, null);
        Router router = new Router();
        FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
        FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, null);
        DirectorySnapshot snapshot = new DirectorySnapshot(fileTriggerRouter);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(2, snapshot.size());        
    }
    
    @Test
    public void testTakeFullSnapshotIncludes() throws Exception {
        FileTrigger fileTrigger = new FileTrigger(directory.getAbsolutePath(), false, "*.txt", null);
        Router router = new Router();
        FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
        FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, null);
        DirectorySnapshot snapshot = new DirectorySnapshot(fileTriggerRouter);       
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(snapshot.get(0).getFileName(), FileSyncUtils.getRelativePath(fileInDirectory1, directory));
    }   
    
    @Test
    public void testTakeFullSnapshotExcludes() throws Exception {
        FileTrigger fileTrigger = new FileTrigger(directory.getAbsolutePath(), false, null,  "*.txt");
        Router router = new Router();
        FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
        FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, null);
        DirectorySnapshot snapshot = new DirectorySnapshot(fileTriggerRouter);
        
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(snapshot.get(0).getFileName(), FileSyncUtils.getRelativePath(fileInDirectory2, directory));
    }      
    
    @Test
    public void testTakeSnapshotRecursiveTestDelete() throws Exception {        
        FileTrigger fileTrigger = new FileTrigger(directory.getAbsolutePath(), true, null, null);
        Router router = new Router();
        FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
        FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, null);
        tracker.trackChanges();
        FileUtils.deleteQuietly(fileInDirectory1);
        DirectorySnapshot snapshot = tracker.trackChanges();
        Assert.assertEquals(1, snapshot.size());
        FileSnapshot change = snapshot.get(0);
        Assert.assertEquals(change.getFileName(), FileSyncUtils.getRelativePath(fileInDirectory1, directory));
        Assert.assertEquals(change.getLastEventType(), LastEventType.DELETE);
    }    
    
    @Test
    public void testTakeSnapshotAfterRestart() throws Exception {
        
    }
        
    protected void recreateDirectorySpecAndFiles() throws Exception {     
        FileUtils.deleteQuietly(snapshotDirectory);
        FileUtils.deleteQuietly(directory);
        directory.mkdirs();
        subdirectory.mkdirs();
        FileUtils.write(fileInDirectory1, "abc");
        FileUtils.write(fileInDirectory2, "1,2,3");
        FileUtils.write(fileInSubDirectory, "abc");
    }

}
