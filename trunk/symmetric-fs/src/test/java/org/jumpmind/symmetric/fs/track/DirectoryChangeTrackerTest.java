package org.jumpmind.symmetric.fs.track;

import java.io.File;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.service.filesystem.FileSystemDirectorySpecSnapshotPersister;
import org.junit.Before;
import org.junit.Test;

public class DirectoryChangeTrackerTest {

    File snapshotDirectory = new File("target/snapshots");
    File directory = new File("target/test");
    File subdirectory = new File (directory, "a");
    File fileInDirectory1 = new File(directory, "1.txt");
    File fileInDirectory2 = new File(directory, "2.csv");
    File fileInSubDirectory = new File(subdirectory, "3.doc");
    Node node = new Node("1", "server", "", "");

    @Before
    public void setupTest() throws Exception {
        recreateDirectorySpecAndFiles();
    }
    
    @Test
    public void testTakeFullSnapshotRecursive() throws Exception {
        DirectorySpec directorySpec = new DirectorySpec(directory.getAbsolutePath(), true, null, null);        
        DirectoryChangeTracker tracker = new DirectoryChangeTracker(node, directorySpec, new FileSystemDirectorySpecSnapshotPersister(snapshotDirectory.getAbsolutePath()), -1);
        DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node, directorySpec);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(4, snapshot.getFiles().size());        
    }
    
    @Test
    public void testTakeFullSnapshotNonRecursive() throws Exception {
        DirectorySpec directorySpec = new DirectorySpec(directory.getAbsolutePath(), false, null, null);
        DirectoryChangeTracker tracker = new DirectoryChangeTracker(node, directorySpec, new FileSystemDirectorySpecSnapshotPersister(snapshotDirectory.getAbsolutePath()), -1);
        DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node, directorySpec);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(2, snapshot.getFiles().size());        
    }
    
    @Test
    public void testTakeFullSnapshotIncludes() throws Exception {
        DirectorySpec directorySpec = new DirectorySpec(directory.getAbsolutePath(), false, new String[] {"*.txt"}, null);
        DirectoryChangeTracker tracker = new DirectoryChangeTracker(node, directorySpec, new FileSystemDirectorySpecSnapshotPersister(snapshotDirectory.getAbsolutePath()), -1);
        DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node, directorySpec);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(1, snapshot.getFiles().size());
        Assert.assertEquals(snapshot.getFiles().get(0).getFile().getAbsolutePath(), fileInDirectory1.getAbsolutePath());
    }   
    
    @Test
    public void testTakeFullSnapshotExcludes() throws Exception {
        DirectorySpec directorySpec = new DirectorySpec(directory.getAbsolutePath(), false, null, new String[] {"*.txt"});
        DirectoryChangeTracker tracker = new DirectoryChangeTracker(node, directorySpec, new FileSystemDirectorySpecSnapshotPersister(snapshotDirectory.getAbsolutePath()), -1);
        DirectorySpecSnapshot snapshot = new DirectorySpecSnapshot(node, directorySpec);
        tracker.takeFullSnapshot(snapshot);
        Assert.assertEquals(1, snapshot.getFiles().size());
        Assert.assertEquals(snapshot.getFiles().get(0).getFile().getAbsolutePath(), fileInDirectory2.getAbsolutePath());
    }      
    
    @Test
    public void testTakeSnapshotRecursiveTestDelete() throws Exception {
        DirectorySpec directorySpec = new DirectorySpec(directory.getAbsolutePath(), true, null, null);
        DirectoryChangeTracker tracker = new DirectoryChangeTracker(node, directorySpec, new FileSystemDirectorySpecSnapshotPersister(snapshotDirectory.getAbsolutePath()), -1);
        tracker.start();
        tracker.takeSnapshot();
        FileUtils.deleteQuietly(fileInDirectory1);
        tracker.pollForChanges();
        DirectorySpecSnapshot snapshot = tracker.takeSnapshot();
        Assert.assertEquals(1, snapshot.getFiles().size());
        FileChange change = snapshot.getFiles().get(0);
        Assert.assertEquals(change.getFile().getAbsolutePath(), fileInDirectory1.getAbsolutePath());
        Assert.assertEquals(change.getFileChangeType(), FileChangeType.DELETE);
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
