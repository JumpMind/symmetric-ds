package org.jumpmind.symmetric.fs.client;

import java.io.File;

import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class FileSystemSyncStatusPersisterTest {

    @Test
    public void testSave() {
        final File dir = new File("target/sync_status");
        dir.mkdirs();
        FileSystemSyncStatusPersister persister = new FileSystemSyncStatusPersister(dir.getAbsolutePath());
        SyncStatus status = new SyncStatus(new Node("12345", "clientgroup", "http://10.2.34.11/fsync", "DFR#$#3223S#D%%"));
        status.setSnapshot(new DirectorySpecSnapshot("12345", new DirectorySpec("/opt/send", true, null, new String[] {".svn"})));
        persister.save(status);
        
        SyncStatus newStatus = persister.get(status.getNode());
        Assert.assertNotNull(newStatus);
        Assert.assertNotNull(newStatus.getNode());
        Assert.assertNotNull(newStatus.getStage());
        Assert.assertEquals(status.getStage(), newStatus.getStage());
        
        File expectedFile = new File(dir, "12345.status");
        Assert.assertTrue(expectedFile.exists());
        
    }
}
