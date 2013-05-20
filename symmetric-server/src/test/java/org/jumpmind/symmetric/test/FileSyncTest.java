package org.jumpmind.symmetric.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.junit.Assert;

public class FileSyncTest extends AbstractTest {

    File allSvrSourceDir = new File("target/fs_svr/all");
    File allClntTargetDir = new File("target/fs_clnt/all");
    
    File[] allDirs = new File[] { allSvrSourceDir, allClntTargetDir }; 
    
    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        createDirs();        
        loadConfigAtRegistrationServer();
        rootServer.openRegistration("client", "client");
        pull("client");
        rootServer.getFileSyncService().trackChanges(true);
        
        File allFile1 = new File(allSvrSourceDir, "1.txt");
        String file1Contents = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\nq\nr\ns\nt\nu\nv\nw\nx\ny\nz";
        FileUtils.write(allFile1, file1Contents);
        
        rootServer.getFileSyncService().trackChanges(true);
        rootServer.getRouterService().routeData(true);
        pullFiles("client");
        
        File allFile1Target = new File(allClntTargetDir, allFile1.getName());
        Assert.assertTrue(allFile1Target.exists());
        Assert.assertEquals(file1Contents, FileUtils.readFileToString(allFile1Target));
                       
    }
    
    protected void createDirs() throws Exception {
        for (File dir : allDirs) {
            FileUtils.deleteDirectory(dir);
            dir.mkdirs();
        }
    }

}
