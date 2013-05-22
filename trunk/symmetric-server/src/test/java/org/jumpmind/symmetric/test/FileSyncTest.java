package org.jumpmind.symmetric.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.junit.Assert;

public class FileSyncTest extends AbstractTest {

    File allSvrSourceDir = new File("target/fs_svr/all");
    File allClntTargetDir = new File("target/fs_clnt/all");
    
    File pingbackServerDir = new File("target/fs_svr/ping_back");
    File pingbackClientDir = new File("target/fs_clnt/ping_back");

    File[] allDirs = new File[] { allSvrSourceDir, allClntTargetDir, pingbackClientDir, pingbackServerDir };

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        createDirs();

        loadConfigAndRegisterNode("client", "server");

        pullFiles();

        testPullAllFromServerToClient(rootServer, clientServer);
        
        testPingback();

    }

    protected void testPullAllFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        File allFile1 = new File(allSvrSourceDir, "1.txt");
        String file1Contents = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\nq\nr\ns\nt\nu\nv\nw\nx\ny\nz";
        FileUtils.write(allFile1, file1Contents);

        pullFiles();

        File allFile1Target = new File(allClntTargetDir, allFile1.getName());
        Assert.assertTrue(allFile1Target.exists());
        Assert.assertEquals(file1Contents, FileUtils.readFileToString(allFile1Target));

        allFile1.delete();

        pullFiles();

        Assert.assertFalse(allFile1Target.exists());
    }
    
    protected void testPingback() throws Exception {
        File serverFile = new File(pingbackServerDir, "ping.txt");
        Assert.assertFalse(serverFile.exists());
        
        Assert.assertFalse("Should not have pulled any files", pullFiles());
        
        File clientFile = new File(pingbackClientDir, "ping.txt");      
        FileUtils.write(clientFile, "test");
                
        Assert.assertTrue(pushFiles());
        
        Assert.assertTrue(serverFile.exists());
        
        Assert.assertFalse("Should not have pulled any files", pullFiles());
        
    }

    protected void testChooseTargetDirectory() {
        
    }
    
    protected void testUseExpressionInPathToRoute() {
        
    }
    
    protected boolean pullFiles() {
        getWebServer("server").getEngine().getFileSyncService().trackChanges(true);
        getWebServer("server").getEngine().getRouterService().routeData(true);
        return pullFiles("client");
    }
    
    protected boolean pushFiles() {
        getWebServer("client").getEngine().getFileSyncService().trackChanges(true);
        getWebServer("client").getEngine().getRouterService().routeData(true);
        return pushFiles("client");
    }

    protected void createDirs() throws Exception {
        for (File dir : allDirs) {
            FileUtils.deleteDirectory(dir);
            dir.mkdirs();
        }
    }

}
