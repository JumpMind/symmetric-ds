package org.jumpmind.symmetric.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.junit.Assert;

public class FileSyncTest extends AbstractTest {

    File allSvrSourceDir = new File("target/fs_svr/all");
    File allSvrSourceInitialLoadFile = new File(allSvrSourceDir, "initialload.txt");
    File allClntTargetDir = new File("target/fs_clnt/all");
    File allClntTargetInitialLoadFile = new File(allClntTargetDir, "initialload.txt");

    File pingbackServerDir = new File("target/fs_svr/ping_back");
    File pingbackClientDir = new File("target/fs_clnt/ping_back");

    File chooseTargetServerDir = new File("target/fs_svr/choose_target");
    File chooseTargetClientDirA = new File("target/fs_clnt/choose_target/a");
    File chooseTargetClientDirB = new File("target/fs_clnt/choose_target/b");

    File[] allDirs = new File[] { allSvrSourceDir, allClntTargetDir, pingbackClientDir,
            pingbackServerDir, chooseTargetClientDirA, chooseTargetClientDirB,
            chooseTargetServerDir };

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        createDirsAndInitialFiles();

        loadConfigAndRegisterNode("client", "server");

        pullFiles();

        testInitialLoadFromServerToClient(rootServer, clientServer);

        testPullAllFromServerToClient(rootServer, clientServer);

        testPingback();

        testChooseTargetDirectory(rootServer, clientServer);

    }

    protected void testInitialLoadFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        Assert.assertFalse("The initial load file should not exist at the client",
                allClntTargetInitialLoadFile.exists());
        Assert.assertTrue("The initial load file should exist at the server",
                allSvrSourceInitialLoadFile.exists());
        pullFiles();
        Assert.assertFalse("The initial load file should not exist at the client",
                allClntTargetInitialLoadFile.exists());
        Assert.assertTrue("The initial load file should exist at the server",
                allSvrSourceInitialLoadFile.exists());
        rootServer.reloadNode(clientServer.getNodeService().findIdentityNodeId(), "unit_test");
        pullFiles();
        Assert.assertTrue("The initial load file should exist at the client",
                allClntTargetInitialLoadFile.exists());
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

    protected void testChooseTargetDirectory(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
         File one = new File(chooseTargetServerDir, "1.txt");
         File two = new File(chooseTargetServerDir, "2.txt");
         File three = new File(chooseTargetServerDir, "3.txt");
         
         File clientOneA = new File(chooseTargetClientDirA, "1.txt");
         File clientOneB = new File(chooseTargetClientDirB, "1.txt");
         
         File clientTwoA = new File(chooseTargetClientDirA, "2.txt");
         File clientTwoB = new File(chooseTargetClientDirB, "2.txt");

         
         File clientThreeA = new File(chooseTargetClientDirA, "3.txt");
         File clientThreeB = new File(chooseTargetClientDirB, "3.txt");

         Assert.assertFalse(clientOneA.exists());
         Assert.assertFalse(clientOneB.exists());
         
         FileUtils.write(one, "abc");
         
         pullFiles();
         
         Assert.assertTrue(clientOneA.exists());
         Assert.assertFalse(clientOneB.exists());
         
         FileUtils.write(two, "abcdef");
         
         pullFiles();

         Assert.assertFalse(clientTwoA.exists());
         Assert.assertTrue(clientTwoB.exists());
                  
         FileUtils.write(three, "abcdef");
         
         pullFiles();
         
         Assert.assertTrue(clientThreeA.exists());
         Assert.assertFalse(clientThreeB.exists());



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

    protected void createDirsAndInitialFiles() throws Exception {
        for (File dir : allDirs) {
            FileUtils.deleteDirectory(dir);
            dir.mkdirs();
        }

        FileUtils.write(allSvrSourceInitialLoadFile, "Initial Load Data");
    }

}
