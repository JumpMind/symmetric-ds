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
package org.jumpmind.symmetric.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import static org.junit.Assert.*;

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
    
    File changeNameSvrSourceDir = new File("target/fs_svr/change_name");
    File changeNameClntTargetDir = new File("target/fs_clnt/change_name");


    File[] allDirs = new File[] { allSvrSourceDir, allClntTargetDir, pingbackClientDir,
            pingbackServerDir, chooseTargetClientDirA, chooseTargetClientDirB,
            chooseTargetServerDir, changeNameSvrSourceDir };

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        createDirsAndInitialFiles();

        loadConfigAndRegisterNode("client", "server");
        
        assertFalse(pullFiles());

        testInitialLoadFromServerToClient(rootServer, clientServer);

        testPullAllFromServerToClient(rootServer, clientServer);

        testPingback();

        testChooseTargetDirectory(rootServer, clientServer);
        
        testChangeFileNameAndCreateTargetDir(rootServer, clientServer);

    }

    protected void testInitialLoadFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        assertFalse("The initial load file should not exist at the client",
                allClntTargetInitialLoadFile.exists());
        assertTrue("The initial load file should exist at the server",
                allSvrSourceInitialLoadFile.exists());
        assertFalse(pullFiles());
        assertFalse("The initial load file should not exist at the client",
                allClntTargetInitialLoadFile.exists());
        assertTrue("The initial load file should exist at the server",
                allSvrSourceInitialLoadFile.exists());
        rootServer.reloadNode(clientServer.getNodeService().findIdentityNodeId(), "unit_test");
        assertTrue(pullFiles());
        assertTrue("The initial load file should exist at the client",
                allClntTargetInitialLoadFile.exists());
    }

    protected void testPullAllFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        File allFile1 = new File(allSvrSourceDir, "subdir/1.txt");
        allFile1.getParentFile().mkdirs();
        String file1Contents = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\nq\nr\ns\nt\nu\nv\nw\nx\ny\nz";
        FileUtils.write(allFile1, file1Contents);

        pullFiles();

        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        assertTrue(allFile1Target.exists());
        assertEquals(file1Contents, FileUtils.readFileToString(allFile1Target));
        
        // test update
        FileUtils.write(allFile1, file1Contents, true);

        pullFiles();
        
        assertEquals(file1Contents + file1Contents, FileUtils.readFileToString(allFile1Target));

        allFile1.delete();

        pullFiles();

        assertFalse(allFile1Target.exists());
    }

    protected void testPingback() throws Exception {
        File serverFile = new File(pingbackServerDir, "ping.txt");
        assertFalse(serverFile.exists());

        assertFalse("Should not have pulled any files", pullFiles());

        File clientFile = new File(pingbackClientDir, "ping.txt");
        FileUtils.write(clientFile, "test");

        assertTrue(pushFiles());

        assertTrue(serverFile.exists());

        assertFalse("Should not have pulled any files", pullFiles());

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

         assertFalse(clientOneA.exists());
         assertFalse(clientOneB.exists());
         
         FileUtils.write(one, "abc");
         
         pullFiles();
         
         assertTrue(clientOneA.exists());
         assertFalse(clientOneB.exists());
         
         FileUtils.write(two, "abcdef");
         
         pullFiles();

         assertFalse(clientTwoA.exists());
         assertTrue(clientTwoB.exists());
                  
         FileUtils.write(three, "abcdef");
         
         pullFiles();
         
         assertTrue(clientThreeA.exists());
         assertFalse(clientThreeB.exists());

    }
    
    protected void testChangeFileNameAndCreateTargetDir(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        if (changeNameClntTargetDir.exists()) {
            FileUtils.deleteDirectory(changeNameClntTargetDir);
        }
        
        File sourceFile = new File(changeNameSvrSourceDir, "source.txt");
        File targetFile = new File(changeNameClntTargetDir, "target.txt");
        
        assertFalse(targetFile.exists());
        assertFalse(targetFile.getParentFile().exists());
        
        FileUtils.write(sourceFile, "1234567890");
        
        pullFiles();
        
        assertTrue(targetFile.getParentFile().exists());
        assertTrue(targetFile.exists());
        
        FileUtils.deleteQuietly(sourceFile);
        
        pullFiles();
        
        assertFalse(targetFile.exists());
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
