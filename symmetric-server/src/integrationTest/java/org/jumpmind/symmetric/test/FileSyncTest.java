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
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.FileConflictStrategy;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.service.IFileSyncService;

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
        testSourceWins(rootServer, clientServer);
        testTargetWins(rootServer, clientServer);
        testManual(rootServer, clientServer);
        testUpdateManual(rootServer, clientServer);
        testCreateAndUpdateInSameBatch(rootServer, clientServer);
    }

    protected void testInitialLoadFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testInitialLoadFromServerToClient");
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
        logEndOfTest("testInitialLoadFromServerToClient");
    }

    protected void testPullAllFromServerToClient(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testPullAllFromServerToClient");
        File allFile1 = new File(allSvrSourceDir, "subdir/1.txt");
        allFile1.getParentFile().mkdirs();
        String file1Contents = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\nq\nr\ns\nt\nu\nv\nw\nx\ny\nz";
        FileUtils.write(allFile1, file1Contents, Charset.defaultCharset(), false);
        assertTrue(allFile1.exists());
        assertEquals(file1Contents, FileUtils.readFileToString(allFile1, Charset.defaultCharset()));
        pullFiles();
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        // Check if target file exists. If not, run the pullFiles() again
        if (!allFile1Target.exists()) {
            pullFiles();
        }
        assertTrue(allFile1Target.exists());
        assertEquals(file1Contents, FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        // test update
        FileUtils.write(allFile1, file1Contents, Charset.defaultCharset(), true);
        pullFiles();
        if (!(file1Contents + file1Contents).equals(FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()))) {
            // Let's try again
            pullFiles();
        }
        assertEquals(file1Contents + file1Contents, FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        allFile1.delete();
        pullFiles();
        if (allFile1Target.exists()) {
            // Let's try again
            pullFiles();
        }
        assertFalse(allFile1Target.exists());
        logEndOfTest("testPullAllFromServerToClient");
    }

    protected void testPingback() throws Exception {
        logStartOfTest("testPingback");
        File serverFile = new File(pingbackServerDir, "ping.txt");
        assertFalse(serverFile.exists());
        assertFalse("Should not have pulled any files", pullFiles());
        File clientFile = new File(pingbackClientDir, "ping.txt");
        FileUtils.write(clientFile, "test", Charset.defaultCharset(), false);
        assertTrue(pushFiles());
        assertTrue(serverFile.exists());
        assertFalse("Should not have pulled any files", pullFiles());
        logEndOfTest("testPingback");
    }

    protected void testChooseTargetDirectory(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testChooseTargetDirectory");
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
        FileUtils.write(one, "abc", Charset.defaultCharset(), false);
        pullFiles();
        if (!clientOneA.exists()) {
            // Let's try again
            pullFiles();
        }
        assertTrue(clientOneA.exists());
        assertFalse(clientOneB.exists());
        FileUtils.write(two, "abcdef", Charset.defaultCharset(), false);
        pullFiles();
        if (!clientTwoB.exists()) {
            // Let's try again
            pullFiles();
        }
        assertFalse(clientTwoA.exists());
        assertTrue(clientTwoB.exists());
        FileUtils.write(three, "abcdef", Charset.defaultCharset(), false);
        pullFiles();
        if (!clientThreeA.exists()) {
            // Let's try again
            pullFiles();
        }
        assertTrue(clientThreeA.exists());
        assertFalse(clientThreeB.exists());
        logEndOfTest("testChooseTargetDirectory");
    }

    protected void testChangeFileNameAndCreateTargetDir(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testChangeFileNameAndCreateTargetDir");
        if (changeNameClntTargetDir.exists()) {
            FileUtils.deleteDirectory(changeNameClntTargetDir);
        }
        File sourceFile = new File(changeNameSvrSourceDir, "source.txt");
        File targetFile = new File(changeNameClntTargetDir, "target.txt");
        assertFalse(targetFile.exists());
        assertFalse(targetFile.getParentFile().exists());
        FileUtils.write(sourceFile, "1234567890", Charset.defaultCharset(), false);
        pullFiles();
        if (!targetFile.exists()) {
            // Let's try again
            pullFiles();
        }
        assertTrue(targetFile.getParentFile().exists());
        assertTrue(targetFile.exists());
        FileUtils.deleteQuietly(sourceFile);
        pullFiles();
        if (targetFile.exists()) {
            // Let's try again
            pullFiles();
        }
        assertFalse(targetFile.exists());
        logEndOfTest("testChangeFileNameAndCreateTargetDir");
    }

    protected void testSourceWins(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testSourceWins");
        File allFile1 = new File(allSvrSourceDir, "svr_wins/test.txt");
        allFile1.getParentFile().mkdirs();
        String file1Contents = "server value";
        FileUtils.write(allFile1, file1Contents, Charset.defaultCharset(), false);
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        allFile1Target.getParentFile().mkdirs();
        FileUtils.write(allFile1Target, "client value", Charset.defaultCharset(), false);
        pullFiles();
        if (!file1Contents.equals(FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()))) {
            // Let's try again
            pullFiles();
        }
        assertEquals(file1Contents, FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        logEndOfTest("testSourceWins");
    }

    protected void testTargetWins(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testTargetWins");
        IFileSyncService fileSyncService = rootServer.getFileSyncService();
        FileTriggerRouter fileTriggerRouter = fileSyncService.getFileTriggerRouter("all", "server_2_client", false);
        fileTriggerRouter.setConflictStrategy(FileConflictStrategy.TARGET_WINS);
        fileSyncService.saveFileTriggerRouter(fileTriggerRouter);
        pull("client");
        File allFile1 = new File(allSvrSourceDir, "tgt_wins/test.txt");
        allFile1.getParentFile().mkdirs();
        FileUtils.write(allFile1, "server value", Charset.defaultCharset(), false);
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        allFile1Target.getParentFile().mkdirs();
        FileUtils.write(allFile1Target, "client value", Charset.defaultCharset(), false);
        pullFiles();
        if ("client value".equals(FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()))) {
            // Let's try again
            pullFiles();
        }
        assertEquals("client value", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        logEndOfTest("testTargetWins");
    }

    protected void testManual(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testManual");
        IFileSyncService fileSyncService = rootServer.getFileSyncService();
        FileTriggerRouter fileTriggerRouter = fileSyncService.getFileTriggerRouter("all", "server_2_client", false);
        fileTriggerRouter.setConflictStrategy(FileConflictStrategy.MANUAL);
        fileSyncService.saveFileTriggerRouter(fileTriggerRouter);
        pull("client");
        File allFile1 = new File(allSvrSourceDir, "manual/test.txt");
        allFile1.getParentFile().mkdirs();
        FileUtils.write(allFile1, "server value", Charset.defaultCharset(), false);
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        allFile1Target.getParentFile().mkdirs();
        FileUtils.write(allFile1Target, "client value", Charset.defaultCharset(), false);
        pullFiles();
        if ("client value".equals(FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()))) {
            // Let's try again
            pullFiles();
        }
        assertEquals("client value", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        OutgoingBatches batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        List<OutgoingBatch> batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(1, batches.size());
        allFile1Target.delete();
        pullFiles();
        if ((!allFile1Target.exists())) {
            // Let's try again
            Thread.sleep(1000l);
            pullFiles();
        }
        assertEquals("server value", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        logEndOfTest("testManual");
    }

    protected void testUpdateManual(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testUpdateManual");
        OutgoingBatches batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        List<OutgoingBatch> batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        IFileSyncService fileSyncService = rootServer.getFileSyncService();
        FileTriggerRouter fileTriggerRouter = fileSyncService.getFileTriggerRouter("all", "server_2_client", false);
        fileTriggerRouter.setConflictStrategy(FileConflictStrategy.MANUAL);
        fileSyncService.saveFileTriggerRouter(fileTriggerRouter);
        pull("client");
        File allFile1 = new File(allSvrSourceDir, "manual/test2.txt");
        allFile1.getParentFile().mkdirs();
        FileUtils.write(allFile1, "base value", Charset.defaultCharset(), false);
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        allFile1Target.getParentFile().mkdirs();
        pullFiles();
        if (!allFile1Target.exists()) {
            // Let's try again
            pullFiles();
        }
        assertEquals("base value", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        FileUtils.write(allFile1, "new value", Charset.defaultCharset(), true);
        pullFiles();
        if (!"base valuenew value".equals(FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()))) {
            // Let's try again
            pullFiles();
        }
        assertEquals("base valuenew value", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        logEndOfTest("testUpdateManual");
    }

    protected void testCreateAndUpdateInSameBatch(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        logStartOfTest("testCreateAndUpdateInSameBatch");
        OutgoingBatches batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        List<OutgoingBatch> batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        File allFile1 = new File(allSvrSourceDir, "createAndUpdate/test.txt");
        allFile1.getParentFile().mkdirs();
        FileUtils.write(allFile1, "create value ", Charset.defaultCharset(), false);
        trackChangesOnServer();
        FileUtils.write(allFile1, "plus update", Charset.defaultCharset(), true);
        trackChangesOnServer();
        File allFile1Target = new File(allClntTargetDir, allFile1.getParentFile().getName() + "/" + allFile1.getName());
        allFile1Target.getParentFile().mkdirs();
        pullFiles();
        batchesInError = rootServer.getOutgoingBatchService().getOutgoingBatchErrors(10);
        batches = batchesInError.getBatchesForChannel(Constants.CHANNEL_FILESYNC);
        assertEquals(0, batches.size());
        if (!allFile1Target.exists()) {
            // Let's try again
            pullFiles();
        }
        assertTrue(allFile1Target.exists());
        assertEquals("create value plus update", FileUtils.readFileToString(allFile1Target, Charset.defaultCharset()));
        logEndOfTest("testCreateAndUpdateInSameBatch");
    }

    protected void trackChangesOnServer() {
        getWebServer("server").getEngine().getFileSyncService().trackChanges(true);
    }

    protected boolean pullFiles() {
        trackChangesOnServer();
        getWebServer("server").getEngine().getInitialLoadService().queueLoads(true);
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
        FileUtils.write(allSvrSourceInitialLoadFile, "Initial Load Data", Charset.defaultCharset(), false);
    }
}
