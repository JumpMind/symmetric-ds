package org.jumpmind.symmetric.fs.client;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.fs.SyncParameterConstants;
import org.jumpmind.symmetric.fs.client.connector.TransportConnectorFactory;
import org.jumpmind.symmetric.fs.config.ConflictStrategy;
import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.config.SyncDirection;
import org.jumpmind.symmetric.fs.service.filesystem.FileSystemPersisterServices;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class SyncJobTest {

    ThreadPoolTaskScheduler taskScheduler;

    SyncJob job;

    final static String CONFIG_DIR = "target/config";
    final static String STATUS_DIR = "target/status";
    final static String CLIENT_DIR = "target/client";
    final static String SERVER_DIR = "target/server";

    final static File clientFile = new File(CLIENT_DIR, "test.txt");
    final static File serverFile = new File(SERVER_DIR, "test.txt");

    @Before
    public void createAndInitTestableSyncJob() throws Exception {
        File configDir = new File(CONFIG_DIR);
        File statusDir = new File(STATUS_DIR);
        File clientDir = new File(CLIENT_DIR);
        File serverDir = new File(SERVER_DIR);

        FileUtils.deleteDirectory(configDir);
        FileUtils.deleteDirectory(statusDir);
        FileUtils.deleteDirectory(clientDir);
        FileUtils.deleteDirectory(serverDir);

        clientDir.mkdirs();
        serverDir.mkdirs();

        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("test-thread");
        taskScheduler.setPoolSize(5);
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.initialize();
        TypedProperties properties = new TypedProperties();
        properties
                .setProperty(SyncParameterConstants.DIRECTORY_TRACKER_POLL_FOR_CHANGE_INTERVAL, 1);

        FileSystemPersisterServices services = new FileSystemPersisterServices(STATUS_DIR,
                CONFIG_DIR);
        TransportConnectorFactory factory = new TransportConnectorFactory(services, properties);

        Node targetNode = new Node("0", "server", "file://", "XX");

        SyncConfig config = new SyncConfig();
        config.setClientDir(CLIENT_DIR);
        config.setServerDir(SERVER_DIR);
        config.setDirectorySpec(new DirectorySpec(true, null, null));
        config.setConflictStrategy(ConflictStrategy.REPORT_ERROR);
        config.setFrequency("5");
        config.setSyncDirection(SyncDirection.BIDIRECTIONAL);
        config.setTransportConnectorType("local");

        job = new SyncJob(factory, services, new NoOpServerNodeLocker(), taskScheduler, targetNode,
                config, properties, null, null);
    }

    @After
    public void shutdownTestableSyncJob() {
        taskScheduler.destroy();
    }

    @Test
    public void testCreateFileAtClient() throws Exception {

        Assert.assertFalse(clientFile.exists());
        Assert.assertFalse(serverFile.exists());

        FileUtils.touch(clientFile);

        Assert.assertTrue(clientFile.exists());
        Assert.assertFalse(serverFile.exists());

        job.invoke(true);

        Assert.assertTrue(clientFile.exists());
        Assert.assertTrue(serverFile.exists());

    }

    @Test
    public void testCreateNewFileAtServer() throws Exception {

        Assert.assertFalse(clientFile.exists());
        Assert.assertFalse(serverFile.exists());

        FileUtils.touch(serverFile);

        Assert.assertFalse(clientFile.exists());
        Assert.assertTrue(serverFile.exists());

        job.invoke(true);

        Assert.assertTrue(clientFile.exists());
        Assert.assertTrue(serverFile.exists());

    }

    @Test
    public void testDeleteFileAtServer() throws Exception {

        testCreateFileAtClient();

        Assert.assertTrue(clientFile.exists());
        Assert.assertTrue(serverFile.exists());

        FileUtils.deleteQuietly(serverFile);

        Assert.assertTrue(clientFile.exists());
        Assert.assertFalse(serverFile.exists());

        long ts = System.currentTimeMillis();
        while (clientFile.exists() && System.currentTimeMillis() - ts < 10000) {
            job.invoke(true);
        }

        Assert.assertFalse(clientFile.exists());
        Assert.assertFalse(serverFile.exists());

    }

    @Test
    public void testFileInConflict() throws Exception {
        testCreateFileAtClient();

        Assert.assertTrue(clientFile.exists());
        Assert.assertTrue(serverFile.exists());

        FileUtils.write(clientFile, "This is a test");
        FileUtils.write(serverFile, "of the emergency broadcast system");

        long ts = System.currentTimeMillis();
        while (!job.hasConflict() && System.currentTimeMillis() - ts < 10000) {
            job.invoke(true);
        }
        Assert.assertTrue("Should have received a conflict exception", job.hasConflict());
        Assert.assertEquals(1, job.getFilesInConflict().size());
        Assert.assertEquals("test.txt", job.getFilesInConflict().get(0));

    }

}
