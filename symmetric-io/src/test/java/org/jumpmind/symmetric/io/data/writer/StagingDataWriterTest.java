package org.jumpmind.symmetric.io.data.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.StagedResource;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StagingDataWriterTest {

    static final File DIR = new File("target/tmp");

    List<String> batchesWritten = new ArrayList<String>();

    @BeforeClass
    public static void setup() throws Exception {
        FileUtils.deleteDirectory(DIR);
    }

    @Before
    public void clearBatches() {
        batchesWritten.clear();
    }

    @Test
    public void testReadThenWriteToFile() throws Exception {
        readThenWrite(0);
    }

    @Test
    public void testReadThenWriteToMemory() throws Exception {
        readThenWrite(10000000);
    }

    public void readThenWrite(long threshold) throws Exception {

        InputStream is = getClass().getResourceAsStream("FileCsvDataWriterTest.1.csv");
        String origCsv = IOUtils.toString(is);
        is.close();

        StagingManager stagingManager = new StagingManager(threshold, 0l, DIR.getAbsolutePath());
        ProtocolDataReader reader = new ProtocolDataReader(origCsv);
        StagingDataWriter writer = new StagingDataWriter("aaa", "test", stagingManager, new BatchListener());
        DataProcessor processor = new DataProcessor(reader, writer);
        processor.process(new DataContext());

        Assert.assertEquals(1, batchesWritten.size());
        Assert.assertEquals(origCsv, batchesWritten.get(0));

        StagedResource resource = (StagedResource) stagingManager.find("test", "aaa", 1);
        Assert.assertNotNull(resource);
        if (threshold > origCsv.length()) {
            Assert.assertFalse(resource.getFile().exists());
        } else {
            Assert.assertTrue(resource.getFile().exists());
        }
        
        resource.delete();
        Assert.assertFalse(resource.getFile().exists());

    }

    class BatchListener implements IProtocolDataWriterListener {
        public void start(DataContext ctx, Batch batch) {
        }

        public void end(DataContext ctx, Batch batch, IStagedResource resource) {
            try {
                BufferedReader reader = resource.getReader();
                batchesWritten.add(IOUtils.toString(reader));
                resource.close();
            } catch (IOException e) {
                throw new IoException(e);
            }
        }
    }

}
