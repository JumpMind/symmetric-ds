package org.jumpmind.symmetric.io.data.writer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.FileIoResource;
import org.jumpmind.symmetric.io.IoResource;
import org.jumpmind.symmetric.io.MemoryIoResource;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.reader.TextualCsvDataReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileCsvDataWriterTest {

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

        InputStream is = getClass().getResourceAsStream(
        "FileCsvDataWriterTest.1.csv");
        String origCsv = IOUtils.toString(is);
        is.close();

        TextualCsvDataReader reader = new TextualCsvDataReader(origCsv);
        FileCsvDataWriter writer = new FileCsvDataWriter(DIR, threshold, new BatchListener());
        DataProcessor<TextualCsvDataReader, FileCsvDataWriter> processor = new DataProcessor<TextualCsvDataReader, FileCsvDataWriter>(
                reader, writer);
        processor.process();
        
        Assert.assertEquals(1, batchesWritten.size());
        Assert.assertEquals(origCsv, batchesWritten.get(0));
        
        if (threshold > origCsv.length()) {
            Assert.assertTrue(writer.getLastBatch() instanceof MemoryIoResource);
        } else {
            Assert.assertTrue(writer.getLastBatch() instanceof FileIoResource);
        }
        
    }

    class BatchListener implements ICsvDataWriterListener {
        public void start(Batch batch) {
        }

        public void end(Batch batch, IoResource resource) {
            try {
                InputStream is = resource.open();
                batchesWritten.add(IOUtils.toString(is));
                is.close();
            } catch (IOException e) {
                throw new IoException(e);
            }
        }
    }

}
