package org.jumpmind.symmetric.io.data.stage;

import java.io.BufferedReader;
import java.io.File;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.io.stage.ThresholdFileWriter;
import org.junit.Assert;
import org.junit.Test;

public class ThresholdFileWriterTest {

    final String TEST_STR = "The quick brown fox jumped over the lazy dog";

    @Test
    public void testNoWriteToFile() throws Exception {
        File file = getTestFile();
        ThresholdFileWriter writer = new ThresholdFileWriter(TEST_STR.length() + 1, new StringBuilder(), file);
        writer.write(TEST_STR);

        // File does not exist since we did not meet the threshold
        Assert.assertFalse(file.exists());
        
        // Check the contents of the buffer (not yet written to a file)
        Assert.assertEquals(TEST_STR, IOUtils.toString(writer.getReader()));
    }

    @Test
    public void testWriteToFile() throws Exception {
        File file = getTestFile();
        Assert.assertFalse(file.exists());

        ThresholdFileWriter writer = new ThresholdFileWriter( TEST_STR.length() - 1, new StringBuilder(), file);
        writer.write(TEST_STR);
        writer.close();

        // The write string exceeded the threshold so the writer should have created/written to the file
        Assert.assertTrue(file.exists());
        
        BufferedReader reader = writer.getReader();
        Assert.assertEquals(TEST_STR, IOUtils.toString(reader));
        reader.close();

        Assert.assertTrue(file.delete());
    }

    private File getTestFile() {
        File file = new File("target/test/buffered.file.writer.tst");
        file.getParentFile().mkdirs();
        
        // Make sure the file doesn't already exist
        file.delete();
        return file;
    }
}