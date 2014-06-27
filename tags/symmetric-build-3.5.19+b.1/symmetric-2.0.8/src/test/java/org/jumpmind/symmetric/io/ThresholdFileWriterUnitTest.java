package org.jumpmind.symmetric.io;

import java.io.File;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class ThresholdFileWriterUnitTest {

    final String TEST_STR = "The quick brown fox jumped over the lazy dog";

    @Test
    public void testNoWriteToFile() throws Exception {
        File file = getTestFile();
        ThresholdFileWriter writer = new ThresholdFileWriter(TEST_STR.length() + 1, file);
        writer.write(TEST_STR);
        Assert.assertFalse(file.exists());
        Assert.assertEquals(TEST_STR, IOUtils.toString(writer.getReader()));
        file.delete();
    }

    @Test
    public void testWriteToFile() throws Exception {
        File file = getTestFile();
        ThresholdFileWriter writer = new ThresholdFileWriter(TEST_STR.length() - 1, file);
        writer.write(TEST_STR);
        Assert.assertTrue(file.exists());
        Assert.assertEquals(TEST_STR, IOUtils.toString(writer.getReader()));
        file.delete();
    }

    private File getTestFile() {
        File file = new File("target/test/buffered.file.writer.tst");
        file.getParentFile().mkdirs();
        file.delete();
        return file;
    }
}
