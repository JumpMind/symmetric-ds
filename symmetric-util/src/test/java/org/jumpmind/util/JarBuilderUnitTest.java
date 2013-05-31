package org.jumpmind.util;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class JarBuilderUnitTest {

    @Test
    public void testJarCreation() throws Exception {
        final String TEST_JAR_DIR = "target/test.jar.dir";
        File outputFile = new File("target/test.jar");
        outputFile.delete();
        Assert.assertFalse(outputFile.exists());
        
        FileUtils.deleteDirectory(new File(TEST_JAR_DIR));
                
        mkdir(TEST_JAR_DIR + "/subdir");
        mkdir(TEST_JAR_DIR + "/META-INF");
        emptyFile(TEST_JAR_DIR + "/META-INF/MANIFEST.MF");
        emptyFile(TEST_JAR_DIR + "/subdir/file2.txt");
        emptyFile(TEST_JAR_DIR + "/file2.txt");
        emptyFile("target/file1.txt");
        emptyFile(TEST_JAR_DIR + "/file3.txt");
        
        JarBuilder jarFile = new JarBuilder(new File(TEST_JAR_DIR), outputFile, new File[] { new File(TEST_JAR_DIR), new File("target/file1.txt") }, "3.0.0");
        jarFile.build();
        
        Assert.assertTrue(outputFile.exists());
        
        JarFile finalJar = new JarFile(outputFile);
        Assert.assertNotNull(finalJar.getEntry("subdir/file2.txt"));
        Assert.assertNotNull(finalJar.getEntry("file2.txt"));
        Assert.assertNull(finalJar.getEntry("target/test.jar.dir"));
        Assert.assertNull(finalJar.getEntry("test.jar.dir"));
        Assert.assertNull(finalJar.getEntry("file1.txt"));
        Assert.assertNotNull(finalJar.getEntry("file3.txt"));
        finalJar.close();
    }

    private void mkdir(String dir) {
        new File(dir).mkdirs();
    }

    private void emptyFile(String file) throws IOException {
        new File(file).createNewFile();
    }
}