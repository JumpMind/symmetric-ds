/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.util;

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
        emptyFile(TEST_JAR_DIR + "/subdir/file2.txt");
        emptyFile(TEST_JAR_DIR + "/file2.txt");
        emptyFile("target/file1.txt");
        
        JarBuilder jarFile = new JarBuilder(new File("target"), outputFile, new File[] { new File(TEST_JAR_DIR), new File("target/file1.txt") });
        jarFile.build();
        
        Assert.assertTrue(outputFile.exists());
        
        JarFile finalJar = new JarFile(outputFile);
        Assert.assertNotNull(finalJar.getEntry("test.jar.dir/subdir/file2.txt"));
        Assert.assertNotNull(finalJar.getEntry("test.jar.dir/file2.txt"));
        Assert.assertNotNull(finalJar.getEntry("test.jar.dir/file2.txt"));
        Assert.assertNotNull(finalJar.getEntry("file1.txt"));
        Assert.assertNotNull(finalJar.getEntry("file1.txt"));
    }

    private void mkdir(String dir) {
        new File(dir).mkdirs();
    }

    private void emptyFile(String file) throws IOException {
        new File(file).createNewFile();
    }
}
