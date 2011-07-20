/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

/**
 * 
 */
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