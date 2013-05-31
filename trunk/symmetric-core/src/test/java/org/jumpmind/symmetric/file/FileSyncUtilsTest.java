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
package org.jumpmind.symmetric.file;

import org.junit.Assert;
import org.junit.Test;

public class FileSyncUtilsTest {

    @Test
    public void testGetRelativePathsUnix() {
        Assert.assertEquals("stuff/xyz.dat", FileSyncUtils.getRelativePath("/var/data/stuff/xyz.dat", "/var/data/", "/"));
        Assert.assertEquals("../../b/c", FileSyncUtils.getRelativePath("/a/b/c", "/a/x/y/", "/"));
        Assert.assertEquals("../../b/c", FileSyncUtils.getRelativePath("/m/n/o/a/b/c", "/m/n/o/a/x/y/", "/"));
    }

    @Test
    public void testGetRelativePathFileToFile() {
        String target = "C:\\Windows\\Boot\\Fonts\\chs_boot.ttf";
        String base = "C:\\Windows\\Speech\\Common\\sapisvr.exe";

        String relPath = FileSyncUtils.getRelativePath(target, base, "\\");
        Assert.assertEquals("..\\..\\Boot\\Fonts\\chs_boot.ttf", relPath);
    }

    @Test
    public void testGetRelativePathDirectoryToFile() {
        String target = "C:\\Windows\\Boot\\Fonts\\chs_boot.ttf";
        String base = "C:\\Windows\\Speech\\Common\\";

        String relPath = FileSyncUtils.getRelativePath(target, base, "\\");
        Assert.assertEquals("..\\..\\Boot\\Fonts\\chs_boot.ttf", relPath);
    }

    @Test
    public void testGetRelativePathFileToDirectory() {
        String target = "C:\\Windows\\Boot\\Fonts";
        String base = "C:\\Windows\\Speech\\Common\\foo.txt";

        String relPath = FileSyncUtils.getRelativePath(target, base, "\\");
        Assert.assertEquals("..\\..\\Boot\\Fonts", relPath);
    }
    
    @Test
    public void testGetRelativePathDirectoryToDirectory() {
        String target = "C:\\Windows\\Boot\\";
        String base = "C:\\Windows\\Speech\\Common\\";
        String expected = "..\\..\\Boot";

        String relPath = FileSyncUtils.getRelativePath(target, base, "\\");
        Assert.assertEquals(expected, relPath);
    }

    @Test
    public void testGetRelativePathDifferentDriveLetters() {
        String target = "D:\\sources\\recovery\\RecEnv.exe";
        String base = "C:\\Java\\workspace\\AcceptanceTests\\Standard test data\\geo\\";

        try {
            FileSyncUtils.getRelativePath(target, base, "\\");
            Assert.fail();

        } catch (PathResolutionException ex) {
            // expected exception
        }
    }
}
