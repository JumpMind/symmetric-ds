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
