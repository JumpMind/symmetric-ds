import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;


public class Test {
    public static void main(String[] args) {
        File dir = new File(System.getProperty("user.dir")).getParentFile();
        Collection<File> files = FileUtils.listFiles(dir, new String[] {"java"}, true);
        for (File file : files) {
            System.out.println("Found file: " + file.getName());
        }
    }
}
