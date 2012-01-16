package org.jumpmind.symmetric.tool;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;

public class RefactorTool {

    public static void main(String[] args) throws Exception {
        File dir = new File(System.getProperty("user.dir")).getParentFile();
        Collection<File> files = FileUtils.listFiles(dir, new String[] { "java" }, true);
        for (File file : files) {
            if (file.getAbsolutePath().contains("/symmetric-")) {
                System.out.println("Refactoring file: " + file.getName());
                StringBuilder contents = new StringBuilder(FileUtils.readFileToString(file));
                if (refactor(contents)) {
                    FileUtils.write(file, contents.toString());
                }
            }
        }
    }

    protected static boolean refactor(StringBuilder contents) {
        String[] lines = contents.toString().split("\n");
        contents.setLength(0);
        int logmode = 0;
        for (String line : lines) {
            if (line.contains("log.") || line.contains("logger.")) {
                logmode = 4;
            }

            if (!line.contains("String.format") && logmode > 0) {
                line = line.replace("%s", "{}");
                line = line.replace("%d", "{}");
            }

            contents.append(line);
            contents.append("\n");

        }
        return true;
    }
}