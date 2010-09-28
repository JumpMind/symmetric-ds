package org.jumpmind.symmetric;

import java.io.File;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

public class UpdateSourceHeaders {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Collection<File> files = (Collection<File>) FileUtils.listFiles(new File("src/main"),
                new String[] { "java" }, true);
        for (File file : files) {
            updateHeader(file, FileUtils.readFileToString(new File("HEADER.txt")));
        }

    }
    
    private static void updateHeader(File file, String newHeaderTxt) throws Exception {
        String fileContents = FileUtils.readFileToString(file).trim();        
        if (fileContents.startsWith("/*")) {
            String oldHeader = fileContents.substring(0, fileContents.indexOf("*/"));
            StringBuilder newContents = new StringBuilder(fileContents.substring(fileContents.indexOf("*/") + 2));
            Pattern pattern = Pattern.compile("Copyright.*");
            Matcher matcher = pattern.matcher(oldHeader);
            while (matcher.find()) {
                String group = matcher.group();
                String author = group.substring("Copyright (C) ".length());
                insertAuthor(file.getName(), newContents, author);
            }
            
            FileUtils.writeStringToFile(file, newHeaderTxt+"\n"+newContents.toString());
        }
    }
    
    private static void insertAuthor(String fileName, StringBuilder contents, String author) {
        int classBeginIndex = contents.indexOf("class " + fileName.substring(0, fileName.length()-5));
        if (classBeginIndex < 0) {
            classBeginIndex = contents.indexOf("interface " + fileName.substring(0, fileName.length()-5));            
        }               
        if (classBeginIndex < 0) {
            classBeginIndex = contents.indexOf("enum " + fileName.substring(0, fileName.length()-5));            
        }                   
        
        if (classBeginIndex < 0) {
            throw new IllegalStateException("Could not find class start: " + contents);
        } else {
            if (contents.substring(0, classBeginIndex).indexOf("final") > 0) {
                classBeginIndex = contents.substring(0, classBeginIndex).indexOf("final");
            }
            if (contents.substring(0, classBeginIndex).indexOf("public") > 0) {
                classBeginIndex = contents.substring(0, classBeginIndex).indexOf("public");
            }
            if (contents.substring(0, classBeginIndex).indexOf("abstract") > 0) {
                classBeginIndex = contents.substring(0, classBeginIndex).indexOf("abstract");
            }
            
            int insertPoint = contents.substring(0, classBeginIndex+1).indexOf("*/")-1;
            if (insertPoint < 0) {
                contents.insert(classBeginIndex, " */\n");
                contents.insert(classBeginIndex, " * @author " + author + "\n");
                contents.insert(classBeginIndex, "/**\n");                
            } else {
                contents.insert(insertPoint, " * @author " + author + "\n");
                contents.insert(insertPoint, " *\n");                
            }
        }
    }
}
