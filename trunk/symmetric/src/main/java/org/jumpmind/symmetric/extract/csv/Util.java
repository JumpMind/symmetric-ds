package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Util {

    static final Log logger = LogFactory.getLog(Util.class);

    public static void write(BufferedWriter writer, String ... data) throws IOException {
        StringBuilder buffer = new StringBuilder();
        for (String string : data) {
            buffer.append(string);              
        }
        
        writer.write(buffer.toString()); 
        if (logger.isDebugEnabled()) {
            logger.debug("writing: " + buffer);
        }
    }
}
