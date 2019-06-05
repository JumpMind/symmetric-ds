package org.jumpmind.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

public class StreamGobbler extends Thread {

    private InputStream is;
    
    public StreamGobbler(InputStream is) {
        this.is = is;
    }
    
    @Override
    public void run() {
        try {
            IOUtils.copy(is, new StringWriter(), Charset.defaultCharset());
        } catch (IOException e) {
        }
    }
}
