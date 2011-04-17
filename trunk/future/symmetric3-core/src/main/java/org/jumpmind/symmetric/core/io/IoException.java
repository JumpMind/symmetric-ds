package org.jumpmind.symmetric.core.io;

import java.io.IOException;

public class IoException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IoException(IOException ex) {
        super(ex);
    }
    
    public IoException(String msg) {
        super(msg);
    }
}
