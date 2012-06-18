package org.jumpmind.exception;

import java.io.IOException;

/**
 * Wraps an {@link IOException} with a runtime version
 */
public class IoException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IoException() {     
    }
    
    public IoException(IOException e) {
        super(e);
    }
    
    public IoException(String msg, Object... args) {
        super(String.format(msg, args));
    }

}
