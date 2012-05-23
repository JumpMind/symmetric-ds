package org.jumpmind.exception;

public class InterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InterruptedException(String msg) {
        super(msg);
    }
    
    public InterruptedException(java.lang.InterruptedException ex) {
        super(ex);
    }

}
