package org.jumpmind.exception;

public class InterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InterruptedException(java.lang.InterruptedException ex) {
        super(ex);
    }

}
