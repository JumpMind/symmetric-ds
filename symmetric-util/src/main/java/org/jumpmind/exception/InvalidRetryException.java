package org.jumpmind.exception;

public class InvalidRetryException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    public InvalidRetryException() {
        super("Received retry when stream.to.file.enabled=false. Please update stream.to.file.enabled on each node to the same value.");
    }
    
    public InvalidRetryException(String message) {
        super(message);
    }

}