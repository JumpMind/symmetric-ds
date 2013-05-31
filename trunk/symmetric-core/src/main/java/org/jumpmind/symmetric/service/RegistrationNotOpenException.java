package org.jumpmind.symmetric.service;

/**
 * Throw this exception to indicate that registration is not allowed or is not
 * open for a specific node.
 */
public class RegistrationNotOpenException extends RuntimeException {

    private static final long serialVersionUID = 7736136383224998646L;
    
    public RegistrationNotOpenException() {
     
    }
    
    public RegistrationNotOpenException(String msg) {
        super(msg);
    }

}