package org.jumpmind.exception;

import java.security.GeneralSecurityException;

public class SecurityException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    public SecurityException(java.lang.SecurityException ex) {
        super(ex);
    }
    
    public SecurityException(GeneralSecurityException ex) {
        super(ex);
    }

}
