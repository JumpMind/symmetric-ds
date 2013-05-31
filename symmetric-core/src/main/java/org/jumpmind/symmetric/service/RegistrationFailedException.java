package org.jumpmind.symmetric.service;

/**
 * An exception that indicates a failed registration attempt.
 */
public class RegistrationFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RegistrationFailedException(String msg) {
        super(msg);
    }
}