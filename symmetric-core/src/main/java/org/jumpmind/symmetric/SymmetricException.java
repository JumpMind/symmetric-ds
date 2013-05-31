package org.jumpmind.symmetric;

/**
 * This is a {@link RuntimeException} that supports using the SymmetricDS
 * {@link Message} infrastructure
 */
public class SymmetricException extends RuntimeException {

    private static final long serialVersionUID = -3111453874504638368L;

    public SymmetricException() {
        super();
    }

    public SymmetricException(Throwable cause) {
        super(cause);
    }

    public SymmetricException(String message, Object... args) {
        super(String.format(message, args));
    }

    public SymmetricException(String message, Throwable cause) {
        super(message, cause);
    }

    public SymmetricException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }
    
    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }


}