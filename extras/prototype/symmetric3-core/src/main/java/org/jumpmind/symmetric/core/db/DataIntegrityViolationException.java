package org.jumpmind.symmetric.core.db;

public class DataIntegrityViolationException extends SqlException {

    private static final long serialVersionUID = 1L;

    public DataIntegrityViolationException() {
        super();
    }

    public DataIntegrityViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataIntegrityViolationException(String message) {
        super(message);
    }

    public DataIntegrityViolationException(Throwable cause) {
        super(cause);
    }

}
