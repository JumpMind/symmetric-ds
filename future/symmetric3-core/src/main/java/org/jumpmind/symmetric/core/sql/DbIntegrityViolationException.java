package org.jumpmind.symmetric.core.sql;

public class DbIntegrityViolationException extends DbException {

    private static final long serialVersionUID = 1L;

    public DbIntegrityViolationException() {
        super();
    }

    public DbIntegrityViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DbIntegrityViolationException(String message) {
        super(message);
    }

    public DbIntegrityViolationException(Throwable cause) {
        super(cause);
    }

}
