package org.jumpmind.symmetric.core.sql;

public class DbException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DbException() {
        super();
    }

    public DbException(String message, Throwable cause) {
        super(message, cause);
    }

    public DbException(String message) {
        super(message);
    }

    public DbException(Throwable cause) {
        super(cause);
    }

}
