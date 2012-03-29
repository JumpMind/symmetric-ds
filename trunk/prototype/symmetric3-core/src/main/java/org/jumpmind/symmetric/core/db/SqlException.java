package org.jumpmind.symmetric.core.db;

public class SqlException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SqlException() {
        super();
    }

    public SqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlException(String message) {
        super(message);
    }

    public SqlException(Throwable cause) {
        super(cause);
    }

}
