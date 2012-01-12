package org.jumpmind.db.sql;

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
