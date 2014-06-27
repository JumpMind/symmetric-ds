package org.jumpmind.db.sql;

public class ConcurrencySqlException extends SqlException {

    private static final long serialVersionUID = 1L;

    public ConcurrencySqlException() {
        super();
    }

    public ConcurrencySqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConcurrencySqlException(String message) {
        super(message);
    }

    public ConcurrencySqlException(Throwable cause) {
        super(cause);
    }

}
