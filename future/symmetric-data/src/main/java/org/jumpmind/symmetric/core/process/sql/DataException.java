package org.jumpmind.symmetric.core.process.sql;

public class DataException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DataException() {
        super();
    }

    public DataException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataException(String message) {
        super(message);
    }

    public DataException(Throwable cause) {
        super(cause);
    }

}
