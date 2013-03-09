package org.jumpmind.db.sql;

public class UniqueKeyException extends SqlException {

    private static final long serialVersionUID = 1L;

    public UniqueKeyException() {
        super();
    }

    public UniqueKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public UniqueKeyException(String message) {
        super(message);
    }

    public UniqueKeyException(Throwable cause) {
        super(cause);
    }

    
}
