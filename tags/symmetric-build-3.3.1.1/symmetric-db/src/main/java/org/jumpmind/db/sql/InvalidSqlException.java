package org.jumpmind.db.sql;

/**
 * This exception indicates that the SQL statement was invalid for some reason.
 */
public class InvalidSqlException extends SqlException {

    private static final long serialVersionUID = 1L;

    public InvalidSqlException() {
        super();
    }

    public InvalidSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSqlException(String message, Object ... args) {
        super(String.format(message, args));
    }

    public InvalidSqlException(Throwable cause) {
        super(cause);
    }

    
}
