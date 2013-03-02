package org.jumpmind.db.sql;

import java.sql.SQLException;

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
    
    public int getErrorCode() {
        Throwable rootCause = getRootCause();
        if (rootCause instanceof SQLException) {
            return ((SQLException)rootCause).getErrorCode();
        } else {
            return -1;
        }
    }

    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        
        if (rootCause != null) {
            rootCause = this;
        }
        return rootCause;
    }
    
    public String getRootMessage() {
        return getRootCause().getMessage();
    }

}
