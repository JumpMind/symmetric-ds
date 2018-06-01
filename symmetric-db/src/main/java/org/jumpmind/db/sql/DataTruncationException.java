package org.jumpmind.db.sql;

public class DataTruncationException extends SqlException {

	 private static final long serialVersionUID = 1L;

	    public DataTruncationException() {
	        super();
	    }

	    public DataTruncationException(String message, Throwable cause) {
	        super(message, cause);
	    }

	    public DataTruncationException(String message) {
	        super(message);
	    }

	    public DataTruncationException(Throwable cause) {
	        super(cause);
	    }

}
