package org.jumpmind.db.sql;

public class BulkSqlException extends SqlException {

    private static final long serialVersionUID = 1L;

    private int[] errors;

    public BulkSqlException(int[] errors) {
        super();
        this.errors = errors;
    }

    public BulkSqlException(int[] errors, Exception ex) {
        super(ex);
        this.errors = errors;
    }

    public int[] getErrors() {
        return errors;
    }

}
