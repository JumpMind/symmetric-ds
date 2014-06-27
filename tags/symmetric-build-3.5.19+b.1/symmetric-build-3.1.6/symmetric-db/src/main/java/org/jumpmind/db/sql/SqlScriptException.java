package org.jumpmind.db.sql;

public class SqlScriptException extends SqlException {

    private static final long serialVersionUID = 1L;
    
    private int lineNumber;

    public SqlScriptException(Throwable cause, int lineNumber) {
        super("Script failed at line " + lineNumber, cause);
        this.lineNumber = lineNumber;
    }
    
    public int getLineNumber() {
        return lineNumber;
    }


}
