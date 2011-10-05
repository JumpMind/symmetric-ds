package org.jumpmind.symmetric.core.db;

public class DbDialectNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    String dbDialectId;

    public DbDialectNotFoundException(String dbDialectId) {
        super("Could not find a matching platform for the database id of " + dbDialectId);
        this.dbDialectId = dbDialectId;
    }

    public String getDbDialectId() {
        return dbDialectId;
    }

}
