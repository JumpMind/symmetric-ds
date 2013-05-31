package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.sql.DmlStatement.DmlType;

/**
 * 
 */
public enum DataEventType {

    /**
     * Insert DML type.
     */
    INSERT("I"),

    /**
     * Update DML type.
     */
    UPDATE("U"),

    /**
     * Delete DML type.
     */
    DELETE("D"),

    /**
     * An event that indicates that a table needs to be reloaded.
     */
    RELOAD("R"),

    /**
     * An event that indicates that the data payload has a sql statement that needs to be executed. This is more of a
     * remote control feature (that would have been very handy in past lives).
     */
    SQL("S"),

    /**
     * An event that indicates that the data payload is a table creation.
     */
    CREATE("C"),

    /**
     * An event the indicates that the data payload is going to be a Java bean shell script that is to be run at the
     * client.
     */
    BSH("B"),
    
    
    UNKNOWN("U");

    private String code;

    DataEventType(String code) {
        this.code = code;
    }
    
    public boolean isDml() {
        return this == INSERT || this == DELETE || this == UPDATE;
    }

    public String getCode() {
        return this.code;
    }
    
    public DmlType getDmlType() {
        switch (this) {
        case INSERT:
            return DmlType.INSERT;
        case UPDATE:
            return DmlType.UPDATE;
        case DELETE:
            return DmlType.DELETE;
        default:
            return DmlType.UNKNOWN;
        }
    }

    public static DataEventType getEventType(String s) {
        if (s.equals(INSERT.getCode())) {
            return INSERT;
        } else if (s.equals(UPDATE.getCode())) {
            return UPDATE;
        } else if (s.equals(DELETE.getCode())) {
            return DELETE;
        } else if (s.equals(RELOAD.getCode())) {
            return RELOAD;
        } else if (s.equals(SQL.getCode())) {
            return SQL;
        } else if (s.equals(CREATE.getCode())) {
            return CREATE;
        } else if (s.equals(RELOAD.getCode())) {
            return RELOAD;
        } else if (s.equals(BSH.getCode())) {
            return BSH;
        } else {
            throw new IllegalStateException(String.format("Invalid data event type of %s", s));
        }
    }
}