package org.jumpmind.symmetric.model;

import org.jumpmind.symmetric.util.ICoded;

public enum DataEventType implements ICoded {

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
     * An event that indicates that table validation needs to be done.
     */
    VALIDATE("V"),

    /**
     * An event that indicates that a table needs to be reloaded.
     */
    RELOAD("R"),

    /**
     * An event that indicates that the data payload has a sql statement that needs to be executed.
     * This is more of a remote control feature (that would have been very handy in past lives).
     */
    SQL("S");

    private String code;

    DataEventType(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }
    
    public static DataEventType getEventType(String s) {
        if (s.equals("I")) {
            return DataEventType.INSERT;
        } else if (s.equals("U")) {
            return DataEventType.UPDATE;
        } else if (s.equals("D")) {
            return DataEventType.DELETE;
        }
        return null;
    }    
}
