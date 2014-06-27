package org.jumpmind.db.platform;

import java.sql.Types;

import org.jumpmind.db.model.TypeMap;

abstract public class PlatformUtils {

    /**
     * Determines whether the system supports the Java 1.4 JDBC Types, DATALINK
     * and BOOLEAN.
     * 
     * @return <code>true</code> if BOOLEAN and DATALINK are available
     */
    public static boolean supportsJava14JdbcTypes() {
        try {
            return (Types.class.getField(TypeMap.BOOLEAN) != null)
                    && (Types.class.getField(TypeMap.DATALINK) != null);
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Determines the type code for the BOOLEAN JDBC type.
     * 
     * @return The type code
     * @throws UnsupportedOperationException
     *             If the BOOLEAN type is not supported
     */
    public static int determineBooleanTypeCode() throws UnsupportedOperationException {
        try {
            return Types.class.getField(TypeMap.BOOLEAN).getInt(null);
        } catch (Exception ex) {
            throw new UnsupportedOperationException("The jdbc type BOOLEAN is not supported");
        }
    }

    /**
     * Determines the type code for the DATALINK JDBC type.
     * 
     * @return The type code
     * @throws UnsupportedOperationException
     *             If the DATALINK type is not supported
     */
    public static int determineDatalinkTypeCode() throws UnsupportedOperationException {
        try {
            return Types.class.getField(TypeMap.DATALINK).getInt(null);
        } catch (Exception ex) {
            throw new UnsupportedOperationException("The jdbc type DATALINK is not supported");
        }
    }

    
}
