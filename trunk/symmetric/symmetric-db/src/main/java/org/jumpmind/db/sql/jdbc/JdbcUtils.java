package org.jumpmind.db.sql.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.jumpmind.db.model.TypeMap;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobHandler;

abstract public class JdbcUtils {

    private JdbcUtils() {
    }
    
    public static void setValues(PreparedStatement ps, Object[] args, int[] argTypes,
            LobHandler lobHandler) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            Object arg = args[i - 1];
            int argType = argTypes != null && argTypes.length > i ? argTypes[i - 1] : SqlTypeValue.TYPE_UNKNOWN;
            if (argType == Types.BLOB && lobHandler != null) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            } else {
                StatementCreatorUtils.setParameterValue(ps, i, argType, arg);
            }
        }
    }

    public static void setValues(PreparedStatement ps, Object[] args) throws SQLException {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                doSetValue(ps, i + 1, arg);
            }
        }
    }
    
    /**
     * Set the value for prepared statements specified parameter index using the
     * passed in value. This method can be overridden by sub-classes if needed.
     * 
     * @param ps
     *            the PreparedStatement
     * @param parameterPosition
     *            index of the parameter position
     * @param argValue
     *            the value to set
     * @throws SQLException
     */
    public static void doSetValue(PreparedStatement ps, int parameterPosition, Object argValue)
            throws SQLException {
        StatementCreatorUtils.setParameterValue(ps, parameterPosition, SqlTypeValue.TYPE_UNKNOWN, argValue);
    }

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

    /**
     * Determines whether the system supports the Java 1.4 batch result codes.
     * 
     * @return <code>true</code> if SUCCESS_NO_INFO and EXECUTE_FAILED are
     *         available in the {@link java.sql.Statement} class
     */
    public static boolean supportsJava14BatchResultCodes() {
        try {
            return (Statement.class.getField("SUCCESS_NO_INFO") != null)
                    && (Statement.class.getField("EXECUTE_FAILED") != null);
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Returns the logging message corresponding to the given result code of a
     * batch message. Note that these code values are only available in JDBC 3
     * and newer (see {@link java.sql.Statement} for details).
     * 
     * @param tableName
     *            The name of the table that the batch update/insert was
     *            performed on
     * @param rowIdx
     *            The index of the row within the batch for which this code is
     * @param resultCode
     *            The code
     * @return The string message or <code>null</code> if the code does not
     *         indicate an error
     */
    public static String getBatchResultMessage(String tableName, int rowIdx, int resultCode) {
        if (resultCode < 0) {
            try {
                if (resultCode == Statement.class.getField("SUCCESS_NO_INFO").getInt(null)) {
                    return null;
                } else if (resultCode == Statement.class.getField("EXECUTE_FAILED").getInt(null)) {
                    return "The batch insertion of row " + rowIdx + " into table " + tableName
                            + " failed but the driver is able to continue processing";
                } else {
                    return "The batch insertion of row " + rowIdx + " into table " + tableName
                            + " returned an undefined status value " + resultCode;
                }
            } catch (Exception ex) {
                throw new UnsupportedOperationException("The batch result codes are not supported");
            }
        } else {
            return null;
        }
    }

}
