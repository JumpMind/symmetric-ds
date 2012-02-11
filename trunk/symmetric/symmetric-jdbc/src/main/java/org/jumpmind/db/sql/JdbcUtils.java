package org.jumpmind.db.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

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
            int argType = argTypes != null && argTypes.length >= i ? argTypes[i - 1] : SqlTypeValue.TYPE_UNKNOWN;
            if (argType == Types.BLOB && lobHandler != null && arg instanceof byte[]) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.BLOB && lobHandler != null && arg instanceof String) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, arg.toString().getBytes());
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            } else {
                StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(argType), arg);
            }
        }
    }
    
    public static void main(String[] args) {
        System.out.println(new byte[] {'0'}.getClass().equals(byte[].class));
    }
    
    protected static int verifyArgType(int argType) {
        if (argType == -101) {
            return SqlTypeValue.TYPE_UNKNOWN;
        } else {
            return argType;
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

}
