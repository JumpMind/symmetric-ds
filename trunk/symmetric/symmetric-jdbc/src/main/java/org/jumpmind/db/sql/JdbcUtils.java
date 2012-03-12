package org.jumpmind.db.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

abstract public class JdbcUtils {
    
    private static Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T getObjectFromResultSet(ResultSet rs, Class<T> clazz) throws SQLException {
        T result;
        if (Date.class.isAssignableFrom(clazz)) {
            result = (T) rs.getTimestamp(1);
        } else if (String.class.isAssignableFrom(clazz)) {
            result = (T) rs.getString(1);
        } else {
            result = (T) rs.getObject(1);
        }
        return result;
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
    
    protected static int verifyArgType(int argType) {
        if (argType == -101 || argType == Types.OTHER) {
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
    
    public static NativeJdbcExtractor getNativeJdbcExtractory () {
        try {
            return (NativeJdbcExtractor) Class
                    .forName(
                            System.getProperty("db.native.extractor",
                                    "org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor"))
                    .newInstance();
        } catch (Exception ex) {
            log.error("The native jdbc extractor has not been configured.  Defaulting to the common basic datasource extractor.", ex);
            return new CommonsDbcpNativeJdbcExtractor();
        }
    }

}
