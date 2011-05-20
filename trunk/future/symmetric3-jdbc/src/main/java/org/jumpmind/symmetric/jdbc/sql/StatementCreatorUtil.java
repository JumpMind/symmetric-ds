package org.jumpmind.symmetric.jdbc.sql;

/*
 * Copyright 2002-2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;

public abstract class StatementCreatorUtil {

    private static final Log log = LogFactory.getLog(StatementCreatorUtil.class);

    private static final int UNKNOWN_TYPE = Integer.MIN_VALUE;

    private static Map<Class<?>, Integer> javaTypeToSqlTypeMap = new HashMap<Class<?>, Integer>(32);

    static {
        /*
         * JDBC 3.0 only - not compatible with e.g. MySQL at present
         * javaTypeToSqlTypeMap.put(boolean.class, new Integer(Types.BOOLEAN));
         * javaTypeToSqlTypeMap.put(Boolean.class, new Integer(Types.BOOLEAN));
         */
        javaTypeToSqlTypeMap.put(byte.class, Types.TINYINT);
        javaTypeToSqlTypeMap.put(Byte.class, Types.TINYINT);
        javaTypeToSqlTypeMap.put(short.class, Types.SMALLINT);
        javaTypeToSqlTypeMap.put(Short.class, Types.SMALLINT);
        javaTypeToSqlTypeMap.put(int.class, Types.INTEGER);
        javaTypeToSqlTypeMap.put(Integer.class, Types.INTEGER);
        javaTypeToSqlTypeMap.put(long.class, Types.BIGINT);
        javaTypeToSqlTypeMap.put(Long.class, Types.BIGINT);
        javaTypeToSqlTypeMap.put(BigInteger.class, Types.BIGINT);
        javaTypeToSqlTypeMap.put(float.class, Types.FLOAT);
        javaTypeToSqlTypeMap.put(Float.class, Types.FLOAT);
        javaTypeToSqlTypeMap.put(double.class, Types.DOUBLE);
        javaTypeToSqlTypeMap.put(Double.class, Types.DOUBLE);
        javaTypeToSqlTypeMap.put(BigDecimal.class, Types.DECIMAL);
        javaTypeToSqlTypeMap.put(java.sql.Date.class, Types.DATE);
        javaTypeToSqlTypeMap.put(java.sql.Time.class, Types.TIME);
        javaTypeToSqlTypeMap.put(java.sql.Timestamp.class, Types.TIMESTAMP);
        javaTypeToSqlTypeMap.put(Blob.class, Types.BLOB);
        javaTypeToSqlTypeMap.put(Clob.class, Types.CLOB);
    }

    public static void setValues(PreparedStatement ps, Object[] args, int[] argTypes,
            ILobHandler lobHandler) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            Object arg = args[i - 1];
            int argType = argTypes[i - 1];
            if (argType == Types.BLOB && lobHandler != null) {
                lobHandler.setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.setClobAsString(ps, i, (String) arg);
            } else {
                setParameterValue(ps, i, argType, arg);
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
        setParameterValue(ps, parameterPosition, UNKNOWN_TYPE, argValue);
    }

    /**
     * Derive a default SQL type from the given Java type.
     * 
     * @param javaType
     *            the Java type to translate
     * @return the corresponding SQL type, or <code>null</code> if none found
     */
    public static int javaTypeToSqlParameterType(Class<?> javaType) {
        Integer sqlType = javaTypeToSqlTypeMap.get(javaType);
        if (sqlType != null) {
            return sqlType;
        }
        if (Number.class.isAssignableFrom(javaType)) {
            return Types.NUMERIC;
        }
        if (isStringValue(javaType)) {
            return Types.VARCHAR;
        }
        if (isDateValue(javaType) || Calendar.class.isAssignableFrom(javaType)) {
            return Types.TIMESTAMP;
        }
        return UNKNOWN_TYPE;
    }

    /**
     * Set the value for a parameter. The method used is based on the SQL type
     * of the parameter and we can handle complex types like arrays and LOBs.
     * 
     * @param ps
     *            the prepared statement or callable statement
     * @param paramIndex
     *            index of the parameter we are setting
     * @param sqlType
     *            the SQL type of the parameter
     * @param inValue
     *            the value to set (plain value or a SqlTypeValue)
     * @throws SQLException
     *             if thrown by PreparedStatement methods
     * @see SqlTypeValue
     */
    public static void setParameterValue(PreparedStatement ps, int paramIndex, int sqlType,
            Object inValue) throws SQLException {

        setParameterValueInternal(ps, paramIndex, sqlType, null, null, inValue);
    }

    /**
     * Set the value for a parameter. The method used is based on the SQL type
     * of the parameter and we can handle complex types like arrays and LOBs.
     * 
     * @param ps
     *            the prepared statement or callable statement
     * @param paramIndex
     *            index of the parameter we are setting
     * @param sqlType
     *            the SQL type of the parameter
     * @param typeName
     *            the type name of the parameter (optional, only used for SQL
     *            NULL and SqlTypeValue)
     * @param inValue
     *            the value to set (plain value or a SqlTypeValue)
     * @throws SQLException
     *             if thrown by PreparedStatement methods
     * @see SqlTypeValue
     */
    public static void setParameterValue(PreparedStatement ps, int paramIndex, int sqlType,
            String typeName, Object inValue) throws SQLException {

        setParameterValueInternal(ps, paramIndex, sqlType, typeName, null, inValue);
    }

    /**
     * Set the value for a parameter. The method used is based on the SQL type
     * of the parameter and we can handle complex types like arrays and LOBs.
     * 
     * @param ps
     *            the prepared statement or callable statement
     * @param paramIndex
     *            index of the parameter we are setting
     * @param sqlType
     *            the SQL type of the parameter
     * @param typeName
     *            the type name of the parameter (optional, only used for SQL
     *            NULL and SqlTypeValue)
     * @param scale
     *            the number of digits after the decimal point (for DECIMAL and
     *            NUMERIC types)
     * @param inValue
     *            the value to set (plain value or a SqlTypeValue)
     * @throws SQLException
     *             if thrown by PreparedStatement methods
     * @see SqlTypeValue
     */
    private static void setParameterValueInternal(PreparedStatement ps, int paramIndex,
            int sqlType, String typeName, Integer scale, Object inValue) throws SQLException {

        String typeNameToUse = typeName;
        int sqlTypeToUse = sqlType;
        Object inValueToUse = inValue;

        if (inValueToUse == null) {
            setNull(ps, paramIndex, sqlTypeToUse, typeNameToUse);
        } else {
            setValue(ps, paramIndex, sqlTypeToUse, typeNameToUse, scale, inValueToUse);
        }
    }

    /**
     * Set the specified PreparedStatement parameter to null, respecting
     * database-specific peculiarities.
     */
    private static void setNull(PreparedStatement ps, int paramIndex, int sqlType, String typeName)
            throws SQLException {

        if (sqlType == UNKNOWN_TYPE) {
            boolean useSetObject = false;
            sqlType = Types.NULL;
            try {
                DatabaseMetaData dbmd = ps.getConnection().getMetaData();
                String databaseProductName = dbmd.getDatabaseProductName();
                String jdbcDriverName = dbmd.getDriverName();
                if (databaseProductName.startsWith("Informix")
                        || jdbcDriverName.startsWith("Microsoft SQL Server")) {
                    useSetObject = true;
                } else if (databaseProductName.startsWith("DB2")
                        || jdbcDriverName.startsWith("jConnect")
                        || jdbcDriverName.startsWith("SQLServer")
                        || jdbcDriverName.startsWith("Apache Derby")) {
                    sqlType = Types.VARCHAR;
                }
            } catch (Throwable ex) {
                log.log(LogLevel.DEBUG, "Could not check database or driver name", ex);
            }
            if (useSetObject) {
                ps.setObject(paramIndex, null);
            } else {
                ps.setNull(paramIndex, sqlType);
            }
        } else if (typeName != null) {
            ps.setNull(paramIndex, sqlType, typeName);
        } else {
            ps.setNull(paramIndex, sqlType);
        }
    }

    private static void setValue(PreparedStatement ps, int paramIndex, int sqlType,
            String typeName, Integer scale, Object inValue) throws SQLException {

        if (sqlType == Types.VARCHAR || sqlType == Types.LONGVARCHAR
                || (sqlType == Types.CLOB && isStringValue(inValue.getClass()))) {
            ps.setString(paramIndex, inValue.toString());
        } else if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
            if (inValue instanceof BigDecimal) {
                ps.setBigDecimal(paramIndex, (BigDecimal) inValue);
            } else if (scale != null) {
                ps.setObject(paramIndex, inValue, sqlType, scale);
            } else {
                ps.setObject(paramIndex, inValue, sqlType);
            }
        } else if (sqlType == Types.DATE) {
            if (inValue instanceof java.util.Date) {
                if (inValue instanceof java.sql.Date) {
                    ps.setDate(paramIndex, (java.sql.Date) inValue);
                } else {
                    ps.setDate(paramIndex, new java.sql.Date(((java.util.Date) inValue).getTime()));
                }
            } else if (inValue instanceof Calendar) {
                Calendar cal = (Calendar) inValue;
                ps.setDate(paramIndex, new java.sql.Date(cal.getTime().getTime()), cal);
            } else {
                ps.setObject(paramIndex, inValue, Types.DATE);
            }
        } else if (sqlType == Types.TIME) {
            if (inValue instanceof java.util.Date) {
                if (inValue instanceof java.sql.Time) {
                    ps.setTime(paramIndex, (java.sql.Time) inValue);
                } else {
                    ps.setTime(paramIndex, new java.sql.Time(((java.util.Date) inValue).getTime()));
                }
            } else if (inValue instanceof Calendar) {
                Calendar cal = (Calendar) inValue;
                ps.setTime(paramIndex, new java.sql.Time(cal.getTime().getTime()), cal);
            } else {
                ps.setObject(paramIndex, inValue, Types.TIME);
            }
        } else if (sqlType == Types.TIMESTAMP) {
            if (inValue instanceof java.util.Date) {
                if (inValue instanceof java.sql.Timestamp) {
                    ps.setTimestamp(paramIndex, (java.sql.Timestamp) inValue);
                } else {
                    ps.setTimestamp(paramIndex,
                            new java.sql.Timestamp(((java.util.Date) inValue).getTime()));
                }
            } else if (inValue instanceof Calendar) {
                Calendar cal = (Calendar) inValue;
                ps.setTimestamp(paramIndex, new java.sql.Timestamp(cal.getTime().getTime()), cal);
            } else {
                ps.setObject(paramIndex, inValue, Types.TIMESTAMP);
            }
        } else if (sqlType == UNKNOWN_TYPE) {
            if (isStringValue(inValue.getClass())) {
                ps.setString(paramIndex, inValue.toString());
            } else if (isDateValue(inValue.getClass())) {
                ps.setTimestamp(paramIndex,
                        new java.sql.Timestamp(((java.util.Date) inValue).getTime()));
            } else if (inValue instanceof Calendar) {
                Calendar cal = (Calendar) inValue;
                ps.setTimestamp(paramIndex, new java.sql.Timestamp(cal.getTime().getTime()), cal);
            } else {
                // Fall back to generic setObject call without SQL type
                // specified.
                ps.setObject(paramIndex, inValue);
            }
        } else {
            // Fall back to generic setObject call with SQL type specified.
            ps.setObject(paramIndex, inValue, sqlType);
        }
    }

    /**
     * Check whether the given value can be treated as a String value.
     */
    private static boolean isStringValue(Class<?> inValueType) {
        // Consider any CharSequence (including StringBuffer and StringBuilder)
        // as a String.
        return (CharSequence.class.isAssignableFrom(inValueType) || StringWriter.class
                .isAssignableFrom(inValueType));
    }

    /**
     * Check whether the given value is a <code>java.util.Date</code> (but not
     * one of the JDBC-specific subclasses).
     */
    private static boolean isDateValue(Class<?> inValueType) {
        return (java.util.Date.class.isAssignableFrom(inValueType) && !(java.sql.Date.class
                .isAssignableFrom(inValueType) || java.sql.Time.class.isAssignableFrom(inValueType) || java.sql.Timestamp.class
                .isAssignableFrom(inValueType)));
    }

}
