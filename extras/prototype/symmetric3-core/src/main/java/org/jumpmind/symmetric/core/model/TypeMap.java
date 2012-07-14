package org.jumpmind.symmetric.core.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * A class that maps SQL type names to their JDBC type ID found in
 * {@link java.sql.Types} and vice versa.
 */
public abstract class TypeMap {
    /** The string representation of the {@link java.sql.Types#ARRAY} constant. */
    public static final String ARRAY = "ARRAY";
    /** The string representation of the {@link java.sql.Types#BIGINT} constant. */
    public static final String BIGINT = "BIGINT";
    /** The string representation of the {@link java.sql.Types#BINARY} constant. */
    public static final String BINARY = "BINARY";
    /** The string representation of the {@link java.sql.Types#BIT} constant. */
    public static final String BIT = "BIT";
    /** The string representation of the {@link java.sql.Types#BLOB} constant. */
    public static final String BLOB = "BLOB";
    /**
     * The string representation of the {@link java.sql.Types#BOOLEAN} constant.
     */
    public static final String BOOLEAN = "BOOLEAN";
    /** The string representation of the {@link java.sql.Types#CHAR} constant. */
    public static final String CHAR = "CHAR";
    /** The string representation of the {@link java.sql.Types#CLOB} constant. */
    public static final String CLOB = "CLOB";
    /**
     * The string representation of the {@link java.sql.Types#DATALINK}
     * constant.
     */
    public static final String DATALINK = "DATALINK";
    /** The string representation of the {@link java.sql.Types#DATE} constant. */
    public static final String DATE = "DATE";
    /**
     * The string representation of the {@link java.sql.Types#DECIMAL} constant.
     */
    public static final String DECIMAL = "DECIMAL";
    /**
     * The string representation of the {@link java.sql.Types#DISTINCT}
     * constant.
     */
    public static final String DISTINCT = "DISTINCT";
    /** The string representation of the {@link java.sql.Types#DOUBLE} constant. */
    public static final String DOUBLE = "DOUBLE";
    /** The string representation of the {@link java.sql.Types#FLOAT} constant. */
    public static final String FLOAT = "FLOAT";
    /**
     * The string representation of the {@link java.sql.Types#INTEGER} constant.
     */
    public static final String INTEGER = "INTEGER";
    /**
     * The string representation of the {@link java.sql.Types#JAVA_OBJECT}
     * constant.
     */
    public static final String JAVA_OBJECT = "JAVA_OBJECT";
    /**
     * The string representation of the {@link java.sql.Types#LONGVARBINARY}
     * constant.
     */
    public static final String LONGVARBINARY = "LONGVARBINARY";
    /**
     * The string representation of the {@link java.sql.Types#LONGVARCHAR}
     * constant.
     */
    public static final String LONGVARCHAR = "LONGVARCHAR";
    /** The string representation of the {@link java.sql.Types#NULL} constant. */
    public static final String NULL = "NULL";
    /**
     * The string representation of the {@link java.sql.Types#NUMERIC} constant.
     */
    public static final String NUMERIC = "NUMERIC";
    /** The string representation of the {@link java.sql.Types#OTHER} constant. */
    public static final String OTHER = "OTHER";
    /** The string representation of the {@link java.sql.Types#REAL} constant. */
    public static final String REAL = "REAL";
    /** The string representation of the {@link java.sql.Types#REF} constant. */
    public static final String REF = "REF";
    /**
     * The string representation of the {@link java.sql.Types#SMALLINT}
     * constant.
     */
    public static final String SMALLINT = "SMALLINT";
    /** The string representation of the {@link java.sql.Types#STRUCT} constant. */
    public static final String STRUCT = "STRUCT";
    /** The string representation of the {@link java.sql.Types#TIME} constant. */
    public static final String TIME = "TIME";
    /**
     * The string representation of the {@link java.sql.Types#TIMESTAMP}
     * constant.
     */
    public static final String TIMESTAMP = "TIMESTAMP";
    
    public static final String TIMESTAMPTZ     = "TIMESTAMPTZ";

    /**
     * The string representation of the {@link java.sql.Types#TINYINT} constant.
     */
    public static final String TINYINT = "TINYINT";
    /**
     * The string representation of the {@link java.sql.Types#VARBINARY}
     * constant.
     */
    public static final String VARBINARY = "VARBINARY";
    /**
     * The string representation of the {@link java.sql.Types#VARCHAR} constant.
     */
    public static final String VARCHAR = "VARCHAR";

    public static final String SQLXML = "SQLXML";
    /** Maps type names to the corresponding {@link java.sql.Types} constants. */
    private static HashMap<String, Integer> typeNameToTypeCode = new HashMap<String, Integer>();
    /**
     * Maps {@link java.sql.Types} type code constants to the corresponding type
     * names.
     */
    private static HashMap<Integer, String> typeCodeToTypeName = new HashMap<Integer, String>();

    /** Conatins the types per category. */
    private static HashMap<TypeCategory, Set<Integer>> typesPerCategory = new HashMap<TypeCategory, Set<Integer>>();

    static {
        registerJdbcType(Types.ARRAY, ARRAY, TypeCategory.SPECIAL);
        registerJdbcType(Types.BIGINT, BIGINT, TypeCategory.NUMERIC);
        registerJdbcType(Types.BINARY, BINARY, TypeCategory.BINARY);
        registerJdbcType(Types.BIT, BIT, TypeCategory.NUMERIC);
        registerJdbcType(Types.BLOB, BLOB, TypeCategory.BINARY);
        registerJdbcType(Types.CHAR, CHAR, TypeCategory.TEXTUAL);
        registerJdbcType(Types.CLOB, CLOB, TypeCategory.TEXTUAL);
        registerJdbcType(Types.DATE, DATE, TypeCategory.DATETIME);
        registerJdbcType(Types.DECIMAL, DECIMAL, TypeCategory.NUMERIC);
        registerJdbcType(Types.DISTINCT, DISTINCT, TypeCategory.SPECIAL);
        registerJdbcType(Types.DOUBLE, DOUBLE, TypeCategory.NUMERIC);
        registerJdbcType(Types.FLOAT, FLOAT, TypeCategory.NUMERIC);
        registerJdbcType(Types.INTEGER, INTEGER, TypeCategory.NUMERIC);
        registerJdbcType(Types.JAVA_OBJECT, JAVA_OBJECT, TypeCategory.SPECIAL);
        registerJdbcType(Types.LONGVARBINARY, LONGVARBINARY, TypeCategory.BINARY);
        registerJdbcType(Types.LONGVARCHAR, LONGVARCHAR, TypeCategory.TEXTUAL);
        registerJdbcType(Types.NULL, NULL, TypeCategory.SPECIAL);
        registerJdbcType(Types.NUMERIC, NUMERIC, TypeCategory.NUMERIC);
        registerJdbcType(Types.OTHER, OTHER, TypeCategory.SPECIAL);
        registerJdbcType(Types.REAL, REAL, TypeCategory.NUMERIC);
        registerJdbcType(Types.REF, REF, TypeCategory.SPECIAL);
        registerJdbcType(Types.SMALLINT, SMALLINT, TypeCategory.NUMERIC);
        registerJdbcType(Types.STRUCT, STRUCT, TypeCategory.SPECIAL);
        registerJdbcType(Types.TIME, TIME, TypeCategory.DATETIME);
        registerJdbcType(Types.TIMESTAMP, TIMESTAMP, TypeCategory.DATETIME);
        registerJdbcType(Types.TINYINT, TINYINT, TypeCategory.NUMERIC);
        registerJdbcType(Types.VARBINARY, VARBINARY, TypeCategory.BINARY);
        registerJdbcType(Types.VARCHAR, VARCHAR, TypeCategory.TEXTUAL);
        registerJdbcType(-101, TIMESTAMPTZ, TypeCategory.DATETIME);

        registerJdbcType(determineBooleanTypeCode(), BOOLEAN, TypeCategory.NUMERIC);
        registerJdbcType(determineDatalinkTypeCode(), DATALINK, TypeCategory.SPECIAL);

        // Torque/Turbine extensions which we only support when reading from an
        // XML schema
        typeNameToTypeCode.put("BOOLEANINT", new Integer(Types.TINYINT));
        typeNameToTypeCode.put("BOOLEANCHAR", new Integer(Types.CHAR));

        registerJdbcType(Types.SQLXML, SQLXML, TypeCategory.TEXTUAL);
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
     * Returns the JDBC type code (one of the {@link java.sql.Types} constants)
     * that corresponds to the given JDBC type name.
     * 
     * @param typeName
     *            The JDBC type name (case is ignored)
     * @return The type code or <code>null</code> if the type is unknown
     */
    public static Integer getJdbcTypeCode(String typeName) {
        return (Integer) typeNameToTypeCode.get(typeName.toUpperCase());
    }

    /**
     * Returns the JDBC type name that corresponds to the given type code (one
     * of the {@link java.sql.Types} constants).
     * 
     * @param typeCode
     *            The type code
     * @return The JDBC type name (one of the constants in this class) or
     *         <code>null</code> if the type is unknown
     */
    public static String getJdbcTypeName(int typeCode) {
        return (String) typeCodeToTypeName.get(new Integer(typeCode));
    }

    /**
     * Registers a jdbc type.
     * 
     * @param typeCode
     *            The type code (one of the {@link java.sql.Types} constants)
     * @param typeName
     *            The type name (case is ignored)
     * @param category
     *            The type category
     */
    protected static void registerJdbcType(int typeCode, String typeName, TypeCategory category) {
        Integer typeId = new Integer(typeCode);

        typeNameToTypeCode.put(typeName.toUpperCase(), typeId);
        typeCodeToTypeName.put(typeId, typeName.toUpperCase());

        Set<Integer> typesInCategory = typesPerCategory.get(category);

        if (typesInCategory == null) {
            typesInCategory = new HashSet<Integer>();
            typesPerCategory.put(category, typesInCategory);
        }
        typesInCategory.add(typeId);
    }

    /**
     * Determines whether the given jdbc type (one of the {@link java.sql.Types}
     * constants) is a numeric type.
     * 
     * @param jdbcTypeCode
     *            The type code
     * @return <code>true</code> if the type is a numeric one
     */
    public static boolean isNumericType(int jdbcTypeCode) {
        Set<Integer> typesInCategory = typesPerCategory.get(TypeCategory.NUMERIC);

        return typesInCategory == null ? false : typesInCategory
                .contains(new Integer(jdbcTypeCode));
    }

    /**
     * Determines whether the given jdbc type (one of the {@link java.sql.Types}
     * constants) is a date/time type.
     * 
     * @param jdbcTypeCode
     *            The type code
     * @return <code>true</code> if the type is a numeric one
     */
    public static boolean isDateTimeType(int jdbcTypeCode) {
        Set<Integer> typesInCategory = typesPerCategory.get(TypeCategory.DATETIME);

        return typesInCategory == null ? false : typesInCategory
                .contains(new Integer(jdbcTypeCode));
    }

    /**
     * Determines whether the given jdbc type (one of the {@link java.sql.Types}
     * constants) is a text type.
     * 
     * @param jdbcTypeCode
     *            The type code
     * @return <code>true</code> if the type is a text one
     */
    public static boolean isTextType(int jdbcTypeCode) {
        Set<Integer> typesInCategory = typesPerCategory.get(TypeCategory.TEXTUAL);

        return typesInCategory == null ? false : typesInCategory
                .contains(new Integer(jdbcTypeCode));
    }

    /**
     * Determines whether the given jdbc type (one of the {@link java.sql.Types}
     * constants) is a binary type.
     * 
     * @param jdbcTypeCode
     *            The type code
     * @return <code>true</code> if the type is a binary one
     */
    public static boolean isBinaryType(int jdbcTypeCode) {
        Set<Integer> typesInCategory = typesPerCategory.get(TypeCategory.BINARY);

        return typesInCategory == null ? false : typesInCategory
                .contains(new Integer(jdbcTypeCode));
    }

    /**
     * Determines whether the given sql type (one of the {@link java.sql.Types}
     * constants) is a special type.
     * 
     * @param jdbcTypeCode
     *            The type code
     * @return <code>true</code> if the type is a special one
     */
    public static boolean isSpecialType(int jdbcTypeCode) {
        Set<Integer> typesInCategory = typesPerCategory.get(TypeCategory.SPECIAL);

        return typesInCategory == null ? false : typesInCategory
                .contains(new Integer(jdbcTypeCode));
    }

    public static Boolean convertToBoolean(Object value) {
        if (value == null) {
            return Boolean.FALSE;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        String stringValue = value.toString();
        if (stringValue.equalsIgnoreCase("yes") || stringValue.equalsIgnoreCase("y")
                || stringValue.equalsIgnoreCase("true") || stringValue.equalsIgnoreCase("on")
                || stringValue.equalsIgnoreCase("1")) {
            return Boolean.TRUE;
        }

        if (stringValue.equalsIgnoreCase("no") || stringValue.equalsIgnoreCase("n")
                || stringValue.equalsIgnoreCase("false") || stringValue.equalsIgnoreCase("off")
                || stringValue.equalsIgnoreCase("0")) {
            return Boolean.FALSE;
        }
        return Boolean.FALSE;
    }
}
