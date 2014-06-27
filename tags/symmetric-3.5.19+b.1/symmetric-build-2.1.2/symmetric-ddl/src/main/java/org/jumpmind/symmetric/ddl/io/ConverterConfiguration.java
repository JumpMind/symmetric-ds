package org.jumpmind.symmetric.ddl.io;

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

import org.jumpmind.symmetric.ddl.io.converters.ByteArrayBase64Converter;
import org.jumpmind.symmetric.ddl.io.converters.DateConverter;
import org.jumpmind.symmetric.ddl.io.converters.NumberConverter;
import org.jumpmind.symmetric.ddl.io.converters.SqlTypeConverter;
import org.jumpmind.symmetric.ddl.io.converters.TimeConverter;
import org.jumpmind.symmetric.ddl.io.converters.TimestampConverter;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.util.Jdbc3Utils;

/**
 * Contains the configuration for converters, which convert between the Java data types
 * corresponding to SQL data, and string representations.
 * 
 * @version $Revision: 289996 $
 */
public class ConverterConfiguration
{
    /** The converters per type. */
    private HashMap  _convertersPerType = new HashMap();
    /** The converters per table-column path. */
    private HashMap  _convertersPerPath = new HashMap();

    /**
     * Creates a new configuration object with the default converters.
     */
    public ConverterConfiguration()
    {
        NumberConverter          numberConverter = new NumberConverter();
        ByteArrayBase64Converter binaryConverter = new ByteArrayBase64Converter();

        registerConverter(Types.DATE,          new DateConverter());
        registerConverter(Types.TIME,          new TimeConverter());
        registerConverter(Types.TIMESTAMP,     new TimestampConverter());
        registerConverter(Types.BIGINT,        numberConverter);
        registerConverter(Types.BIT,           numberConverter);
        registerConverter(Types.DECIMAL,       numberConverter);
        registerConverter(Types.DOUBLE,        numberConverter);
        registerConverter(Types.FLOAT,         numberConverter);
        registerConverter(Types.INTEGER,       numberConverter);
        registerConverter(Types.NUMERIC,       numberConverter);
        registerConverter(Types.REAL,          numberConverter);
        registerConverter(Types.SMALLINT,      numberConverter);
        registerConverter(Types.TINYINT,       numberConverter);
        registerConverter(Types.BINARY,        binaryConverter);
        registerConverter(Types.VARBINARY,     binaryConverter);
        registerConverter(Types.LONGVARBINARY, binaryConverter);
        registerConverter(Types.BLOB,          binaryConverter);
        if (Jdbc3Utils.supportsJava14JdbcTypes())
        {
            registerConverter(Jdbc3Utils.determineBooleanTypeCode(), numberConverter);
        }
    }

    /**
     * Registers the given type converter for an sql type.
     * 
     * @param sqlTypeCode The type code, one of the {@link java.sql.Types} constants
     * @param converter   The converter
     */
    public void registerConverter(int sqlTypeCode, SqlTypeConverter converter)
    {
        _convertersPerType.put(new Integer(sqlTypeCode), converter);
    }

    /**
     * Registers the given type converter for the specified column.
     * 
     * @param tableName  The name of the table
     * @param columnName The name of the column
     * @param converter  The converter
     */
    public void registerConverter(String tableName, String columnName, SqlTypeConverter converter)
    {
        _convertersPerPath.put(tableName +"/" + columnName, converter);
    }

    /**
     * Returns the converter registered for the specified column.
     * 
     * @param table  The table
     * @param column The column
     * @return The converter
     */
    public SqlTypeConverter getRegisteredConverter(Table table, Column column)
    {
        SqlTypeConverter result = (SqlTypeConverter)_convertersPerPath.get(table.getName() + "/" + column.getName());

        if (result == null)
        {
            result = (SqlTypeConverter)_convertersPerType.get(new Integer(column.getTypeCode()));
        }
        return result;
    }
}
