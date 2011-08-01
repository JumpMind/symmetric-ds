package org.jumpmind.symmetric.ddl.util;

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

import java.sql.Statement;
import java.sql.Types;

import org.jumpmind.symmetric.ddl.model.TypeMap;

/**
 * Little helper class providing functions for dealing with the newer JDBC functionality
 * in a way that is safe to compile with Java 1.3.
 * 
 * @version $Revision: 289996 $
 */
public abstract class Jdbc3Utils
{
    /**
     * Determines whether the system supports the Java 1.4 JDBC Types, DATALINK
     * and BOOLEAN.
     *   
     * @return <code>true</code> if BOOLEAN and DATALINK are available
     */
    public static boolean supportsJava14JdbcTypes()
    {
        try
        {
            return (Types.class.getField(TypeMap.BOOLEAN) != null) &&
                   (Types.class.getField(TypeMap.DATALINK) != null);
        }
        catch (Exception ex)
        {
            return false;
        }
    }

    /**
     * Determines the type code for the BOOLEAN JDBC type.
     * 
     * @return The type code
     * @throws UnsupportedOperationException If the BOOLEAN type is not supported
     */
    public static int determineBooleanTypeCode() throws UnsupportedOperationException
    {
        try
        {
            return Types.class.getField(TypeMap.BOOLEAN).getInt(null);
        }
        catch (Exception ex)
        {
            throw new UnsupportedOperationException("The jdbc type BOOLEAN is not supported");
        }
    }

    /**
     * Determines the type code for the DATALINK JDBC type.
     * 
     * @return The type code
     * @throws UnsupportedOperationException If the DATALINK type is not supported
     */
    public static int determineDatalinkTypeCode() throws UnsupportedOperationException
    {
        try
        {
            return Types.class.getField(TypeMap.DATALINK).getInt(null);
        }
        catch (Exception ex)
        {
            throw new UnsupportedOperationException("The jdbc type DATALINK is not supported");
        }
    }

    /**
     * Determines whether the system supports the Java 1.4 batch result codes.
     *   
     * @return <code>true</code> if SUCCESS_NO_INFO and EXECUTE_FAILED are available
     *         in the {@link java.sql.Statement} class
     */
    public static boolean supportsJava14BatchResultCodes()
    {
        try
        {
            return (Statement.class.getField("SUCCESS_NO_INFO") != null) &&
                   (Statement.class.getField("EXECUTE_FAILED") != null);
        }
        catch (Exception ex)
        {
            return false;
        }
    }

    /**
     * Returns the logging message corresponding to the given result code of a batch message.
     * Note that these code values are only available in JDBC 3 and newer (see
     * {@link java.sql.Statement} for details).
     * 
     * @param tableName  The name of the table that the batch update/insert was performed on
     * @param rowIdx     The index of the row within the batch for which this code is
     * @param resultCode The code
     * @return The string message or <code>null</code> if the code does not indicate an error
     */
    public static String getBatchResultMessage(String tableName, int rowIdx, int resultCode)
    {
        if (resultCode < 0)
        {
            try
            {
                if (resultCode == Statement.class.getField("SUCCESS_NO_INFO").getInt(null))
                {
                    return null;
                }
                else if (resultCode == Statement.class.getField("EXECUTE_FAILED").getInt(null))
                {
                    return "The batch insertion of row " + rowIdx + " into table " + tableName + " failed but the driver is able to continue processing";
                }
                else
                {
                    return "The batch insertion of row " + rowIdx + " into table " + tableName + " returned an undefined status value " + resultCode;
                }
            }
            catch (Exception ex)
            {
                throw new UnsupportedOperationException("The batch result codes are not supported");
            }
        }
        else
        {
            return null;
        }
    }
}
