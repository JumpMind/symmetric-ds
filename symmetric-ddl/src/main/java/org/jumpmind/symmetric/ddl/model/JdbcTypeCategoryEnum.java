package org.jumpmind.symmetric.ddl.model;

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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.enums.ValuedEnum;

/**
 * Represents the different categories of jdbc types.
 * 
 * @version $Revision: $
 */
public class JdbcTypeCategoryEnum extends ValuedEnum
{
    /** The integer value for the enum value for numeric jdbc types. */
    public static final int VALUE_NUMERIC  = 1;
    /** The integer value for the enum value for date/time jdbc types. */
    public static final int VALUE_DATETIME = 2;
    /** The integer value for the enum value for textual jdbc types. */
    public static final int VALUE_TEXTUAL  = 3;
    /** The integer value for the enum value for binary jdbc types. */
    public static final int VALUE_BINARY   = 4;
    /** The integer value for the enum value for special jdbc types. */
    public static final int VALUE_SPECIAL  = 5;
    /** The integer value for the enum value for all other jdbc types. */
    public static final int VALUE_OTHER    = 6;

    /** The enum value for numeric jdbc types. */
    public static final JdbcTypeCategoryEnum NUMERIC  = new JdbcTypeCategoryEnum("numeric",  VALUE_NUMERIC);
    /** The enum value for date/time jdbc types. */
    public static final JdbcTypeCategoryEnum DATETIME = new JdbcTypeCategoryEnum("datetime", VALUE_DATETIME);
    /** The enum value for textual jdbc types. */
    public static final JdbcTypeCategoryEnum TEXTUAL  = new JdbcTypeCategoryEnum("textual",  VALUE_TEXTUAL);
    /** The enum value for binary jdbc types. */
    public static final JdbcTypeCategoryEnum BINARY   = new JdbcTypeCategoryEnum("binary",   VALUE_BINARY);
    /** The enum value for special jdbc types. */
    public static final JdbcTypeCategoryEnum SPECIAL  = new JdbcTypeCategoryEnum("special",  VALUE_SPECIAL);
    /** The enum value for other jdbc types. */
    public static final JdbcTypeCategoryEnum OTHER    = new JdbcTypeCategoryEnum("other",    VALUE_OTHER);

    /** Version id for this class as relevant for serialization. */
    private static final long serialVersionUID = -2695615907467866410L;

    /**
     * Creates a new enum object.
     * 
     * @param defaultTextRep The textual representation
     * @param value          The corresponding integer value
     */
    private JdbcTypeCategoryEnum(String defaultTextRep, int value)
    {
        super(defaultTextRep, value);
    }

    /**
     * Returns the enum value that corresponds to the given textual
     * representation.
     * 
     * @param defaultTextRep The textual representation
     * @return The enum value
     */
    public static JdbcTypeCategoryEnum getEnum(String defaultTextRep)
    {
        return (JdbcTypeCategoryEnum)getEnum(JdbcTypeCategoryEnum.class, defaultTextRep);
    }
    
    /**
     * Returns the enum value that corresponds to the given integer
     * representation.
     * 
     * @param intValue The integer value
     * @return The enum value
     */
    public static JdbcTypeCategoryEnum getEnum(int intValue)
    {
        return (JdbcTypeCategoryEnum)getEnum(JdbcTypeCategoryEnum.class, intValue);
    }

    /**
     * Returns the map of enum values.
     * 
     * @return The map of enum values
     */
    public static Map getEnumMap()
    {
        return getEnumMap(JdbcTypeCategoryEnum.class);
    }

    /**
     * Returns a list of all enum values.
     * 
     * @return The list of enum values
     */
    public static List getEnumList()
    {
        return getEnumList(JdbcTypeCategoryEnum.class);
    }

    /**
     * Returns an iterator of all enum values.
     * 
     * @return The iterator
     */
    public static Iterator iterator()
    {
        return iterator(JdbcTypeCategoryEnum.class);
    }
}
