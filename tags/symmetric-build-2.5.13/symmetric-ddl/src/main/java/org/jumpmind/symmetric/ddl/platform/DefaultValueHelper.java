package org.jumpmind.symmetric.ddl.platform;

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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtils;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.util.Jdbc3Utils;

/**
 * Helper class for dealing with default values, e.g. converting them to other types.
 * 
 * @version $Revision: 289996 $
 */
public class DefaultValueHelper
{
    /**
     * Converts the given default value from the specified original to the target
     * jdbc type.
     * 
     * @param defaultValue     The default value
     * @param originalTypeCode The original type code
     * @param targetTypeCode   The target type code
     * @return The converted default value 
     */
    public String convert(String defaultValue, int originalTypeCode, int targetTypeCode)
    {
        String result = defaultValue;

        if (defaultValue != null)
        {
            switch (originalTypeCode)
            {
                case Types.BIT:
                    result = convertBoolean(defaultValue, targetTypeCode).toString();
                    break;
                case Types.DATE:
                	if (targetTypeCode == Types.TIMESTAMP)
                	{
                		try
                		{
                			Date date = Date.valueOf(result);

                			return new Timestamp(date.getTime()).toString();
                		}
                		catch (IllegalArgumentException ex)
                		{}
                	}
                	break;
                case Types.TIME:
                	if (targetTypeCode == Types.TIMESTAMP)
                	{
                		try
                		{
                			Time time = Time.valueOf(result);

                			return new Timestamp(time.getTime()).toString();
                		}
                		catch (IllegalArgumentException ex)
                		{}
                	}
                    break;
                default:
                    if (Jdbc3Utils.supportsJava14JdbcTypes() &&
                        (originalTypeCode == Jdbc3Utils.determineBooleanTypeCode()))
                    {
                        result = convertBoolean(defaultValue, targetTypeCode).toString();
                    }
                    break;
            }
        }
        return result;
    }

    /**
     * Converts a boolean default value to the given target type.
     * 
     * @param defaultValue   The default value
     * @param targetTypeCode The target type code
     * @return The converted value
     */
    private Object convertBoolean(String defaultValue, int targetTypeCode)
    {
        Boolean value  = null;
        Object  result = null;

        try
        {
            value = (Boolean)ConvertUtils.convert(defaultValue, Boolean.class);
        }
        catch (ConversionException ex)
        {
            return defaultValue;
        }
        
        if ((targetTypeCode == Types.BIT) ||
            (Jdbc3Utils.supportsJava14JdbcTypes() && (targetTypeCode == Jdbc3Utils.determineBooleanTypeCode())))
        {
            result = value;
        }
        else if (TypeMap.isNumericType(targetTypeCode))
        {
            result = (value.booleanValue() ? new Integer(1) : new Integer(0));
        }
        else
        {
            result = value.toString();
        }
        return result;
    }
}
