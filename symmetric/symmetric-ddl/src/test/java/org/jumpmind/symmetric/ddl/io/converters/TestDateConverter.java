package org.jumpmind.symmetric.ddl.io.converters;

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
import java.sql.Types;
import java.util.Calendar;

import junit.framework.TestCase;

/**
 * Tests the {@link DateConverter}.
 *
 * @version $Revision: 1.0 $
 */
public class TestDateConverter extends TestCase
{
	/** The tested date converter. */
    private DateConverter _dateConverter;

    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        _dateConverter = new DateConverter();
    }

    /**
     * {@inheritDoc}
     */
    protected void tearDown() throws Exception
    {
        _dateConverter = null;
        super.tearDown();
    }

    /**
     * Tests a normal date string.
     */
    public void testNormalConvertFromYearMonthDateString()
    {
        String   textRep = "2005-12-19";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(2005, 11, 19);
        
        Object result = _dateConverter.convertFromString(textRep, Types.DATE);
        
        assertTrue(result instanceof Date);
        assertEquals(cal.getTimeInMillis(), ((Date)result).getTime());
    }

    /**
     * Tests a date string that has no day.
     */
    public void testNormalConvertFromYearMonthString()
    {
        String   textRep = "2005-12";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(2005, 11, 1);
        
        Object result = _dateConverter.convertFromString(textRep, Types.DATE);
        
        assertTrue(result instanceof Date);
        assertEquals(cal.getTimeInMillis(), ((Date)result).getTime());
    }

    /**
     * Tests a date string that has only a year.
     */
    public void testNormalConvertFromYearString()
    {
        String   textRep = "2005";
        Calendar cal     = Calendar.getInstance();

        cal.clear();
        cal.set(2005, 0, 1);
        
        Object result = _dateConverter.convertFromString(textRep, Types.DATE);

        assertTrue(result instanceof Date);
        assertEquals(cal.getTimeInMillis(), ((Date)result).getTime());
    }

    /**
     * Tests a full datetime string.
     */
    public void testNormalConvertFromFullDateTimeString()
    {
        String   textRep = "2005-06-07 10:11:12";
        Calendar cal     = Calendar.getInstance();

        cal.clear();
        cal.set(2005, 5, 7);

        Object result = _dateConverter.convertFromString(textRep, Types.DATE);

        assertTrue(result instanceof Date);
        assertEquals(cal.getTimeInMillis(), ((Date)result).getTime());
    }

    /**
     * Tests converting with an invalid SQL type.
     */
    public void testConvertFromStringWithInvalidSqlType()
    {
        String textRep = "2005-12-19";
        Object result  = _dateConverter.convertFromString(textRep, Types.INTEGER);

        // Make sure that the text representation is returned since SQL type was not a DATE
        assertNotNull(result);
        assertEquals(textRep, result);
    }

    /**
     * Tests handling of null.
     */
    public void testConvertFromStringWithNullTextRep()
    {
        String textRep = null;
        Object result  = _dateConverter.convertFromString(textRep, Types.DATE);

        assertNull(result);
    }

    /**
     * Tests an invalid date.
     */
    public void testConvertFromStringWithInvalidTextRep()
    {
        String textRep = "9999-99-99";

        try
        {
            _dateConverter.convertFromString(textRep, Types.DATE);
            fail("ConversionException expected");
        }
        catch (ConversionException ex)
        {
            // we expect the exception
        }
    }

    /**
     * Tests an invalid date that contains non-numbers.
     */
    public void testConvertFromStringWithAlphaTextRep()
    {
        String textRep = "aaaa-bb-cc";

        try
        {
            _dateConverter.convertFromString(textRep, Types.DATE);
            fail("ConversionException expected");
        }
        catch (ConversionException ex)
        {
            // we expect the exception
        }
    }

    /**
     * Tests converting a normal date to a string.
     */
    public void testNormalConvertToString()
    {
        Calendar cal = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(2005, 11, 19);

        Date   date   = new Date(cal.getTimeInMillis());
        String result = _dateConverter.convertToString(date, Types.DATE);

        assertNotNull(result);
        assertEquals("2005-12-19", result);
    }

    /**
     * Tests converting a null.
     */
    public void testConvertToStringWithNullDate()
    {
        Date   date   = null;
        String result = _dateConverter.convertToString(date, Types.DATE);

        assertNull(result);
    }

    /**
     * Tests converting a {@link java.util.Date}.
     */
    public void testConvertToStringWithWrongType()
    {
        java.util.Date date = new java.util.Date();

        try
        {
            _dateConverter.convertToString(date, Types.DATE);
            fail("ConversionException expected");
        }
        catch (ConversionException expected)
        {
            // we expect the exception
        }
    }
}
