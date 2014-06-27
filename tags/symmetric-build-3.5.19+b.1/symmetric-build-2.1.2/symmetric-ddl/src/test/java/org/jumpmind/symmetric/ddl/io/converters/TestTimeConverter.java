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

import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;

import junit.framework.TestCase;

/**
 * Tests the {@link TimeConverter}.
 *
 * @version $Revision: 1.0 $
 */
public class TestTimeConverter extends TestCase
{
	/** The tested time converter. */
    private TimeConverter _timeConverter;

    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        _timeConverter = new TimeConverter();
    }

    /**
     * {@inheritDoc}
     */
    protected void tearDown() throws Exception
    {
        _timeConverter = null;
        super.tearDown();
    }

    /**
     * Tests a normal time string.
     */
    public void testNormalConvertFromHoursMinutesSecondsTimeString()
    {
        String   textRep = "02:15:59";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.HOUR, 2);
        cal.set(Calendar.MINUTE, 15);
        cal.set(Calendar.SECOND, 59);
        
        Object result = _timeConverter.convertFromString(textRep, Types.TIME);
        
        assertTrue(result instanceof Time);
        assertEquals(cal.getTimeInMillis(), ((Time)result).getTime());
    }

    /**
     * Tests a time string without seconds.
     */
    public void testNormalConvertFromHoursMinutesTimeString()
    {
        String   textRep = "02:15";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.HOUR, 2);
        cal.set(Calendar.MINUTE, 15);

        Object result = _timeConverter.convertFromString(textRep, Types.TIME);

        assertTrue(result instanceof Time);
        assertEquals(cal.getTimeInMillis(), ((Time)result).getTime());
    }

    /**
     * Tests a time string with only an hour value.
     */
    public void testNormalConvertFromHoursTimeString()
    {
        String   textRep = "02";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.HOUR, 2);

        Object result = _timeConverter.convertFromString(textRep, Types.TIME);

        assertTrue(result instanceof Time);
        assertEquals(cal.getTimeInMillis(), ((Time)result).getTime());
    }

    /**
     * Tests a full ISO datetime string.
     */
    public void testNormalConvertFromIsoDateTimeString()
    {
        String   textRep = "2004-01-13 04:45:09.245";
        Calendar cal     = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.HOUR, 4);
        cal.set(Calendar.MINUTE, 45);
        cal.set(Calendar.SECOND, 9);

        Object result = _timeConverter.convertFromString(textRep, Types.TIME);

        assertTrue(result instanceof Time);
        assertEquals(cal.getTimeInMillis(), ((Time)result).getTime());
    }

    /**
     * Tests converting with an invalid SQL type.
     */
    public void testConvertFromStringWithInvalidSqlType()
    {
        String textRep = "02:15:59";
        Object result  = _timeConverter.convertFromString(textRep, Types.INTEGER);

        assertNotNull(result);
        assertEquals(textRep, result);
    }

    /**
     * Tests converting a null.
     */
    public void testConvertFromStringWithNullTextRep()
    {
        String textRep = null;
        Object result  = _timeConverter.convertFromString(textRep, Types.TIME);

        assertNull(result);
    }

    /**
     * Tests converting an invalid time string.
     */
    public void testConvertFromStringWithInvalidTextRep()
    {
        String textRep = "99:99:99";

        try
        {
            _timeConverter.convertFromString(textRep, Types.TIME);
            fail("ConversionException expected");
        }
        catch (ConversionException ex)
        {
            // We expect the exception 
        }
    }

    /**
     * Tests converting an invalid time string containing not only numbers.
     */
    public void testConvertFromStringWithAlphaTextRep()
    {
        String textRep = "aa:bb:cc";

        try
        {
            _timeConverter.convertFromString(textRep, Types.TIME);
            fail("ConversionException expected");
        }
        catch (ConversionException expected)
        {
            // We expect the exception
        }
    }

    /**
     * Tests converting a normal time to a string.
     */
    public void testNormalConvertToString()
    {
        Calendar cal = Calendar.getInstance();

        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.HOUR, 2);
        cal.set(Calendar.MINUTE, 15);
        cal.set(Calendar.SECOND, 59);

        Time   time   = new Time(cal.getTimeInMillis());
        String result = _timeConverter.convertToString(time, Types.TIME);

        assertNotNull(result);
        assertEquals("02:15:59", result);
    }

    /**
     * Tests converting a null time.
     */
    public void testConvertToStringWithNullTime()
    {
        Time   time   = null;
        String result = _timeConverter.convertToString(time, Types.TIME);

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
            _timeConverter.convertToString(date, Types.TIME);
            fail("ConversionException expected");
        }
        catch (ConversionException expected)
        {
            // We expect the exception
        }
    }
}
