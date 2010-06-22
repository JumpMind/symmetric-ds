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

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import junit.framework.Test;

import org.jumpmind.symmetric.ddl.io.BinaryObjectsHelper;
import org.jumpmind.symmetric.ddl.io.DatabaseDataIO;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.platform.interbase.InterbasePlatform;

/**
 * Performs roundtrip datatype tests.
 * 
 * @version $Revision: $
 */
public class TestDatatypes extends RoundtripTestBase
{
    // TODO: special columns (java_object, array, distinct, ...)

    /**
     * Parameterized test case pattern.
     * 
     * @return The tests
     */
    public static Test suite() throws Exception
    {
        return getTests(TestDatatypes.class);
    }

    /**
     * Performs a data type test.
     * 
     * @param modelXml The model as XML
     * @param value1   The non-pk value for the first row 
     * @param value2   The non-pk value for the second row 
     */
    protected void performDataTypeTest(String modelXml, Object value1, Object value2)
    {
        performDataTypeTest(modelXml, value1, value2, value1, value2);
    }

    /**
     * Performs a data type test for a model with a default value.
     * 
     * @param modelXml     The model as XML
     * @param value1       The non-pk value for the first row; use <code>null</code> for
     *                     the default value
     * @param value2       The non-pk value for the second row; use <code>null</code> for
     *                     the default value 
     * @param defaultValue The default value 
     */
    protected void performDataTypeTest(String modelXml, Object value1, Object value2, Object defaultValue)
    {
        performDataTypeTest(modelXml,
                            value1,
                            value2,
                            value1 == null ? defaultValue : value1,
                            value2 == null ? defaultValue : value2);
    }

    /**
     * Performs a data type test. In short, we're testing creation of a database, insertion of values
     * into it, and reading the model back. In addition we also check that DdlUtils does not try to
     * alter the new database when using the <code>alterTables</code>/<code>getAlterTablesSql</code>
     * methods of the {@link org.jumpmind.symmetric.ddl.Platform} with the read-back model.
     * 
     * @param modelXml  The model as XML
     * @param inserted1 The non-pk value to insert for the first row 
     * @param inserted2 The non-pk value to insert for the second row 
     * @param expected1 The expected non-pk value for the first row 
     * @param expected2 The expected non-pk value for the second row 
     */
    protected void performDataTypeTest(String modelXml, Object inserted1, Object inserted2, Object expected1, Object expected2)
    {
        createDatabase(modelXml);
        insertRow("roundtrip", new Object[] { new Integer(1), inserted1 });
        insertRow("roundtrip", new Object[] { new Integer(2), inserted2 });

        List beans = getRows("roundtrip");

        assertEquals(expected1, beans.get(0), "avalue");
        assertEquals(expected2, beans.get(1), "avalue");

        Database modelFromDb = readModelFromDatabase("roundtriptest");
        
        assertEquals(getAdjustedModel(),
                     modelFromDb);

        String alterTablesSql = getAlterTablesSql(modelFromDb).trim();

        assertEquals("",
        		     alterTablesSql);

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String dataSql = stringWriter.toString();
        
        assertTrue((dataSql != null) && (dataSql.length() > 0));

        getPlatform().dropTables(getModel(), false);

        createDatabase(modelXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { new StringReader(dataSql) });

        beans = getRows("roundtrip");

        assertEquals(expected1, beans.get(0), "avalue");
        assertEquals(expected2, beans.get(1), "avalue");
    }

    /**
     * Tests a simple BIT column.
     */
    public void testBit()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BIT'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, Boolean.TRUE, Boolean.FALSE);
    }

    /**
     * Tests a BIT column with a default value.
     */
    public void testBitWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BIT' required='true' default='FALSE'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, Boolean.TRUE, Boolean.FALSE);
    }

    /**
     * Tests a simple BOOLEAN column.
     */
    public void testBoolean()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BOOLEAN'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, Boolean.FALSE, Boolean.TRUE);
    }

    /**
     * Tests a BOOLEAN column with a default value.
     */
    public void testBooleanWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BOOLEAN' required='true' default='true'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, Boolean.TRUE, null, Boolean.TRUE);
    }

    /**
     * Tests a simple TINYINT column.
     */
    public void testTinyInt()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TINYINT'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Integer(254), new Integer(-254));
    }

    /**
     * Tests a TINYINT column with a default value.
     */
    public void testTinyIntWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TINYINT' required='true' default='-200'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Integer(128), null, new Integer(-200));
    }

    /**
     * Tests a simple SMALLINT column.
     */
    public void testSmallInt()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='SMALLINT'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Integer(Short.MIN_VALUE), new Integer(Short.MAX_VALUE));
    }

    /**
     * Tests a SMALLINT column with a default value.
     */
    public void testSmallIntWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='SMALLINT' required='true' default='-30000'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Integer(256), null, new Integer(-30000));
    }

    /**
     * Tests a simple INTEGER column.
     */
    public void testInteger()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Integer(0), new Integer(-2147483648));
    }

    /**
     * Tests a INTEGER column with a default value.
     */
    public void testIntegerWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='true' default='2147483647'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new Integer(2147483646), new Integer(2147483647));
    }

    /**
     * Tests a simple BIGINT column.
     */
    public void testBigInt()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BIGINT'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Long(Long.MAX_VALUE), new Long(0l));
    }

    /**
     * Tests a BIGINT column with a default value.
     */
    public void testBigIntWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BIGINT' required='true' default='-9000000000000000000'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new Long(-1l), new Long(-9000000000000000000l));
    }

    /**
     * Tests a simple REAL column.
     */
    public void testReal()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='REAL'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Float(12345.6f), new Float(0.0f));
    }

    /**
     * Tests a REAL column with a default value.
     */
    public void testRealWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='REAL' required='true' default='-1.01234'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Float(1e+20f), null, new Float(-1.01234f));
    }

    /**
     * Tests a simple FLOAT column.
     */
    public void testFloat()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='FLOAT'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Double(-1.0), new Double(1e-45));
    }

    /**
     * Tests a FLOAT column with a default value.
     */
    public void testFloatWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='FLOAT' required='true' default='12345678.9012345'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new Double(1e+25), new Double(12345678.9012345));
    }

    /**
     * Tests a simple DOUBLE column.
     */
    public void testDouble()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Double(1e+38), new Double(1.01));
    }

    /**
     * Tests a DOUBLE column with a default value.
     */
    public void testDoubleWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE' required='true' default='-987654321.098765'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new Double(-1e+25), null, new Double(-987654321.098765));
    }

    /**
     * Tests a simple DECIMAL column.
     */
    public void testDecimal()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DECIMAL' size='13'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new BigDecimal("0"), new BigDecimal("-1234567890123"));
    }

    /**
     * Tests a DECIMAL column with a default value.
     */
    public void testDecimalWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DECIMAL' size='15' required='true' default='123456789012345'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new BigDecimal("-1"), new BigDecimal("123456789012345"));
    }

    /**
     * Tests a simple DECIMAL column with a scale.
     */
    public void testDecimalWithScale()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DECIMAL' size='15,7'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new BigDecimal("0.0100001"), new BigDecimal("-87654321.1234567"));
    }

    /**
     * Tests a DECIMAL column with a scale and default value.
     */
    public void testDecimalWithScaleAndDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DECIMAL' size='15,7' required='true' default='12345678.7654321'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new BigDecimal("1.0000001"), null, new BigDecimal("12345678.7654321"));
    }

    /**
     * Tests a simple NUMERIC column.
     */
    public void testNumeric()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='12'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new BigDecimal("210987654321"), new BigDecimal("-2"));
    }

    /**
     * Tests a NUMERIC column with a default value.
     */
    public void testNumericWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='15' required='true' default='-123456789012345'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new BigDecimal("100"), new BigDecimal("-123456789012345"));
    }

    /**
     * Tests a simple NUMERIC column with a scale.
     */
    public void testNumericWithScale()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='15,8'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, new BigDecimal("1234567.89012345"), new BigDecimal("1.00000001"));
    }

    /**
     * Tests a NUMERIC column with a scale and default value.
     */
    public void testNumericWithScaleAndDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='15,8' required='true' default='-1234567.87654321'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, new BigDecimal("1e-8"), new BigDecimal("-1234567.87654321"));
    }

    /**
     * Tests a simple CHAR column.
     */
    public void testChar()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='CHAR' size='10'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, "1234567890");
    }

    /**
     * Tests a CHAR column with a default value.
     */
    public void testCharWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='CHAR' size='15' required='true' default='543210987654321'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, "123456789012345", "543210987654321", "123456789012345");
    }

    /**
     * Tests a simple VARCHAR column.
     */
    public void testVarChar()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, "123456789012345678", null);
    }

    /**
     * Tests a VARCHAR column with a default value.
     */
    public void testVarCharWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='254' required='true' default='some value'/>\n"+
            "  </table>\n"+
            "</database>";
        final String value =
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "12345678901234567890123456789012345678901234567890123456789012";

        performDataTypeTest(modelXml, null, value, "some value");
    }

    /**
     * Tests a VARCHAR column with a default value that contains a single quote.
     */
    public void testVarCharWithDefaultValueWithQuote()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='12' required='true' default='someone&apos;s'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, "123456", "someone's");
    }

    /**
     * Tests a VARCHAR column with a single quote as the default value.
     */
    public void testVarCharWithSingleQuoteAsDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='12' required='true' default='&apos;'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, "123456", "'");
    }

    /**
     * Tests a simple LONGVARCHAR column.
     */
    public void testLongVarChar()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='LONGVARCHAR'/>\n"+
            "  </table>\n"+
            "</database>";

        performDataTypeTest(modelXml, null, "some not too long text");
    }

    /**
     * Tests a LONGVARCHAR column with a default value.
     */
    public void testLongVarCharWithDefault()
    {
        if (!getPlatformInfo().isDefaultValuesForLongTypesSupported() ||
            InterbasePlatform.DATABASENAME.equals(getPlatform().getName()))
        {
            // Some Interbase versions do not like default values for LOB objects
            return;
        }

        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='LONGVARCHAR' required='true' default='some value'/>\n"+
            "  </table>\n"+
            "</database>";
        final String value =
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234";
        
        performDataTypeTest(modelXml, null, value, "some value");
    }

    /**
     * Tests a simple DATE column.
     */
    public void testDate()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DATE'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        performDataTypeTest(modelXml, null, new Date(103, 12, 25));
    }

    /**
     * Tests a DATE column with a default value.
     */
    public void testDateWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DATE' required='true' default='2000-01-01'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        performDataTypeTest(modelXml, new Date(105, 0, 1), null, new Date(100, 0, 1));
    }

    /**
     * Tests a simple TIME column.
     */
    public void testTime()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TIME'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        performDataTypeTest(modelXml, new Time(03, 47, 15), null);
    }

    /**
     * Tests a TIME column with a default value.
     */
    public void testTimeWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TIME' required='true' default='11:27:03'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        performDataTypeTest(modelXml, new Time(23, 59, 59), null, new Time(11, 27, 03));
    }

    /**
     * Tests a simple TIMESTAMP column.
     */
    public void testTimestamp()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TIMESTAMP'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        // also we leave out the fractional part because databases differ
        // in their support here
        performDataTypeTest(modelXml, new Timestamp(70, 0, 1, 0, 0, 0, 0), new Timestamp(100, 10, 11, 10, 10, 10, 0));
    }

    /**
     * Tests a TIMESTAMP column with a default value.
     */
    public void testTimestampWithDefault()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='TIMESTAMP' required='true' default='1985-06-17 16:17:18'/>\n"+
            "  </table>\n"+
            "</database>";

        // we would use Calendar but that might give Locale problems
        // also we leave out the fractional part because databases differ
        // in their support here
        performDataTypeTest(modelXml, new Timestamp(90, 9, 21, 20, 25, 39, 0), null, new Timestamp(85, 5, 17, 16, 17, 18, 0));
    }

    /**
     * Tests a simple BINARY column.
     */
    public void testBinary()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BINARY'/>\n"+
            "  </table>\n"+
            "</database>";

        HashMap   value1 = new HashMap();
        ArrayList value2 = new ArrayList();

        value1.put("test", "some value");
        value2.add("some other value");

        BinaryObjectsHelper helper = new BinaryObjectsHelper();

        performDataTypeTest(modelXml,
                            helper.serialize(value1), helper.serialize(value2),
                            value1, value2);
    }

    /**
     * Tests a simple VARBINARY column.
     */
    public void testVarBinary()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARBINARY'/>\n"+
            "  </table>\n"+
            "</database>";

        TreeSet value1 = new TreeSet();
        String  value2 = "a value, nothing special";

        value1.add("o look, a value !");

        BinaryObjectsHelper helper = new BinaryObjectsHelper();

        performDataTypeTest(modelXml,
                            helper.serialize(value1), helper.serialize(value2),
                            value1, value2);
    }

    /**
     * Tests a simple LONGVARBINARY column.
     */
    public void testLongVarBinary()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='LONGVARBINARY'/>\n"+
            "  </table>\n"+
            "</database>";

        HashMap value = new HashMap();

        value.put("test1", "some value");
        value.put(null, "some other value");

        BinaryObjectsHelper helper = new BinaryObjectsHelper();

        performDataTypeTest(modelXml,
                            helper.serialize(value), null,
                            value, null);
    }

    /**
     * Tests a simple BLOB column.
     */
    public void testBlob()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='BLOB'/>\n"+
            "  </table>\n"+
            "</database>";

        HashMap value = new HashMap();

        value.put("test1", "some value");
        value.put(null, "some other value");

        BinaryObjectsHelper helper = new BinaryObjectsHelper();

        performDataTypeTest(modelXml,
                            helper.serialize(value), null,
                            value, null);
    }

    /**
     * Tests a simple CLOB column.
     */
    public void testClob()
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='CLOB'/>\n"+
            "  </table>\n"+
            "</database>";
        final String value =
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234"+
            "1234567890123456789012345678901234567890123456789012345678901234";

        performDataTypeTest(modelXml, null, value);
    }
}
