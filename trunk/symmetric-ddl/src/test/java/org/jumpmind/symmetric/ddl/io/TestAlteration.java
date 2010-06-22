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

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;

import org.apache.commons.beanutils.DynaBean;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;

/**
 * Performs tests for the alteration of databases.
 * 
 * @version $Revision: $
 */
public class TestAlteration extends RoundtripTestBase
{
    /**
     * Parameterized test case pattern.
     * 
     * @return The tests
     */
    public static Test suite() throws Exception
    {
        return getTests(TestAlteration.class);
    }

    /**
     * Tests the alteration of a column datatype.
     */
    public void testChangeDatatype1()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='false'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE' required='false'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Double(2.0), beans.get(0), "avalue");
    }

    /**
     * Tests the alteration of a column datatype.
     */
    public void testChangeDatatype2()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='SMALLINT' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Short((short)2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List     beans = getRows("roundtrip");
        DynaBean bean  = (DynaBean)beans.get(0); 

        // Some databases (e.g. DB2) pad the string for some reason, so we manually trim it
        if (bean.get("avalue") instanceof String)
        {
            bean.set("avalue", ((String)bean.get("avalue")).trim());
        }
        assertEquals((Object)"2", beans.get(0), "avalue");
    }

    /**
     * Tests the alteration of the datatypes of PK and FK columns.
     */
    public void testChangePKAndFKDatatypes()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='VARCHAR' size='128' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='VARCHAR' size='128' required='false'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { new Integer(1), new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List     beans = getRows("roundtrip2");
        DynaBean bean  = (DynaBean)beans.get(0);

        // Some databases (e.g. DB2) pad the string for some reason, so we manually trim it
        if (bean.get("fk") instanceof String)
        {
            bean.set("fk", ((String)bean.get("fk")).trim());
        }
        assertEquals((Object)"1", bean, "fk");
    }

    /**
     * Tests the alteration of the datatypes of columns of a PK and FK that
     * will be dropped.
     */
    public void testChangeDroppedPKAndFKDatatypes()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='VARCHAR' primaryKey='false' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='VARCHAR' required='false'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { new Integer(1), new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List     beans = getRows("roundtrip2");
        DynaBean bean  = (DynaBean)beans.get(0);

        // Some databases (e.g. DB2) pad the string for some reason, so we manually trim it
        if (bean.get("fk") instanceof String)
        {
            bean.set("fk", ((String)bean.get("fk")).trim());
        }
        assertEquals((Object)"1", bean, "fk");
    }
    
    /**
     * Tests the alteration of the datatypes of a column that is indexed.
     */
    public void testChangeIndexColumnDatatype()
    {
        if (!getPlatformInfo().isIndicesSupported())
        {
            return;
        }

        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='8' required='false'/>\n"+
            "    <index name='avalue_index'>\n"+
            "      <index-column name='avalue'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='false'/>\n"+
            "    <index name='avalue_index'>\n"+
            "      <index-column name='avalue'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(1) });
        insertRow("roundtrip", new Object[] { new Integer(2), new Integer(10) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(1), beans.get(0), "avalue");
        assertEquals(new Integer(10), beans.get(1), "avalue");
    }

    /**
     * Tests the alteration of the datatypes of an indexed column where
     * the index will be dropped.
     */
    public void testChangeDroppedIndexColumnDatatype()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='NUMERIC' size='8' required='false'/>\n"+
            "    <index name='avalue_index'>\n"+
            "      <index-column name='avalue'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='false'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(1) });
        insertRow("roundtrip", new Object[] { new Integer(2), new Integer(10) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(1), beans.get(0), "avalue");
        assertEquals(new Integer(10), beans.get(1), "avalue");
    }

    /**
     * Tests the alteration of a column size.
     */
    public void testChangeSize()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the alteration of a column's datatype and size.
     */
    public void testChangeDatatypeAndSize()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='CHAR' size='4' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the alteration of a column null constraint.
     */
    public void testChangeNull()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='false'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(2), beans.get(0), "avalue");
    }

    /**
     * Tests the addition of a column's default value.
     */
    public void testAddDefault()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE' default='2.0'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Double(2.0) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Double(2.0), beans.get(0), "avalue");
    }

    /**
     * Tests the change of a column default value.
     */
    public void testChangeDefault()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='1'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='20'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(2), beans.get(0), "avalue");
    }

    /**
     * Tests the removal of a column default value.
     */
    public void testDropDefault()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20' default='test'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the change of a column's auto-increment state.
     */
    public void testMakeAutoIncrement()
    {
        if (!getPlatformInfo().isNonPKIdentityColumnsSupported())
        {
            return;
        }
        // Sybase does not like INTEGER auto-increment columns
        if (SybasePlatform.DATABASENAME.equals(getPlatform().getName()))
        {
            String  model1Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='avalue' type='NUMERIC' size='12,0'/>\n"+
                "  </table>\n"+
                "</database>";
            String model2Xml =
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='avalue' type='NUMERIC' size='12,0' autoIncrement='true' required='true'/>\n"+
                "  </table>\n"+
                "</database>";

            createDatabase(model1Xml);

            insertRow("roundtrip", new Object[] { new Integer(1), new BigDecimal(2) });

            alterDatabase(model2Xml);

            assertEquals(getAdjustedModel(),
                         readModelFromDatabase("roundtriptest"));

            List beans = getRows("roundtrip");

            assertEquals(new BigDecimal(2), beans.get(0), "avalue");
        }
        else
        {
            String  model1Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='avalue' type='INTEGER'/>\n"+
                "  </table>\n"+
                "</database>";
            String model2Xml=
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='avalue' type='INTEGER' autoIncrement='true' required='true'/>\n"+
                "  </table>\n"+
                "</database>";

            createDatabase(model1Xml);

            insertRow("roundtrip", new Object[] { new Integer(1), new Integer(2) });

            alterDatabase(model2Xml);

            assertEquals(getAdjustedModel(),
                         readModelFromDatabase("roundtriptest"));

            List beans = getRows("roundtrip");

            assertEquals(new Integer(2), beans.get(0), "avalue");
        }
    }

    /**
     * Tests the removal the column auto-increment status.
     */
    public void testDropAutoIncrement()
    {
        if (!getPlatformInfo().isNonPKIdentityColumnsSupported())
        {
            return;
        }

        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String model1Xml; 
        final String model2Xml;

        if (isSybase)
        {
            model1Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='NUMERIC' size='12,0' required='true' autoIncrement='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
            model2Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='NUMERIC' size='12,0' required='true' autoIncrement='false'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }
        else
        {
            model1Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='INTEGER' autoIncrement='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
            model2Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='INTEGER' autoIncrement='false'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "avalue");
        }
        else
        {
            assertEquals(new Integer(1), beans.get(0), "avalue");
        }
    }

    /**
     * Tests the addition of a column.
     */
    public void testAddColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)null, beans.get(0), "avalue");
    }

    /**
     * Tests the addition of an auto-increment column.
     */
    public void testAddAutoIncrementColumn()
    {
    	if (!getPlatformInfo().isNonPKIdentityColumnsSupported())
    	{
    		return;
    	}

        // we need special catering for Sybase which does not support identity for INTEGER columns
        boolean      isSybase  = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml;

        if (isSybase)
        {
            model2Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='NUMERIC' size='12,0' autoIncrement='true' required='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }
        else
        {
            model2Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='INTEGER' autoIncrement='true' required='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "avalue");
        }
        else
        {
            Object avalue = ((DynaBean)beans.get(0)).get("avalue");

            assertTrue((avalue == null) || new Integer(1).equals(avalue));
        }
    }

    /**
     * Tests the addition of several columns.
     */
    public void testAddColumns()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue3' type='DOUBLE' default='1.0'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='32'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "    <column name='avalue3' type='DOUBLE' default='1.0'/>\n"+
            "    <column name='avalue4' type='VARCHAR' size='16'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Double(3.0) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)null, beans.get(0), "avalue1");
        assertEquals((Object)null, beans.get(0), "avalue2");
        assertEquals(new Double(3.0), beans.get(0), "avalue3");
        assertEquals((Object)null, beans.get(0), "avalue4");
    }

    /**
     * Tests the addition of several columns at the end of the table.
     */
    public void testAddColumnsAtTheEnd()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='32'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='32'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "    <column name='avalue3' type='DOUBLE' default='1.0'/>\n"+
            "    <column name='avalue4' type='VARCHAR' size='16'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test", new Integer(3) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue1");
        assertEquals(new Integer(3), beans.get(0), "avalue2");

        // we cannot be sure whether the default algorithm is used (which will apply the
        // default value even to existing columns with NULL in it) or the database supports
        // it dircetly (in which case it might still be NULL)
        Object avalue3 = ((DynaBean)beans.get(0)).get("avalue3");

        assertTrue((avalue3 == null) || new Double(1.0).equals(avalue3));
        
        assertEquals((Object)null, beans.get(0), "avalue4");
    }

    /**
     * Tests the addition of a column with a default value. Note that depending
     * on whether the database supports this via a statement, this test may fail.
     * For instance, Sql Server has a statement for this which means that the
     * existing value in column avalue won't be changed and thus the test fails.
     */
    public void testAddColumnWithDefault()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='2'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        // we cannot be sure whether the default algorithm is used (which will apply the
        // default value even to existing columns with NULL in it) or the database supports
        // it dircetly (in which case it might still be NULL)
        Object avalue = ((DynaBean)beans.get(0)).get("avalue");

        assertTrue((avalue == null) || new Integer(2).equals(avalue));
    }

    /**
     * Tests the addition of a column that is set to NOT NULL.
     */
    public void testAddRequiredColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='2' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(2), beans.get(0), "avalue");
    }

    /**
     * Tests the change of the order of the columns of a table.
     */
    public void testChangeColumnOrder()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='32'/>\n"+
            "    <column name='avalue4' type='VARCHAR' size='5'/>\n"+
            "    <column name='avalue3' type='DOUBLE' default='1.0'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='32'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "    <column name='avalue3' type='DOUBLE' default='1.0'/>\n"+
            "    <column name='avalue4' type='VARCHAR' size='5'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test", "value", null, null });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue1");
        assertEquals((Object)null, beans.get(0), "avalue2");
        assertEquals(new Double(1.0), beans.get(0), "avalue3");
        assertEquals((Object)"value", beans.get(0), "avalue4");
    }

    /**
     * Tests the removal of a column.
     */
    public void testDropColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(1), beans.get(0), "pk");
    }

    /**
     * Tests the removal of an auto-increment column.
     */
    public void testDropAutoIncrementColumn()
    {
        if (!getPlatformInfo().isNonPKIdentityColumnsSupported())
        {
            return;
        }

        boolean      isSybase  = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String model1Xml;
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        if (isSybase)
        {
            model1Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='NUMERIC' size='12,0' required='true' autoIncrement='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }
        else
        {
            model1Xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                        "<database name='roundtriptest'>\n"+
                        "  <table name='roundtrip'>\n"+
                        "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                        "    <column name='avalue' type='INTEGER' autoIncrement='true'/>\n"+
                        "  </table>\n"+
                        "</database>";
        }
        
        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(1), beans.get(0), "pk");
    }

    /**
     * Tests the addition of a column to the pk.
     */
    public void testAddColumnToPK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the removal of a column from the pk.
     */
    public void testRemoveColumnFromPK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50' primaryKey='false' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the addition of a pk column.
     */
    public void testAddPKColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='0' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(0), beans.get(0), "avalue");
    }

    /**
     * Tests the addition of a primary key and a column.
     */
    public void testAddPKAndColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' default='0'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        // we cannot be sure whether the default algorithm is used (which will apply the
        // default value even to existing columns with NULL in it) or the database supports
        // it dircetly (in which case it might still be NULL)
        Object avalue = ((DynaBean)beans.get(0)).get("avalue");

        assertTrue((avalue == null) || new Integer(0).equals(avalue));
    }

    /**
     * Tests the addition of a primary key and a primary key column.
     */
    public void testAddPKAndPKColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' primaryKey='true' required='true' default='0'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(0), beans.get(0), "avalue");
    }

    /**
     * Tests the removal of a pk column.
     */
    public void testDropPKColumn()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='50' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(1), beans.get(0), "pk");
    }

    /**
     * Tests the addition of an index.
     */
    public void testAddIndex()
    {
        if (!getPlatformInfo().isIndicesSupported())
        {
            return;
        }

        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='50'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='50'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "    <index name='test'>\n"+
            "      <index-column name='avalue1'/>\n"+
            "      <index-column name='avalue2'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), null, new Integer(2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals((Object)null, beans.get(0), "avalue1");
        assertEquals(new Integer(2), beans.get(0), "avalue2");
    }

    /**
     * Tests the addition of an unique index.
     */
    public void testAddUniqueIndex()
    {
        if (!getPlatformInfo().isIndicesSupported())
        {
            return;
        }

        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "    <unique name='test'>\n"+
            "      <unique-column name='avalue'/>\n"+
            "    </unique>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Integer(2) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Integer(2), beans.get(0), "avalue");
    }

    /**
     * Tests the removal of an unique index.
     */
    public void testDropUniqueIndex()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='VARCHAR' size='50'/>\n"+
            "    <unique name='test_index'>\n"+
            "      <unique-column name='avalue2'/>\n"+
            "      <unique-column name='avalue1'/>\n"+
            "    </unique>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='VARCHAR' size='50'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Double(2.0), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Double(2.0), beans.get(0), "avalue1");
        assertEquals((Object)"test", beans.get(0), "avalue2");
    }

    /**
     * Tests the addition of a column to an index.
     */
    public void testAddColumnToIndex()
    {
        if (!getPlatformInfo().isIndicesSupported())
        {
            return;
        }

        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='VARCHAR' size='40'/>\n"+
            "    <index name='test_index'>\n"+
            "      <index-column name='avalue1'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='VARCHAR' size='40'/>\n"+
            "    <index name='test_index'>\n"+
            "      <index-column name='avalue1'/>\n"+
            "      <index-column name='avalue2'/>\n"+
            "    </index>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Double(2.0), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Double(2.0), beans.get(0), "avalue1");
        assertEquals((Object)"test", beans.get(0), "avalue2");
    }

    /**
     * Tests the removal of a column from an index.
     */
    public void testRemoveColumnFromUniqueIndex()
    {
        if (!getPlatformInfo().isIndicesSupported())
        {
            return;
        }

        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "    <unique name='test_index'>\n"+
            "      <unique-column name='avalue1'/>\n"+
            "      <unique-column name='avalue2'/>\n"+
            "    </unique>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE'/>\n"+
            "    <column name='avalue2' type='INTEGER'/>\n"+
            "    <unique name='test_index'>\n"+
            "      <unique-column name='avalue1'/>\n"+
            "    </unique>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), new Double(2.0), new Integer(3) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip");

        assertEquals(new Double(2.0), beans.get(0), "avalue1");
        assertEquals(new Integer(3), beans.get(0), "avalue2");
    }

    /**
     * Tests the addition of a foreign key.
     */
    public void testAddFK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER' required='true'/>\n"+
            "    <foreign-key name='test' foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { "2", new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");

        assertEquals(new Integer(1), beans1.get(0), "pk");
        assertEquals((Object)"2", beans2.get(0), "pk");
        assertEquals(new Integer(1), beans2.get(0), "avalue");
    }

    /**
     * Tests the removal of a foreign key.
     */
    public void testDropFK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='DOUBLE' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE' required='true'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue2' foreign='pk1'/>\n"+
            "      <reference local='avalue1' foreign='pk2'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='DOUBLE' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='DOUBLE' required='true'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1), new Double(2.0) });
        insertRow("roundtrip2", new Object[] { new Integer(2), new Double(2.0), new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");

        assertEquals(new Integer(1), beans1.get(0), "pk1");
        assertEquals(new Double(2.0), beans1.get(0), "pk2");
        assertEquals(new Integer(2), beans2.get(0), "pk");
        assertEquals(new Double(2.0), beans2.get(0), "avalue1");
        assertEquals(new Integer(1), beans2.get(0), "avalue2");
    }

    /**
     * Tests the removal of several foreign keys. Test for DDLUTILS-150.
     */
    public void testDropFKs()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip3'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip4'>\n"+
            "    <column name='pk' primaryKey='true' required='true' type='INTEGER' />\n"+
            "    <column name='fk1' required='true' type='INTEGER' />\n"+
            "    <column name='fk2' type='INTEGER' required='false' />\n"+
            "    <foreign-key name='roundtrip1_fk' foreignTable='roundtrip1'>\n"+
            "      <reference foreign='pk' local='pk' />\n"+
            "    </foreign-key>\n"+
            "    <foreign-key name='roundtrip2_fk1' foreignTable='roundtrip2'>\n"+
            "      <reference foreign='pk' local='fk1' />\n"+
            "    </foreign-key>\n"+
            "    <foreign-key name='roundtrip2_fk2' foreignTable='roundtrip2'>\n"+
            "      <reference foreign='pk' local='fk2' />\n"+
            "    </foreign-key>\n"+
            "    <foreign-key name='roundtrip3_fk' foreignTable='roundtrip3'>\n"+
            "      <reference foreign='pk1' local='pk' />\n"+
            "      <reference foreign='pk2' local='fk2' />\n"+
            "    </foreign-key>\n"+
            "   </table> \n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip3'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip4'>\n"+
            "    <column name='pk' primaryKey='true' required='true' type='INTEGER' />\n"+
            "    <column name='fk1' required='true' type='INTEGER' />\n"+
            "    <column name='fk2' type='INTEGER' required='false' />\n"+
            "   </table> \n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { new Integer(2) });
        insertRow("roundtrip2", new Object[] { new Integer(3) });
        insertRow("roundtrip3", new Object[] { new Integer(1), new Integer(2) });
        insertRow("roundtrip4", new Object[] { new Integer(1), new Integer(3), new Integer(2) });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");
        List beans3 = getRows("roundtrip3");
        List beans4 = getRows("roundtrip4");

        assertEquals(new Integer(1),  beans1.get(0), "pk");
        assertEquals(new Integer(2),  beans2.get(0), "pk");
        assertEquals(new Integer(3),  beans2.get(1), "pk");
        assertEquals(new Integer(1),  beans3.get(0), "pk1");
        assertEquals(new Integer(2),  beans3.get(0), "pk2");
        assertEquals(new Integer(1),  beans4.get(0), "pk");
        assertEquals(new Integer(3),  beans4.get(0), "fk1");
        assertEquals(new Integer(2),  beans4.get(0), "fk2");
    }

    /**
     * Tests the addition of a reference to a foreign key.
     */
    public void testAddReferenceToFK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='INTEGER' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue1' foreign='pk1'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='DOUBLE' default='0.0' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='INTEGER' required='true'/>\n"+
            "    <column name='avalue2' type='DOUBLE' default='0.0' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue1' foreign='pk1'/>\n"+
            "      <reference local='avalue2' foreign='pk2'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { new Integer(2), new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");

        assertEquals(new Integer(1), beans1.get(0), "pk1");
        assertEquals(new Double(0.0), beans1.get(0), "pk2");
        assertEquals(new Integer(2), beans2.get(0), "pk");
        assertEquals(new Integer(1), beans2.get(0), "avalue1");
        assertEquals(new Double(0.0), beans2.get(0), "avalue2");
    }

    /**
     * Tests the removal of a reference from a foreign key.
     */
    public void testRemoveReferenceFromFK()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='pk2' type='VARCHAR' size='12' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue1' type='VARCHAR' size='12' required='true'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue2' foreign='pk1'/>\n"+
            "      <reference local='avalue1' foreign='pk2'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk1' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue2' type='INTEGER' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue2' foreign='pk1'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1), "test" });
        insertRow("roundtrip2", new Object[] { new Integer(2), "test", new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");

        assertEquals(new Integer(1), beans1.get(0), "pk1");
        assertEquals(new Integer(2), beans2.get(0), "pk");
        assertEquals(new Integer(1), beans2.get(0), "avalue2");
    }

    /**
     * Tests the addition of a table.
     */
    public void testAddTable1()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
           "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='VARCHAR' size='20' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
           "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip1");

        assertEquals(new Integer(1), beans.get(0), "pk");
    }

    /**
     * Tests the addition of a table.
     */
    public void testAddTable2()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32' required='true'/>\n"+
            "  </table>\n"+
           "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "  </table>\n"+
           "</database>";
        final String model3Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='32' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip2'>\n"+
            "      <reference local='avalue' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='INTEGER'/>\n"+
            "  </table>\n"+
           "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1), "test" });

        alterDatabase(model2Xml);

        // note that we have to split the alteration because we can only add the foreign key if
        // there is a corresponding row in the new table

        insertRow("roundtrip2", new Object[] { "test" });

    	alterDatabase(model3Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans1 = getRows("roundtrip1");
        List beans2 = getRows("roundtrip2");

        assertEquals(new Integer(1), beans1.get(0), "pk");
        assertEquals((Object)"test", beans1.get(0), "avalue");
        assertEquals((Object)"test", beans2.get(0), "pk");
    }

    /**
     * Tests the addition of a table with an auto-increment primary key.
     */
    public void testAddAutoIncrementTable()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='VARCHAR' size='20' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
           "</database>";
        final String model2Xml; 

        // Sybase does not like INTEGER auto-increment columns
        if (SybasePlatform.DATABASENAME.equals(getPlatform().getName()))
        {
            model2Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip1'>\n"+
                "    <column name='pk' type='VARCHAR' size='20' primaryKey='true' required='true'/>\n"+
                "  </table>\n"+
                "  <table name='roundtrip2'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' autoIncrement='true' required='true'/>\n"+
                "  </table>\n"+
               "</database>";
        }
        else
        {
            model2Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip1'>\n"+
                "    <column name='pk' type='VARCHAR' size='20' primaryKey='true' required='true'/>\n"+
                "  </table>\n"+
                "  <table name='roundtrip2'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' autoIncrement='true' required='true'/>\n"+
                "  </table>\n"+
               "</database>";
        }
        createDatabase(model1Xml);
        
        insertRow("roundtrip1", new Object[] { "1" });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip1");

        assertEquals((Object)"1", beans.get(0), "pk");
    }

    /**
     * Tests the removal of a table.
     */
    public void testRemoveTable1()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='DOUBLE' required='true'/>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { new Integer(1) });
        insertRow("roundtrip2", new Object[] { new Integer(2), new Double(2.0) });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip1");

        assertEquals(new Integer(1), beans.get(0), "pk");
    }

    /**
     * Tests the removal of a table.
     */
    public void testRemoveTable2()
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip1'>\n"+
            "    <column name='pk' type='VARCHAR' size='20' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
            "    <foreign-key foreignTable='roundtrip1'>\n"+
            "      <reference local='avalue' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='roundtrip2'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip1", new Object[] { "test" });
        insertRow("roundtrip2", new Object[] { new Integer(1), "test" });

    	alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        List beans = getRows("roundtrip2");

        assertEquals(new Integer(1), beans.get(0), "pk");
        assertEquals((Object)"test", beans.get(0), "avalue");
    }

    /**
     * Tests the removal of a table with an auto-increment column.
     */
    public void testRemoveTable3()
    {
        final String model1Xml;
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "</database>";

        // Sybase does not like INTEGER auto-increment columns
        if (SybasePlatform.DATABASENAME.equals(getPlatform().getName()))
        {
            model1Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            model1Xml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='roundtrip'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='VARCHAR' size='20' required='true'/>\n"+
                "  </table>\n"+
                "</database>";
        }

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { null, "1" });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));
    }

    /**
     * Test for DDLUTILS-54.
     */
    public void testIssue54() throws Exception
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='coltype'>\n" +
            "    <column name='COL_FLOAT' primaryKey='false' required='false' type='FLOAT'/>\n" +
            "    <column name='COL_BOOLEAN' primaryKey='false' required='false' type='BOOLEAN'/>\n" +
            "  </table>\n" +
            "</database>";

        createDatabase(modelXml);

        Properties props   = getTestProperties();
        String     catalog = props.getProperty(DDLUTILS_CATALOG_PROPERTY);
        String     schema  = props.getProperty(DDLUTILS_SCHEMA_PROPERTY);
        Database   model   = parseDatabaseFromString(modelXml);

        getPlatform().setSqlCommentsOn(false);

        String alterationSql = getPlatform().getAlterTablesSql(catalog, schema, null, model);

        assertEqualsIgnoringWhitespaces("", alterationSql);
    }

    /**
     * Test for DDLUTILS-159.
     */
    public void testRenamePK() throws Exception
    {
        final String model1Xml = 
            "<?xml version='1.0'?>\n" +
            "<database name='roundtriptest'>\n" +
            "  <table name='roundtrip'>\n" +
            "    <column name='id' primaryKey='true' required='true' type='INTEGER'/>\n" +
            "    <column name='avalue' primaryKey='false' required='false' type='VARCHAR' size='40'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0'?>\n" +
            "<database name='roundtriptest'>\n" +
            "  <table name='roundtrip'>\n" +
            "    <column name='pk' primaryKey='true' required='true' type='INTEGER'/>\n" +
            "    <column name='avalue' primaryKey='false' required='false' type='VARCHAR' size='40'/>\n" +
            "  </table>\n" +
            "</database>";

        createDatabase(model1Xml);

        insertRow("roundtrip", new Object[] { new Integer(1), "test" });

        alterDatabase(model2Xml);

        assertEquals(getAdjustedModel(),
                     readModelFromDatabase("roundtriptest"));

        assertTrue(getRows("roundtrip").isEmpty());
    }
}
