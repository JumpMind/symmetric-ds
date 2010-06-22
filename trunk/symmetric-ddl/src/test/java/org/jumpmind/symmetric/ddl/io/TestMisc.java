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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import junit.framework.Test;

import org.apache.commons.beanutils.DynaBean;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jumpmind.symmetric.ddl.io.DatabaseDataIO;
import org.jumpmind.symmetric.ddl.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;
import org.xml.sax.InputSource;

/**
 * Contains misc tests.
 * 
 * @version $Revision: $
 */
public class TestMisc extends RoundtripTestBase
{
    /**
     * Parameterized test case pattern.
     * 
     * @return The tests
     */
    public static Test suite() throws Exception
    {
        return getTests(TestMisc.class);
    }

    /**
     * Tests the backup and restore of a table with an identity column and a foreign key to
     * it when identity override is turned on.
     */
    public void testIdentityOverrideOn() throws Exception
    {
        if (!getPlatformInfo().isIdentityOverrideAllowed())
        {
            // TODO: for testing these platforms, we need deleteRows
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml; 

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc1'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='INTEGER' required='false'/>\n"+
                "  </table>\n"+
                "  <table name='misc2'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='fk' type='NUMERIC' size='12,0' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc1'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc1'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='INTEGER' required='false'/>\n"+
                "  </table>\n"+
                "  <table name='misc2'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='fk' type='INTEGER' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc1'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        if (isSybase)
        {
            insertRow("misc1", new Object[] { new BigDecimal(10), new Integer(1) });
            insertRow("misc1", new Object[] { new BigDecimal(12), new Integer(2) });
            insertRow("misc1", new Object[] { new BigDecimal(13), new Integer(3) });
            insertRow("misc2", new Object[] { new Integer(1), new BigDecimal(10) });
            insertRow("misc2", new Object[] { new Integer(2), new BigDecimal(13) });
        }
        else
        {
            insertRow("misc1", new Object[] { new Integer(10), new Integer(1) });
            insertRow("misc1", new Object[] { new Integer(12), new Integer(2) });
            insertRow("misc1", new Object[] { new Integer(13), new Integer(3) });
            insertRow("misc2", new Object[] { new Integer(1), new Integer(10) });
            insertRow("misc2", new Object[] { new Integer(2), new Integer(13) });
        }

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String    dataAsXml = stringWriter.toString();
        SAXReader reader    = new SAXReader();
        Document  testDoc   = reader.read(new InputSource(new StringReader(dataAsXml)));

        List   misc1Rows       = testDoc.selectNodes("//misc1");
        List   misc2Rows       = testDoc.selectNodes("//misc2");
        String pkColumnName    = "pk";
        String fkColumnName    = "fk";
        String valueColumnName = "avalue";

        if (misc1Rows.size() == 0)
        {
            misc1Rows       = testDoc.selectNodes("//MISC1");
            misc2Rows       = testDoc.selectNodes("//MISC2");
            pkColumnName    = pkColumnName.toUpperCase();
            fkColumnName    = fkColumnName.toUpperCase();
            valueColumnName = valueColumnName.toUpperCase();
        }

        assertEquals(3, misc1Rows.size());
        assertEquals("10", ((Element)misc1Rows.get(0)).attributeValue(pkColumnName));
        assertEquals("1",  ((Element)misc1Rows.get(0)).attributeValue(valueColumnName));
        assertEquals("12", ((Element)misc1Rows.get(1)).attributeValue(pkColumnName));
        assertEquals("2",  ((Element)misc1Rows.get(1)).attributeValue(valueColumnName));
        assertEquals("13", ((Element)misc1Rows.get(2)).attributeValue(pkColumnName));
        assertEquals("3",  ((Element)misc1Rows.get(2)).attributeValue(valueColumnName));
        assertEquals(2, misc2Rows.size());
        assertEquals("1",  ((Element)misc2Rows.get(0)).attributeValue(pkColumnName));
        assertEquals("10", ((Element)misc2Rows.get(0)).attributeValue(fkColumnName));
        assertEquals("2",  ((Element)misc2Rows.get(1)).attributeValue(pkColumnName));
        assertEquals("13", ((Element)misc2Rows.get(1)).attributeValue(fkColumnName));

        dropDatabase();
        createDatabase(modelXml);

        StringReader stringReader = new StringReader(dataAsXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc1");

        if (isSybase)
        {
            assertEquals(new BigDecimal(10), beans.get(0), "pk");
            assertEquals(new BigDecimal(12), beans.get(1), "pk");
            assertEquals(new BigDecimal(13), beans.get(2), "pk");
        }
        else
        {
            assertEquals(new Integer(10), beans.get(0), "pk");
            assertEquals(new Integer(12), beans.get(1), "pk");
            assertEquals(new Integer(13), beans.get(2), "pk");
        }
        assertEquals(new Integer(1),  beans.get(0), "avalue");
        assertEquals(new Integer(2),  beans.get(1), "avalue");
        assertEquals(new Integer(3),  beans.get(2), "avalue");

        beans = getRows("misc2");

        assertEquals(new Integer(1),  beans.get(0), "pk");
        assertEquals(new Integer(2),  beans.get(1), "pk");
        if (isSybase)
        {
            assertEquals(new BigDecimal(10), beans.get(0), "fk");
            assertEquals(new BigDecimal(13), beans.get(1), "fk");
        }
        else
        {
            assertEquals(new Integer(10), beans.get(0), "fk");
            assertEquals(new Integer(13), beans.get(1), "fk");
        }
    }

    /**
     * Tests the backup and restore of a table with an identity column and a foreign key to
     * it when identity override is turned off.
     */
    public void testIdentityOverrideOff() throws Exception
    {
        if (!getPlatformInfo().isIdentityOverrideAllowed())
        {
            // TODO: for testing these platforms, we need deleteRows
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml; 

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc1'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='INTEGER' required='false'/>\n"+
                "  </table>\n"+
                "  <table name='misc2'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='fk' type='NUMERIC' size='12,0' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc1'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc1'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='avalue' type='INTEGER' required='false'/>\n"+
                "  </table>\n"+
                "  <table name='misc2'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
                "    <column name='fk' type='INTEGER' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc1'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        if (isSybase)
        {
            insertRow("misc1", new Object[] { new BigDecimal(10), new Integer(1) });
            insertRow("misc1", new Object[] { new BigDecimal(12), new Integer(2) });
            insertRow("misc1", new Object[] { new BigDecimal(13), new Integer(3) });
            insertRow("misc2", new Object[] { new Integer(1), new BigDecimal(10) });
            insertRow("misc2", new Object[] { new Integer(2), new BigDecimal(13) });
        }
        else
        {
            insertRow("misc1", new Object[] { new Integer(10), new Integer(1) });
            insertRow("misc1", new Object[] { new Integer(12), new Integer(2) });
            insertRow("misc1", new Object[] { new Integer(13), new Integer(3) });
            insertRow("misc2", new Object[] { new Integer(1), new Integer(10) });
            insertRow("misc2", new Object[] { new Integer(2), new Integer(13) });
        }

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String    dataAsXml = stringWriter.toString();
        SAXReader reader    = new SAXReader();
        Document  testDoc   = reader.read(new InputSource(new StringReader(dataAsXml)));

        List   misc1Rows       = testDoc.selectNodes("//misc1");
        List   misc2Rows       = testDoc.selectNodes("//misc2");
        String pkColumnName    = "pk";
        String fkColumnName    = "fk";
        String valueColumnName = "avalue";

        if (misc1Rows.size() == 0)
        {
            misc1Rows       = testDoc.selectNodes("//MISC1");
            misc2Rows       = testDoc.selectNodes("//MISC2");
            pkColumnName    = pkColumnName.toUpperCase();
            fkColumnName    = fkColumnName.toUpperCase();
            valueColumnName = valueColumnName.toUpperCase();
        }

        assertEquals(3, misc1Rows.size());
        assertEquals("10", ((Element)misc1Rows.get(0)).attributeValue(pkColumnName));
        assertEquals("1",  ((Element)misc1Rows.get(0)).attributeValue(valueColumnName));
        assertEquals("12", ((Element)misc1Rows.get(1)).attributeValue(pkColumnName));
        assertEquals("2",  ((Element)misc1Rows.get(1)).attributeValue(valueColumnName));
        assertEquals("13", ((Element)misc1Rows.get(2)).attributeValue(pkColumnName));
        assertEquals("3",  ((Element)misc1Rows.get(2)).attributeValue(valueColumnName));
        assertEquals(2, misc2Rows.size());
        assertEquals("1",  ((Element)misc2Rows.get(0)).attributeValue(pkColumnName));
        assertEquals("10", ((Element)misc2Rows.get(0)).attributeValue(fkColumnName));
        assertEquals("2",  ((Element)misc2Rows.get(1)).attributeValue(pkColumnName));
        assertEquals("13", ((Element)misc2Rows.get(1)).attributeValue(fkColumnName));

        dropDatabase();
        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(false);

        StringReader stringReader = new StringReader(dataAsXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc1");

        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "pk");
            assertEquals(new BigDecimal(2), beans.get(1), "pk");
            assertEquals(new BigDecimal(3), beans.get(2), "pk");
        }
        else
        {
            assertEquals(new Integer(1), beans.get(0), "pk");
            assertEquals(new Integer(2), beans.get(1), "pk");
            assertEquals(new Integer(3), beans.get(2), "pk");
        }
        assertEquals(new Integer(1), beans.get(0), "avalue");
        assertEquals(new Integer(2), beans.get(1), "avalue");
        assertEquals(new Integer(3), beans.get(2), "avalue");

        beans = getRows("misc2");

        assertEquals(new Integer(1), beans.get(0), "pk");
        assertEquals(new Integer(2), beans.get(1), "pk");
        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "fk");
            assertEquals(new BigDecimal(3), beans.get(1), "fk");
        }
        else
        {
            assertEquals(new Integer(1), beans.get(0), "fk");
            assertEquals(new Integer(3), beans.get(1), "fk");
        }
    }

    /**
     * Tests the backup and restore of a table with an identity column and a foreign key to
     * itself while identity override is off.
     */
    public void testSelfReferenceIdentityOverrideOff() throws Exception
    {
        // Hsqldb does not allow rows to reference themselves
        if (HsqlDbPlatform.DATABASENAME.equals(getPlatform().getName()))
        {
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml;

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='fk' type='NUMERIC' size='12,0' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='fk' type='INTEGER' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(false);

        if (isSybase)
        {
            insertRow("misc", new Object[] { new BigDecimal(1), null });
            insertRow("misc", new Object[] { new BigDecimal(2), new BigDecimal(1) });
            insertRow("misc", new Object[] { new BigDecimal(3), new BigDecimal(2) });
            insertRow("misc", new Object[] { new BigDecimal(4), new BigDecimal(4) });
        }
        else
        {
            insertRow("misc", new Object[] { new Integer(1), null });
            insertRow("misc", new Object[] { new Integer(2), new Integer(1) });
            insertRow("misc", new Object[] { new Integer(3), new Integer(2) });
            insertRow("misc", new Object[] { new Integer(4), new Integer(4) });
        }

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String    dataAsXml = stringWriter.toString();
        SAXReader reader    = new SAXReader();
        Document  testDoc   = reader.read(new InputSource(new StringReader(dataAsXml)));

        List   miscRows     = testDoc.selectNodes("//misc");
        String pkColumnName = "pk";
        String fkColumnName = "fk";

        if (miscRows.size() == 0)
        {
            miscRows     = testDoc.selectNodes("//MISC");
            pkColumnName = pkColumnName.toUpperCase();
            fkColumnName = fkColumnName.toUpperCase();
        }

        assertEquals(4, miscRows.size());
        assertEquals("1", ((Element)miscRows.get(0)).attributeValue(pkColumnName));
        assertNull(((Element)miscRows.get(0)).attributeValue(fkColumnName));
        assertEquals("2", ((Element)miscRows.get(1)).attributeValue(pkColumnName));
        assertEquals("1", ((Element)miscRows.get(1)).attributeValue(fkColumnName));
        assertEquals("3", ((Element)miscRows.get(2)).attributeValue(pkColumnName));
        assertEquals("2", ((Element)miscRows.get(2)).attributeValue(fkColumnName));
        assertEquals("4", ((Element)miscRows.get(3)).attributeValue(pkColumnName));
        assertEquals("4", ((Element)miscRows.get(3)).attributeValue(fkColumnName));

        dropDatabase();
        createDatabase(modelXml);

        StringReader stringReader = new StringReader(dataAsXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc");

        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "pk");
            assertNull(((DynaBean)beans.get(0)).get("fk"));
            assertEquals(new BigDecimal(2), beans.get(1), "pk");
            assertEquals(new BigDecimal(1), beans.get(1), "fk");
            assertEquals(new BigDecimal(3), beans.get(2), "pk");
            assertEquals(new BigDecimal(2), beans.get(2), "fk");
            assertEquals(new BigDecimal(4), beans.get(3), "pk");
            assertEquals(new BigDecimal(4), beans.get(3), "fk");
        }
        else
        {
            assertEquals(new Integer(1), beans.get(0), "pk");
            assertNull(((DynaBean)beans.get(0)).get("fk"));
            assertEquals(new Integer(2), beans.get(1), "pk");
            assertEquals(new Integer(1), beans.get(1), "fk");
            assertEquals(new Integer(3), beans.get(2), "pk");
            assertEquals(new Integer(2), beans.get(2), "fk");
            assertEquals(new Integer(4), beans.get(3), "pk");
            assertEquals(new Integer(4), beans.get(3), "fk");
        }
    }

    /**
     * Tests the backup and restore of a table with an identity column and a foreign key to
     * itself while identity override is off.
     */
    public void testSelfReferenceIdentityOverrideOn() throws Exception
    {
        if (!getPlatformInfo().isIdentityOverrideAllowed())
        {
            // TODO: for testing these platforms, we need deleteRows
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml;

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='pk' type='NUMERIC' size='12,0' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='fk' type='NUMERIC' size='12,0' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='pk' type='INTEGER' primaryKey='true' required='true' autoIncrement='true'/>\n"+
                "    <column name='fk' type='INTEGER' required='false'/>\n"+
                "    <foreign-key name='test' foreignTable='misc'>\n"+
                "      <reference local='fk' foreign='pk'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        if (isSybase)
        {
            insertRow("misc", new Object[] { new BigDecimal(10), null });
            insertRow("misc", new Object[] { new BigDecimal(11), new BigDecimal(10) });
            insertRow("misc", new Object[] { new BigDecimal(12), new BigDecimal(11) });
            insertRow("misc", new Object[] { new BigDecimal(13), new BigDecimal(13) });
        }
        else
        {
            insertRow("misc", new Object[] { new Integer(10), null });
            insertRow("misc", new Object[] { new Integer(11), new Integer(10) });
            insertRow("misc", new Object[] { new Integer(12), new Integer(11) });
            insertRow("misc", new Object[] { new Integer(13), new Integer(13) });
        }

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String    dataAsXml = stringWriter.toString();
        SAXReader reader    = new SAXReader();
        Document  testDoc   = reader.read(new InputSource(new StringReader(dataAsXml)));

        List   miscRows     = testDoc.selectNodes("//misc");
        String pkColumnName = "pk";
        String fkColumnName = "fk";

        if (miscRows.size() == 0)
        {
            miscRows     = testDoc.selectNodes("//MISC");
            pkColumnName = pkColumnName.toUpperCase();
            fkColumnName = fkColumnName.toUpperCase();
        }

        assertEquals(4, miscRows.size());
        assertEquals("10", ((Element)miscRows.get(0)).attributeValue(pkColumnName));
        assertNull(((Element)miscRows.get(0)).attributeValue(fkColumnName));
        assertEquals("11", ((Element)miscRows.get(1)).attributeValue(pkColumnName));
        assertEquals("10", ((Element)miscRows.get(1)).attributeValue(fkColumnName));
        assertEquals("12", ((Element)miscRows.get(2)).attributeValue(pkColumnName));
        assertEquals("11", ((Element)miscRows.get(2)).attributeValue(fkColumnName));
        assertEquals("13", ((Element)miscRows.get(3)).attributeValue(pkColumnName));
        assertEquals("13", ((Element)miscRows.get(3)).attributeValue(fkColumnName));

        dropDatabase();
        createDatabase(modelXml);

        StringReader stringReader = new StringReader(dataAsXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc");

        if (isSybase)
        {
            assertEquals(new BigDecimal(10), beans.get(0), "pk");
            assertNull(((DynaBean)beans.get(0)).get("fk"));
            assertEquals(new BigDecimal(11), beans.get(1), "pk");
            assertEquals(new BigDecimal(10), beans.get(1), "fk");
            assertEquals(new BigDecimal(12), beans.get(2), "pk");
            assertEquals(new BigDecimal(11), beans.get(2), "fk");
            assertEquals(new BigDecimal(13), beans.get(3), "pk");
            assertEquals(new BigDecimal(13), beans.get(3), "fk");
        }
        else
        {
            assertEquals(new Integer(10), beans.get(0), "pk");
            assertNull(((DynaBean)beans.get(0)).get("fk"));
            assertEquals(new Integer(11), beans.get(1), "pk");
            assertEquals(new Integer(10), beans.get(1), "fk");
            assertEquals(new Integer(12), beans.get(2), "pk");
            assertEquals(new Integer(11), beans.get(2), "fk");
            assertEquals(new Integer(13), beans.get(3), "pk");
            assertEquals(new Integer(13), beans.get(3), "fk");
        }
    }

    /**
     * Tests the backup and restore of a self-referencing data set.
     */
    public void testSelfReferences() throws Exception
    {
        if (!getPlatformInfo().isIdentityOverrideAllowed())
        {
            // TODO: for testing these platforms, we need deleteRows
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml;

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='id' primaryKey='true' required='true' type='NUMERIC' size='10,0' autoIncrement='true'/>\n"+
                "    <column name='parent_id' primaryKey='false' required='false' type='NUMERIC' size='10,0' autoIncrement='false'/>\n"+
                "    <foreign-key foreignTable='misc' name='misc_parent_fk'>\n"+
                "      <reference local='parent_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='id' primaryKey='true' required='true' type='SMALLINT' size='2' autoIncrement='true'/>\n"+
                "    <column name='parent_id' primaryKey='false' required='false' type='SMALLINT' size='2' autoIncrement='false'/>\n"+
                "    <foreign-key foreignTable='misc' name='misc_parent_fk'>\n"+
                "      <reference local='parent_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        final String dataXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<data>\n"+
            "  <misc id='4' parent_id='1'/>\n"+
            "  <misc id='7' parent_id='1'/>\n"+
            "  <misc id='3' parent_id='2'/>\n"+
            "  <misc id='5' parent_id='3'/>\n"+
            "  <misc id='8' parent_id='7'/>\n"+
            "  <misc id='9' parent_id='6'/>\n"+
            "  <misc id='10' parent_id='4'/>\n"+
            "  <misc id='1'/>\n"+
            "  <misc id='2'/>\n"+
            "  <misc id='6'/>\n"+
            "  <misc id='11'/>\n"+
            "  <misc id='12' parent_id='11'/>\n"+
            "</data>";

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        DatabaseDataIO dataIO       = new DatabaseDataIO();
        StringReader   stringReader = new StringReader(dataXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc", "id");

        if (isSybase)
        {
            assertEquals(12, beans.size());
            assertEquals(new BigDecimal(1),  beans.get(0), "id");
            assertNull(((DynaBean)beans.get(0)).get("parent_id"));
            assertEquals(new BigDecimal(2),  beans.get(1), "id");
            assertNull(((DynaBean)beans.get(1)).get("parent_id"));
            assertEquals(new BigDecimal(3),  beans.get(2), "id");
            assertEquals(new BigDecimal(2),  beans.get(2), "parent_id");
            assertEquals(new BigDecimal(4),  beans.get(3), "id");
            assertEquals(new BigDecimal(1),  beans.get(3), "parent_id");
            assertEquals(new BigDecimal(5),  beans.get(4), "id");
            assertEquals(new BigDecimal(3),  beans.get(4), "parent_id");
            assertEquals(new BigDecimal(6),  beans.get(5), "id");
            assertNull(((DynaBean)beans.get(5)).get("parent_id"));
            assertEquals(new BigDecimal(7),  beans.get(6), "id");
            assertEquals(new BigDecimal(1),  beans.get(6), "parent_id");
            assertEquals(new BigDecimal(8),  beans.get(7), "id");
            assertEquals(new BigDecimal(7),  beans.get(7), "parent_id");
            assertEquals(new BigDecimal(9),  beans.get(8), "id");
            assertEquals(new BigDecimal(6),  beans.get(8), "parent_id");
            assertEquals(new BigDecimal(10), beans.get(9), "id");
            assertEquals(new BigDecimal(4),  beans.get(9), "parent_id");
            assertEquals(new BigDecimal(11), beans.get(10), "id");
            assertNull(((DynaBean)beans.get(10)).get("parent_id"));
            assertEquals(new BigDecimal(12), beans.get(11), "id");
            assertEquals(new BigDecimal(11), beans.get(11), "parent_id");
        }
        else
        {
            assertEquals(12, beans.size());
            assertEquals(new Integer(1),  beans.get(0), "id");
            assertNull(((DynaBean)beans.get(0)).get("parent_id"));
            assertEquals(new Integer(2),  beans.get(1), "id");
            assertNull(((DynaBean)beans.get(1)).get("parent_id"));
            assertEquals(new Integer(3),  beans.get(2), "id");
            assertEquals(new Integer(2),  beans.get(2), "parent_id");
            assertEquals(new Integer(4),  beans.get(3), "id");
            assertEquals(new Integer(1),  beans.get(3), "parent_id");
            assertEquals(new Integer(5),  beans.get(4), "id");
            assertEquals(new Integer(3),  beans.get(4), "parent_id");
            assertEquals(new Integer(6),  beans.get(5), "id");
            assertNull(((DynaBean)beans.get(5)).get("parent_id"));
            assertEquals(new Integer(7),  beans.get(6), "id");
            assertEquals(new Integer(1),  beans.get(6), "parent_id");
            assertEquals(new Integer(8),  beans.get(7), "id");
            assertEquals(new Integer(7),  beans.get(7), "parent_id");
            assertEquals(new Integer(9),  beans.get(8), "id");
            assertEquals(new Integer(6),  beans.get(8), "parent_id");
            assertEquals(new Integer(10), beans.get(9), "id");
            assertEquals(new Integer(4),  beans.get(9), "parent_id");
            assertEquals(new Integer(11), beans.get(10), "id");
            assertNull(((DynaBean)beans.get(10)).get("parent_id"));
            assertEquals(new Integer(12), beans.get(11), "id");
            assertEquals(new Integer(11), beans.get(11), "parent_id");
        }
    }

    /**
     * Tests the backup and restore of a self-referencing data set (with multiple self references
     * in the same table).
     */
    public void testMultiSelfReferences() throws Exception
    {
        if (!getPlatformInfo().isIdentityOverrideAllowed())
        {
            // TODO: for testing these platforms, we need deleteRows
            return;
        }

        // Sybase does not like INTEGER auto-increment columns
        boolean      isSybase = SybasePlatform.DATABASENAME.equals(getPlatform().getName());
        final String modelXml;

        if (isSybase)
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='id' primaryKey='true' required='true' type='NUMERIC' size='6,0' autoIncrement='true'/>\n"+
                "    <column name='left_id' primaryKey='false' required='false' type='NUMERIC' size='6,0' autoIncrement='false'/>\n"+
                "    <column name='right_id' primaryKey='false' required='false' type='NUMERIC' size='6,0' autoIncrement='false'/>\n"+
                "    <foreign-key foreignTable='misc' name='misc_left_fk'>\n"+
                "      <reference local='left_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "    <foreign-key foreignTable='misc' name='misc_right_fk'>\n"+
                "      <reference local='right_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }
        else
        {
            modelXml = 
                "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
                "<database name='roundtriptest'>\n"+
                "  <table name='misc'>\n"+
                "    <column name='id' primaryKey='true' required='true' type='SMALLINT' size='2' autoIncrement='true'/>\n"+
                "    <column name='left_id' primaryKey='false' required='false' type='SMALLINT' size='2' autoIncrement='false'/>\n"+
                "    <column name='right_id' primaryKey='false' required='false' type='SMALLINT' size='2' autoIncrement='false'/>\n"+
                "    <foreign-key foreignTable='misc' name='misc_left_fk'>\n"+
                "      <reference local='left_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "    <foreign-key foreignTable='misc' name='misc_right_fk'>\n"+
                "      <reference local='right_id' foreign='id'/>\n"+
                "    </foreign-key>\n"+
                "  </table>\n"+
                "</database>";
        }

        final String dataXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<data>\n"+
            "  <misc id='1' left_id='2' right_id='3'/>\n"+
            "  <misc id='3' left_id='2' right_id='4'/>\n"+
            "  <misc id='2' left_id='5' right_id='4'/>\n"+
            "  <misc id='5' right_id='6'/>\n"+
            "  <misc id='6'/>\n"+
            "  <misc id='4' left_id='6'/>\n"+
            "</data>";

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        DatabaseDataIO dataIO       = new DatabaseDataIO();
        StringReader   stringReader = new StringReader(dataXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        List beans = getRows("misc", "id");

        assertEquals(6, beans.size());
        if (isSybase)
        {
            assertEquals(new BigDecimal(1), beans.get(0), "id");
            assertEquals(new BigDecimal(2), beans.get(0), "left_id");
            assertEquals(new BigDecimal(3), beans.get(0), "right_id");
            assertEquals(new BigDecimal(2), beans.get(1), "id");
            assertEquals(new BigDecimal(5), beans.get(1), "left_id");
            assertEquals(new BigDecimal(4), beans.get(1), "right_id");
            assertEquals(new BigDecimal(3), beans.get(2), "id");
            assertEquals(new BigDecimal(2), beans.get(2), "left_id");
            assertEquals(new BigDecimal(4), beans.get(2), "right_id");
            assertEquals(new BigDecimal(4), beans.get(3), "id");
            assertEquals(new BigDecimal(6), beans.get(3), "left_id");
            assertEquals((Object)null,      beans.get(3), "right_id");
            assertEquals(new BigDecimal(5), beans.get(4), "id");
            assertEquals((Object)null,      beans.get(4), "left_id");
            assertEquals(new BigDecimal(6), beans.get(4), "right_id");
            assertEquals(new BigDecimal(6), beans.get(5), "id");
            assertEquals((Object)null,      beans.get(5), "left_id");
            assertEquals((Object)null,      beans.get(5), "right_id");
        }
        else
        {
            assertEquals(new Integer(1), beans.get(0), "id");
            assertEquals(new Integer(2), beans.get(0), "left_id");
            assertEquals(new Integer(3), beans.get(0), "right_id");
            assertEquals(new Integer(2), beans.get(1), "id");
            assertEquals(new Integer(5), beans.get(1), "left_id");
            assertEquals(new Integer(4), beans.get(1), "right_id");
            assertEquals(new Integer(3), beans.get(2), "id");
            assertEquals(new Integer(2), beans.get(2), "left_id");
            assertEquals(new Integer(4), beans.get(2), "right_id");
            assertEquals(new Integer(4), beans.get(3), "id");
            assertEquals(new Integer(6), beans.get(3), "left_id");
            assertEquals((Object)null,   beans.get(3), "right_id");
            assertEquals(new Integer(5), beans.get(4), "id");
            assertEquals((Object)null,   beans.get(4), "left_id");
            assertEquals(new Integer(6), beans.get(4), "right_id");
            assertEquals(new Integer(6), beans.get(5), "id");
            assertEquals((Object)null,   beans.get(5), "left_id");
            assertEquals((Object)null,   beans.get(5), "right_id");
        }
    }

    /**
     * Tests the backup and restore of several tables with complex relationships with an identity column and a foreign key to
     * itself while identity override is off.
     */
    public void testComplexTableModel() throws Exception
    {
        // A: self-reference (A1->A2)
        // B: self- and foreign-reference (B1->B2|G1, B2->G2)
        // C: circular reference involving more than one table (C1->D1,C2->D2)
        // D: foreign-reference to F (D1->F1,D2)
        // E: isolated table (E1)
        // F: foreign-reference to C (F1->C2)
        // G: no references (G1, G2)

        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='roundtriptest'>\n"+
            "  <table name='A'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key name='AtoA' foreignTable='A'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='B'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk1' type='INTEGER' required='false'/>\n"+
            "    <column name='fk2' type='INTEGER' required='false'/>\n"+
            "    <foreign-key name='BtoB' foreignTable='B'>\n"+
            "      <reference local='fk1' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "    <foreign-key name='BtoG' foreignTable='G'>\n"+
            "      <reference local='fk2' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='C'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key name='CtoD' foreignTable='D'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='D'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key name='DtoF' foreignTable='F'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='E'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "  <table name='F'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "    <column name='fk' type='INTEGER' required='false'/>\n"+
            "    <foreign-key name='FtoC' foreignTable='C'>\n"+
            "      <reference local='fk' foreign='pk'/>\n"+
            "    </foreign-key>\n"+
            "  </table>\n"+
            "  <table name='G'>\n"+
            "    <column name='pk' type='INTEGER' primaryKey='true' required='true'/>\n"+
            "  </table>\n"+
            "</database>";

        createDatabase(modelXml);

        getPlatform().setIdentityOverrideOn(true);

        // this is the optimal insertion order
        insertRow("E", new Object[] { new Integer(1) });
        insertRow("G", new Object[] { new Integer(1) });
        insertRow("G", new Object[] { new Integer(2) });
        insertRow("A", new Object[] { new Integer(2), null });
        insertRow("A", new Object[] { new Integer(1), new Integer(2) });
        insertRow("B", new Object[] { new Integer(2), null,           new Integer(2) });
        insertRow("B", new Object[] { new Integer(1), new Integer(2), new Integer(1) });
        insertRow("D", new Object[] { new Integer(2), null });
        insertRow("C", new Object[] { new Integer(2), new Integer(2) });
        insertRow("F", new Object[] { new Integer(1), new Integer(2) });
        insertRow("D", new Object[] { new Integer(1), new Integer(1) });
        insertRow("C", new Object[] { new Integer(1), new Integer(1) });

        StringWriter   stringWriter = new StringWriter();
        DatabaseDataIO dataIO       = new DatabaseDataIO();

        dataIO.writeDataToXML(getPlatform(), getModel(), stringWriter, "UTF-8");

        String dataAsXml = stringWriter.toString();

        // the somewhat optimized order that DdlUtils currently generates is:
        // E1, G1, G2, A2, A1, B2, B1, C2, C1, D2, D1, F1
        // note that the order per table is the insertion order above
        SAXReader reader       = new SAXReader();
        Document  testDoc      = reader.read(new InputSource(new StringReader(dataAsXml)));
        boolean   uppercase    = false;
        List      rows         = testDoc.selectNodes("/*/*");
        String    pkColumnName = "pk";

        assertEquals(12, rows.size());
        if (!"e".equals(((Element)rows.get(0)).getName()))
        {
            assertEquals("E", ((Element)rows.get(0)).getName());
            uppercase    = true;
        }
        if (!"pk".equals(((Element)rows.get(0)).attribute(0).getName()))
        {
            pkColumnName = pkColumnName.toUpperCase();
        }
        assertEquals("1", ((Element)rows.get(0)).attributeValue(pkColumnName));

        // we cannot be sure of the order in which the database returns the rows
        // per table (some return them in pk order, some in insertion order)
        // so we don't assume an order in this test
        HashSet pkValues       = new HashSet();
        HashSet expectedValues = new HashSet(Arrays.asList(new String[] { "1", "2" }));

        assertEquals(uppercase ? "G" : "g", ((Element)rows.get(1)).getName());
        assertEquals(uppercase ? "G" : "g", ((Element)rows.get(2)).getName());
        pkValues.add(((Element)rows.get(1)).attributeValue(pkColumnName));
        pkValues.add(((Element)rows.get(2)).attributeValue(pkColumnName));
        assertEquals(pkValues, expectedValues);

        pkValues.clear();
        
        assertEquals(uppercase ? "A" : "a", ((Element)rows.get(3)).getName());
        assertEquals(uppercase ? "A" : "a", ((Element)rows.get(4)).getName());
        pkValues.add(((Element)rows.get(3)).attributeValue(pkColumnName));
        pkValues.add(((Element)rows.get(4)).attributeValue(pkColumnName));
        assertEquals(pkValues, expectedValues);

        pkValues.clear();

        assertEquals(uppercase ? "B" : "b", ((Element)rows.get(5)).getName());
        assertEquals(uppercase ? "B" : "b", ((Element)rows.get(6)).getName());
        pkValues.add(((Element)rows.get(5)).attributeValue(pkColumnName));
        pkValues.add(((Element)rows.get(6)).attributeValue(pkColumnName));
        assertEquals(pkValues, expectedValues);

        pkValues.clear();

        assertEquals(uppercase ? "C" : "c", ((Element)rows.get(7)).getName());
        assertEquals(uppercase ? "C" : "c", ((Element)rows.get(8)).getName());
        pkValues.add(((Element)rows.get(7)).attributeValue(pkColumnName));
        pkValues.add(((Element)rows.get(8)).attributeValue(pkColumnName));
        assertEquals(pkValues, expectedValues);

        pkValues.clear();

        assertEquals(uppercase ? "D" : "d", ((Element)rows.get(9)).getName());
        assertEquals(uppercase ? "D" : "d", ((Element)rows.get(10)).getName());
        pkValues.add(((Element)rows.get(9)).attributeValue(pkColumnName));
        pkValues.add(((Element)rows.get(10)).attributeValue(pkColumnName));
        assertEquals(pkValues, expectedValues);

        pkValues.clear();

        assertEquals(uppercase ? "F" : "f", ((Element)rows.get(11)).getName());
        assertEquals("1", ((Element)rows.get(11)).attributeValue(pkColumnName));

        dropDatabase();
        createDatabase(modelXml);

        StringReader stringReader = new StringReader(dataAsXml);

        dataIO.writeDataToDatabase(getPlatform(), getModel(), new Reader[] { stringReader });

        assertEquals(2, getRows("A").size());
        assertEquals(2, getRows("B").size());
        assertEquals(2, getRows("C").size());
        assertEquals(2, getRows("D").size());
        assertEquals(1, getRows("E").size());
        assertEquals(1, getRows("F").size());
        assertEquals(2, getRows("G").size());
    }

}
