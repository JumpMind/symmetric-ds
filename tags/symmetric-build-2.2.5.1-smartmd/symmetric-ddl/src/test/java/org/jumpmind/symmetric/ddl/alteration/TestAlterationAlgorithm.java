package org.jumpmind.symmetric.ddl.alteration;

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

import java.io.IOException;
import java.io.StringWriter;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.TestBase;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.platform.TestPlatform;

/**
 * Tests the generation of the alteration statements.
 * 
 * @version $Revision: $
 */
public class TestAlterationAlgorithm extends TestBase
{
    /** The tested platform. */
    private Platform _platform;
    /** The writer that the builder of the platform writes to. */
    private StringWriter _writer;

    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        _writer   = new StringWriter();
        _platform = new TestPlatform();
        _platform.getSqlBuilder().setWriter(_writer);
        _platform.setSqlCommentsOn(false);
        _platform.setDelimitedIdentifierModeOn(true);
    }

    /**
     * {@inheritDoc}
     */
    protected void tearDown() throws Exception
    {
        _platform = null;
        _writer   = null;
    }

    /**
     * Returns the SQL for altering the first into the second database.
     * 
     * @param currentSchema The current schema XML 
     * @param desiredSchema The desired schema XML 
     * @return The sql
     */
    protected String getAlterDatabaseSQL(String currentSchema, String desiredSchema) throws IOException
    {
        Database currentModel = parseDatabaseFromString(currentSchema);
        Database desiredModel = parseDatabaseFromString(desiredSchema);

        _platform.getSqlBuilder().alterDatabase(currentModel, desiredModel, null);

        return _writer.toString();
    }

    /**
     * Test where no change is made to the model.
     */
    public void testNoChange() throws IOException
    {
        final String modelXml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER' required='true'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEA'>\n" +
            "      <reference local='COLFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='COLFK'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "",
            getAlterDatabaseSQL(modelXml, modelXml));
    }

    /**
     * Tests the addition of a table.
     */
    public void testAddTable() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a table that has an index.
     */
    public void testAddTableWithIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='COL'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COL\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TESTINDEX\" ON \"TABLEB\" (\"COL\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a table that has a unique index.
     */
    public void testAddTableWithUniqueIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='COL'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COL\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "CREATE UNIQUE INDEX \"TESTINDEX\" ON \"TABLEB\" (\"COL\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a table that has a foreign key to an existing one.
     */
    public void testAddTableWithForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of two tables that have foreign key to each other.
     */
    public void testAddTablesWithForeignKeys() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEB'>\n" +
            "      <reference local='ColFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"ColFK\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" DOUBLE NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "ALTER TABLE \"TableA\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"ColFK\") REFERENCES \"TABLEB\" (\"COLPK\");\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a table.
     */
    public void testRemoveTable() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP TABLE \"TableA\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a table with an index.
     */
    public void testRemoveTableWithIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP TABLE \"TableA\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a table with a foreign key to an existing table.
     */
    public void testRemoveTableWithForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='VARCHAR' size='64'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEB'>\n" +
            "      <reference local='ColFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TableA\" DROP CONSTRAINT \"TESTFK\";\n"+
            "DROP TABLE \"TableA\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a table that is referenced by a foreign key of an existing table.
     */
    public void testRemoveTableReferencedByForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='VARCHAR' size='64'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEB'>\n" +
            "      <reference local='ColFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TableA\" DROP CONSTRAINT \"TESTFK\";\n"+
            "DROP TABLE \"TABLEB\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of two tables that have foreign key to each other.
     */
    public void testRemoveTablesWithForeignKeys() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEB'>\n" +
            "      <reference local='ColFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TableA\" DROP CONSTRAINT \"TESTFK\";\n"+
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "DROP TABLE \"TABLEB\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an index to an existing table.
     */
    public void testAddIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an unique index to an existing table.
     */
    public void testAddUniqueIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TestIndex'>\n" +
            "      <unique-column name='Col'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE UNIQUE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of an index from a table.
     */
    public void testRemoveIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TestIndex\" ON \"TableA\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of an unique index from a table.
     */
    public void testRemoveUniqueIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TestIndex'>\n" +
            "      <unique-column name='Col'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TestIndex\" ON \"TableA\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a primary key to an existing table.
     */
    public void testAddPrimaryKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TableA\" ADD CONSTRAINT \"TableA_PK\" PRIMARY KEY (\"ColPK1\",\"ColPK2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a primary key and a column to an existing table.
     */
    public void testAddPrimaryKeyAndColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64) NOT NULL,\n"+
            "    \"Col\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64) NOT NULL,\n"+
            "    \"Col\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\",\"Col\") SELECT \"ColPK1\",\"ColPK2\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a primary key from an existing table.
     */
    public void testRemovePrimaryKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' size='64' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64) NOT NULL\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\", \"ColPK2\") SELECT \"ColPK1\", \"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64) NOT NULL\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\", \"ColPK2\") SELECT \"ColPK1\", \"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a column to an existing table.
     */
    public void testAddColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\", \"Col\") SELECT \"ColPK\", \"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a column from an existing table.
     */
    public void testRemoveColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a primary key column to an existing table.
     */
    public void testAddPrimaryKeyColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' primaryKey='true' size='64'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\", \"ColPK2\") SELECT \"ColPK1\", \"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a primary key column from an existing table.
     */
    public void testRemovePrimaryKeyColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='VARCHAR' primaryKey='true' size='64'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of columns to the primary key of a table and the foreign key
     * of another table referencing it.
     */
    public void testAddColumnsToPrimaryAndForeignKeys() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK1' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='INTEGER'/>\n" +
            "    <column name='COLFK2' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK1' foreign='ColPK1'/>\n" +
            "      <reference local='COLFK2' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" DOUBLE NOT NULL,\n"+
            "    \"COLFK1\" INTEGER,\n"+
            "    \"COLFK2\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK1\") SELECT \"COLPK\",\"COLFK1\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" DOUBLE NOT NULL,\n"+
            "    \"COLFK1\" INTEGER,\n"+
            "    \"COLFK2\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK1\",\"COLFK2\") SELECT \"COLPK\",\"COLFK1\",\"COLFK2\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK1\",\"COLFK2\") REFERENCES \"TableA\" (\"ColPK1\",\"ColPK2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of columns from the primary key of a table and the foreign key
     * of another table referencing it.
     */
    public void testRemoveColumnsFromPrimaryAndForeignKeys() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='INTEGER'/>\n" +
            "    <column name='COLFK2' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK1' foreign='ColPK1'/>\n" +
            "      <reference local='COLFK2' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK1' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\") SELECT \"ColPK1\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" DOUBLE NOT NULL,\n"+
            "    \"COLFK1\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK1\") SELECT \"COLPK\",\"COLFK1\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" DOUBLE NOT NULL,\n"+
            "    \"COLFK1\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK1\") SELECT \"COLPK\",\"COLFK1\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK1\") REFERENCES \"TableA\" (\"ColPK1\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an index column to an existing table.
     */
    public void testAddIndexColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col1\",\"Col2\") SELECT \"ColPK\",\"Col1\",\"Col2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\",\"Col2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of an index column from an existing table.
     */
    public void testRemoveIndexColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an unique index column to an existing table.
     */
    public void testAddUniqueIndexColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "      <unique-column name='Col2'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col1\",\"Col2\") SELECT \"ColPK\",\"Col1\",\"Col2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE UNIQUE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\",\"Col2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of an unique index column from an existing table.
     */
    public void testRemoveUniqueIndexColumn() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "      <unique-column name='Col2'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE UNIQUE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }


    /**
     * Tests the addition of a column to a table with an index.
     */
    public void testAddColumnToTableWithIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col1\") SELECT \"ColPK\",\"Col1\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col1\" DOUBLE,\n"+
            "    \"Col2\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col1\",\"Col2\") SELECT \"ColPK\",\"Col1\",\"Col2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a column to a table that has a foreign key.
     */
    public void testAddColumnToTableWithForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <column name='COL' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    \"COL\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    \"COL\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK\",\"COL\") SELECT \"COLPK\",\"COLFK\",\"COL\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of a column to a table that is referenced by a foreign key.
     */
    public void testAddColumnToTableReferencedByForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an existing column to a primary key.
     */
    public void testAddExistingColumnToPrimaryKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a column from a primary key.
     */
    public void testRemoveColumnFromPrimaryKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of existing columns to a primary and the referencing foreign key.
     */
    public void testAddExistingColumnsToPrimaryAndForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='DOUBLE'/>\n" +
            "    <column name='COLFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK2' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='DOUBLE'/>\n" +
            "    <column name='COLFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK2' foreign='ColPK1'/>\n" +
            "      <reference local='COLFK1' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\",\"ColPK2\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK2\",\"COLFK1\") REFERENCES \"TableA\" (\"ColPK1\",\"ColPK2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of existing columns from a primary and the referencing foreign key.
     */
    public void testRemoveExistingColumnsFromPrimaryAndForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='DOUBLE'/>\n" +
            "    <column name='COLFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK2' foreign='ColPK1'/>\n" +
            "      <reference local='COLFK1' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='DOUBLE' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK1' type='DOUBLE'/>\n" +
            "    <column name='COLFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK2' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK1\" INTEGER NOT NULL,\n"+
            "    \"ColPK2\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK1\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK1\",\"ColPK2\") SELECT \"ColPK1\",\"ColPK2\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK2\") REFERENCES \"TableA\" (\"ColPK1\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an existing column to an index.
     */
    public void testAddExistingColumnToIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <unique name='TESTINDEX'>\n" +
            "      <unique-column name='Col1'/>\n" +
            "      <unique-column name='Col2'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE UNIQUE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\",\"Col2\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the addition of an existing column from an index.
     */
    public void testRemoveExistingColumnFromIndex() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "    <column name='Col2' type='VARCHAR' size='64'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "DROP INDEX \"TESTINDEX\" ON \"TableA\";\n"+
            "CREATE INDEX \"TESTINDEX\" ON \"TableA\" (\"Col1\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the removal of a column from a table referenced by a foreign key.
     */
    public void testRemoveColumnFromTableReferencedByForeignKey() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a column's datatype.
     */
    public void testChangeColumnDatatype() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a primary key column's datatype.
     */
    public void testChangePrimaryKeyColumnDatatype() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" DOUBLE NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" DOUBLE NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a index column's datatype.
     */
    public void testChangeIndexColumnDatatype() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of the datatype of the columns of a primary key and the referencing foreign key.
     */
    public void testChangePrimaryAndForeignKeyColumnsDatatype() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DOUBLE' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='DOUBLE'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" DOUBLE NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" DOUBLE,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a column's size.
     */
    public void testChangeColumnSize() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='32' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='64' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(64) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(64) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a primary key column's size.
     */
    public void testChangePrimaryKeyColumnSize() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DECIMAL' size='15,2' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DECIMAL' size='30,4' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" DECIMAL(30,4) NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" DECIMAL(30,4) NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a index column's size.
     */
    public void testChangeIndexColumnSize() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DECIMAL' size='10,4' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DECIMAL' size='15,2' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DECIMAL(15,2) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DECIMAL(15,2) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of the size of the columns of a primary key and the referencing foreign key.
     */
    public void testChangePrimaryAndForeignKeyColumnsSize() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='VARCHAR' size='32'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='VARCHAR' size='64' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='VARCHAR' size='64'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" VARCHAR(64) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" VARCHAR(64) NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" VARCHAR(64),\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a column's default value.
     */
    public void testChangeColumnDefault() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='32' default='test 1' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='32' default='test 2' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(32) DEFAULT 'test 2' NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" VARCHAR(32) DEFAULT 'test 2' NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a primary key column's default value.
     */
    public void testChangePrimaryKeyColumnDefault() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DECIMAL' size='15,2' default='2.0' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='DECIMAL' size='15,2' default='4.0' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" DECIMAL(15,2) DEFAULT 4.0 NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" DECIMAL(15,2) DEFAULT 4.0 NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a index column's default value.
     */
    public void testChangeIndexColumnDefault() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DATE' default='2000-01-02' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DATE' default='2001-02-03' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DATE DEFAULT '2001-02-03' NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" DATE DEFAULT '2001-02-03' NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of the default value of the columns of a primary key and the referencing foreign key.
     */
    public void testChangePrimaryAndForeignKeyColumnsDefault() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' default='0' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER' default='1'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' default='1' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER' default='0'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER DEFAULT 1 NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER DEFAULT 1 NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER DEFAULT 0,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER DEFAULT 0,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a column's auto-increment attribute.
     */
    public void testChangeColumnAutoIncrement() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' autoIncrement='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' autoIncrement='false' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a primary key column's auto-increment attribute.
     */
    public void testChangePrimaryKeyColumnAutoIncrement() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' autoIncrement='false' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' autoIncrement='true' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL IDENTITY,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL IDENTITY,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a index column's auto-increment attribute.
     */
    public void testChangeIndexColumnAutoIncrement() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' autoIncrement='false' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' autoIncrement='true' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL IDENTITY,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL IDENTITY,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of the auto-increment attribute of the columns of a primary key
     * and the referencing foreign key.
     */
    public void testChangePrimaryAndForeignKeyColumnsAutoIncrement() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' autoIncrement='true' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' autoIncrement='false' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a column's required attribute.
     */
    public void testChangeColumnRequired() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='false'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a primary key column's required attribute.
     */
    public void testChangePrimaryKeyColumnRequired() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='false'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of a index column's required attribute.
     */
    public void testChangeIndexColumnRequired() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='false'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='true'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    \"Col\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "CREATE INDEX \"TestIndex\" ON \"TableA\" (\"Col\");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\",\"Col\") SELECT \"ColPK\",\"Col\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }

    /**
     * Tests the change of the required attribute of the columns of a primary key
     * and the referencing foreign key.
     */
    public void testChangePrimaryAndForeignKeyColumnsRequired() throws IOException
    {
        final String model1Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='false'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER' required='true'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String model2Xml = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER' required='false'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='COLFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"TABLEB\" DROP CONSTRAINT \"TESTFK\";\n"+
            "CREATE TABLE \"TableA_\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA_\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA\";\n"+
            "DROP TABLE \"TableA\";\n"+
            "CREATE TABLE \"TableA\"\n"+
            "(\n"+
            "    \"ColPK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"ColPK\")\n"+
            ");\n"+
            "INSERT INTO \"TableA\" (\"ColPK\") SELECT \"ColPK\" FROM \"TableA_\";\n"+
            "DROP TABLE \"TableA_\";\n"+
            "CREATE TABLE \"TABLEB_\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB_\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB\";\n"+
            "DROP TABLE \"TABLEB\";\n"+
            "CREATE TABLE \"TABLEB\"\n"+
            "(\n"+
            "    \"COLPK\" INTEGER NOT NULL,\n"+
            "    \"COLFK\" INTEGER,\n"+
            "    PRIMARY KEY (\"COLPK\")\n"+
            ");\n"+
            "INSERT INTO \"TABLEB\" (\"COLPK\",\"COLFK\") SELECT \"COLPK\",\"COLFK\" FROM \"TABLEB_\";\n"+
            "DROP TABLE \"TABLEB_\";\n"+
            "ALTER TABLE \"TABLEB\" ADD CONSTRAINT \"TESTFK\" FOREIGN KEY (\"COLFK\") REFERENCES \"TableA\" (\"ColPK\");\n",
            getAlterDatabaseSQL(model1Xml, model2Xml));
    }
}
