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

import org.jumpmind.symmetric.ddl.TestPlatformBase;
import org.jumpmind.symmetric.ddl.platform.mckoi.MckoiPlatform;

/**
 * Tests the McKoi platform.
 * 
 * @version $Revision: 231110 $
 */
public class TestMcKoiPlatform extends TestPlatformBase
{
    /** The database schema for testing table constraints, ie. foreign keys and indices.
        This schema is adapted for McKoi which does not support non-unique indices. */
    public static final String TABLE_CONSTRAINT_TEST_SCHEMA =
        "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
        "<database name='tableconstraintstest'>\n" +
        "  <table name='table1'>\n" +
        "    <column name='COL_PK_1' type='VARCHAR' size='32' primaryKey='true' required='true'/>\n" +
        "    <column name='COL_PK_2' type='INTEGER' primaryKey='true'/>\n" +
        "    <column name='COL_INDEX_1' type='BINARY' size='100' required='true'/>\n" +
        "    <column name='COL_INDEX_2' type='DOUBLE' required='true'/>\n" +
        "    <column name='COL_INDEX_3' type='CHAR' size='4'/>\n" +
        "    <unique name='testindex1'>\n" +
        "      <unique-column name='COL_INDEX_2'/>\n" +
        "    </unique>\n" +
        "    <unique name='testindex2'>\n" +
        "      <unique-column name='COL_INDEX_3'/>\n" +
        "      <unique-column name='COL_INDEX_1'/>\n" +
        "    </unique>\n" +
        "  </table>\n" +
        "  <table name='table2'>\n" +
        "    <column name='COL_PK' type='INTEGER' primaryKey='true'/>\n" +
        "    <column name='COL_FK_1' type='INTEGER'/>\n" +
        "    <column name='COL_FK_2' type='VARCHAR' size='32' required='true'/>\n" +
        "    <foreign-key foreignTable='table1'>\n" +
        "      <reference local='COL_FK_1' foreign='COL_PK_2'/>\n" +
        "      <reference local='COL_FK_2' foreign='COL_PK_1'/>\n" +
        "    </foreign-key>\n" +
        "  </table>\n" +
        "  <table name='table3'>\n" +
        "    <column name='COL_PK' type='VARCHAR' size='16' primaryKey='true'/>\n" +
        "    <column name='COL_FK' type='INTEGER' required='true'/>\n" +
        "    <foreign-key name='testfk' foreignTable='table2'>\n" +
        "      <reference local='COL_FK' foreign='COL_PK'/>\n" +
        "    </foreign-key>\n" +
        "  </table>\n" +
        "</database>";
    /** The database schema for testing escaping of character sequences. */
    public static final String COLUMN_CHAR_SEQUENCES_TO_ESCAPE =
        "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
        "<database name='escapetest'>\n" +
        "  <table name='escapedcharacters'>\n" +
        "    <column name='COL_PK' type='INTEGER' primaryKey='true'/>\n" +
        "    <column name='COL_TEXT' type='VARCHAR' size='128' default='&#39; \\'/>\n" +
        "  </table>\n" +
        "</database>";

    /**
     * {@inheritDoc}
     */
    protected String getDatabaseName()
    {
        return MckoiPlatform.DATABASENAME;
    }

    /**
     * Tests the column types.
     */
    public void testColumnTypes() throws Exception
    {
        assertEqualsIgnoringWhitespaces(
            "DROP TABLE IF EXISTS \"coltype\";\n"+
            "CREATE TABLE \"coltype\"\n"+
            "(\n"+
            "    \"COL_ARRAY\"           BLOB,\n"+
            "    \"COL_BIGINT\"          BIGINT,\n"+
            "    \"COL_BINARY\"          BINARY(1024),\n"+
            "    \"COL_BIT\"             BOOLEAN,\n"+
            "    \"COL_BLOB\"            BLOB,\n"+
            "    \"COL_BOOLEAN\"         BOOLEAN,\n"+
            "    \"COL_CHAR\"            CHAR(15),\n"+
            "    \"COL_CLOB\"            CLOB,\n"+
            "    \"COL_DATALINK\"        BLOB,\n"+
            "    \"COL_DATE\"            DATE,\n"+
            "    \"COL_DECIMAL\"         DECIMAL(15,3),\n"+
            "    \"COL_DECIMAL_NOSCALE\" DECIMAL(15,0),\n"+
            "    \"COL_DISTINCT\"        BLOB,\n"+
            "    \"COL_DOUBLE\"          DOUBLE,\n"+
            "    \"COL_FLOAT\"           DOUBLE,\n"+
            "    \"COL_INTEGER\"         INTEGER,\n"+
            "    \"COL_JAVA_OBJECT\"     JAVA_OBJECT,\n"+
            "    \"COL_LONGVARBINARY\"   LONGVARBINARY,\n"+
            "    \"COL_LONGVARCHAR\"     LONGVARCHAR,\n"+
            "    \"COL_NULL\"            BLOB,\n"+
            "    \"COL_NUMERIC\"         NUMERIC(15,0),\n"+
            "    \"COL_OTHER\"           BLOB,\n"+
            "    \"COL_REAL\"            REAL,\n"+
            "    \"COL_REF\"             BLOB,\n"+
            "    \"COL_SMALLINT\"        SMALLINT,\n"+
            "    \"COL_STRUCT\"          BLOB,\n"+
            "    \"COL_TIME\"            TIME,\n"+
            "    \"COL_TIMESTAMP\"       TIMESTAMP,\n"+
            "    \"COL_TINYINT\"         TINYINT,\n"+
            "    \"COL_VARBINARY\"       VARBINARY(15),\n"+
            "    \"COL_VARCHAR\"         VARCHAR(15)\n"+
            ");\n",
            createTestDatabase(COLUMN_TEST_SCHEMA));
    }

    /**
     * Tests the column constraints.
     */
    public void testColumnConstraints() throws Exception
    {
        // note that this is not valid SQL as obviously only one auto increment field
        // can be defined for each table
        assertEqualsIgnoringWhitespaces(
            "DROP TABLE IF EXISTS \"constraints\";\n"+
            "DROP SEQUENCE \"seq_constraints_COL_PK_AUTO_INCR\";\n"+
            "DROP SEQUENCE \"seq_constraints_COL_AUTO_INCR\";\n"+
            "CREATE SEQUENCE \"seq_constraints_COL_PK_AUTO_INCR\";\n"+
            "CREATE SEQUENCE \"seq_constraints_COL_AUTO_INCR\";\n"+
            "CREATE TABLE \"constraints\"\n"+
            "(\n"+
            "    \"COL_PK\"               VARCHAR(32),\n"+
            "    \"COL_PK_AUTO_INCR\"     INTEGER DEFAULT NEXTVAL('seq_constraints_COL_PK_AUTO_INCR'),\n"+
            "    \"COL_NOT_NULL\"         BINARY(100) NOT NULL,\n"+
            "    \"COL_NOT_NULL_DEFAULT\" DOUBLE DEFAULT -2.0 NOT NULL,\n"+
            "    \"COL_DEFAULT\"          CHAR(4) DEFAULT 'test',\n"+
            "    \"COL_AUTO_INCR\"        BIGINT DEFAULT NEXTVAL('seq_constraints_COL_AUTO_INCR'),\n"+
            "    PRIMARY KEY (\"COL_PK\", \"COL_PK_AUTO_INCR\")\n"+
            ");\n",
            createTestDatabase(COLUMN_CONSTRAINT_TEST_SCHEMA));
    }

    /**
     * Tests the table constraints.
     */
    public void testTableConstraints() throws Exception
    {
        assertEqualsIgnoringWhitespaces(
            "ALTER TABLE \"table3\" DROP CONSTRAINT \"testfk\";\n"+
            "ALTER TABLE \"table2\" DROP CONSTRAINT \"table2_FK_COL_FK_1_COL_FK_2_table1\";\n"+
            "DROP TABLE IF EXISTS \"table3\";\n"+
            "DROP TABLE IF EXISTS \"table2\";\n"+
            "DROP TABLE IF EXISTS \"table1\";\n"+
            "CREATE TABLE \"table1\"\n"+
            "(\n"+
            "    \"COL_PK_1\"    VARCHAR(32) NOT NULL,\n"+
            "    \"COL_PK_2\"    INTEGER,\n"+
            "    \"COL_INDEX_1\" BINARY(100) NOT NULL,\n"+
            "    \"COL_INDEX_2\" DOUBLE NOT NULL,\n"+
            "    \"COL_INDEX_3\" CHAR(4),\n"+
            "    PRIMARY KEY (\"COL_PK_1\", \"COL_PK_2\")\n"+
            ");\n"+
            "CREATE TABLE \"table2\"\n"+
            "(\n"+
            "    \"COL_PK\"   INTEGER,\n"+
            "    \"COL_FK_1\" INTEGER,\n"+
            "    \"COL_FK_2\" VARCHAR(32) NOT NULL,\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n"+
            "CREATE TABLE \"table3\"\n"+
            "(\n"+
            "    \"COL_PK\" VARCHAR(16),\n"+
            "    \"COL_FK\" INTEGER NOT NULL,\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n"+
            "ALTER TABLE \"table2\" ADD CONSTRAINT \"table2_FK_COL_FK_1_COL_FK_2_table1\" FOREIGN KEY (\"COL_FK_1\", \"COL_FK_2\") REFERENCES \"table1\" (\"COL_PK_2\", \"COL_PK_1\");\n"+
            "ALTER TABLE \"table3\" ADD CONSTRAINT \"testfk\" FOREIGN KEY (\"COL_FK\") REFERENCES \"table2\" (\"COL_PK\");\n",
            createTestDatabase(TABLE_CONSTRAINT_TEST_SCHEMA));
    }

    /**
     * Tests the proper escaping of character sequences where McKoi requires it.
     */
    public void testCharacterEscaping() throws Exception
    {
        assertEqualsIgnoringWhitespaces(
            "DROP TABLE IF EXISTS \"escapedcharacters\";\n"+
            "CREATE TABLE \"escapedcharacters\"\n"+
            "(\n"+
            "    \"COL_PK\"   INTEGER,\n"+
            "    \"COL_TEXT\" VARCHAR(128) DEFAULT '\\\' \\\\',\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n",
            createTestDatabase(COLUMN_CHAR_SEQUENCES_TO_ESCAPE));
    }
}
