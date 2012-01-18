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
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;

/**
 * Tests the Sybase platform.
 * 
 * @version $Revision: 231110 $
 */
public class TestSybasePlatform extends TestPlatformBase
{
    /** The database schema for testing escaping of character sequences. */
    public static final String COLUMN_CHAR_SEQUENCES_TO_ESCAPE =
        "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
        "<database name='escapetest'>\n" +
        "  <table name='escapedcharacters'>\n" +
        "    <column name='COL_PK' type='INTEGER' primaryKey='true'/>\n" +
        "    <column name='COL_TEXT' type='VARCHAR' size='128' default='&#39;'/>\n" +
        "  </table>\n" +
        "</database>";

    /**
     * {@inheritDoc}
     */
    protected String getDatabaseName()
    {
        return SybasePlatform.DATABASENAME;
    }

    /**
     * Tests the column types.
     */
    public void testColumnTypes() throws Exception
    {
        assertEqualsIgnoringWhitespaces(
            "SET quoted_identifier on;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'coltype')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"coltype\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"coltype\"\n"+
            "(\n"+
            "    \"COL_ARRAY\"           IMAGE NULL,\n"+
            "    \"COL_BIGINT\"          DECIMAL(19,0) NULL,\n"+
            "    \"COL_BINARY\"          BINARY(254) NULL,\n"+
            "    \"COL_BIT\"             SMALLINT NULL,\n"+
            "    \"COL_BLOB\"            IMAGE NULL,\n"+
            "    \"COL_BOOLEAN\"         SMALLINT NULL,\n"+
            "    \"COL_CHAR\"            CHAR(15) NULL,\n"+
            "    \"COL_CLOB\"            TEXT NULL,\n"+
            "    \"COL_DATALINK\"        IMAGE NULL,\n"+
            "    \"COL_DATE\"            DATETIME NULL,\n"+
            "    \"COL_DECIMAL\"         DECIMAL(15,3) NULL,\n"+
            "    \"COL_DECIMAL_NOSCALE\" DECIMAL(15,0) NULL,\n"+
            "    \"COL_DISTINCT\"        IMAGE NULL,\n"+
            "    \"COL_DOUBLE\"          DOUBLE PRECISION NULL,\n"+
            "    \"COL_FLOAT\"           DOUBLE PRECISION NULL,\n"+
            "    \"COL_INTEGER\"         INT NULL,\n"+
            "    \"COL_JAVA_OBJECT\"     IMAGE NULL,\n"+
            "    \"COL_LONGVARBINARY\"   IMAGE NULL,\n"+
            "    \"COL_LONGVARCHAR\"     TEXT NULL,\n"+
            "    \"COL_NULL\"            IMAGE NULL,\n"+
            "    \"COL_NUMERIC\"         NUMERIC(15,0) NULL,\n"+
            "    \"COL_OTHER\"           IMAGE NULL,\n"+
            "    \"COL_REAL\"            REAL NULL,\n"+
            "    \"COL_REF\"             IMAGE NULL,\n"+
            "    \"COL_SMALLINT\"        SMALLINT NULL,\n"+
            "    \"COL_STRUCT\"          IMAGE NULL,\n"+
            "    \"COL_TIME\"            DATETIME NULL,\n"+
            "    \"COL_TIMESTAMP\"       DATETIME NULL,\n"+
            "    \"COL_TINYINT\"         SMALLINT NULL,\n"+
            "    \"COL_VARBINARY\"       VARBINARY(15) NULL,\n"+
            "    \"COL_VARCHAR\"         VARCHAR(15) NULL\n"+
            ");\n",
            createTestDatabase(COLUMN_TEST_SCHEMA));
    }

    /**
     * Tests the column constraints.
     */
    public void testColumnConstraints() throws Exception
    {
        // this is not valid sql as a table can have only one identity column at most 
        assertEqualsIgnoringWhitespaces(
            "SET quoted_identifier on;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'constraints')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"constraints\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"constraints\"\n"+
            "(\n"+
            "    \"COL_PK\"               VARCHAR(32) NULL,\n"+
            "    \"COL_PK_AUTO_INCR\"     INT IDENTITY,\n"+
            "    \"COL_NOT_NULL\"         BINARY(100) NOT NULL,\n"+
            "    \"COL_NOT_NULL_DEFAULT\" DOUBLE PRECISION DEFAULT -2.0 NOT NULL,\n"+
            "    \"COL_DEFAULT\"          CHAR(4) DEFAULT 'test' NULL,\n"+
            "    \"COL_AUTO_INCR\"        DECIMAL(19,0) IDENTITY,\n"+
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
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'RI' AND name = 'testfk')\n"+
            "    ALTER TABLE \"table3\" DROP CONSTRAINT \"testfk\";\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'RI' AND name = 'table2_FK_COL_FK_1_COL_FK_2_table1')\n"+
            "    ALTER TABLE \"table2\" DROP CONSTRAINT \"table2_FK_COL_FK_1_COL_FK_2_table1\";\n"+
            "SET quoted_identifier on;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'table3')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"table3\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'table2')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"table2\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'table1')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"table1\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"table1\"\n"+
            "(\n"+
            "    \"COL_PK_1\"    VARCHAR(32) NOT NULL,\n"+
            "    \"COL_PK_2\"    INT NULL,\n"+
            "    \"COL_INDEX_1\" BINARY(100) NOT NULL,\n"+
            "    \"COL_INDEX_2\" DOUBLE PRECISION NOT NULL,\n"+
            "    \"COL_INDEX_3\" CHAR(4) NULL,\n"+
            "    PRIMARY KEY (\"COL_PK_1\", \"COL_PK_2\")\n"+
            ");\n"+
            "CREATE INDEX \"testindex1\" ON \"table1\" (\"COL_INDEX_2\");\n"+
            "CREATE UNIQUE INDEX \"testindex2\" ON \"table1\" (\"COL_INDEX_3\", \"COL_INDEX_1\");\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"table2\"\n"+
            "(\n"+
            "    \"COL_PK\"   INT NULL,\n"+
            "    \"COL_FK_1\" INT NULL,\n"+
            "    \"COL_FK_2\" VARCHAR(32) NOT NULL,\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"table3\"\n"+
            "(\n"+
            "    \"COL_PK\" VARCHAR(16) NULL,\n"+
            "    \"COL_FK\" INT NOT NULL,\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n"+
            "ALTER TABLE \"table2\" ADD CONSTRAINT \"table2_FK_COL_FK_1_COL_FK_2_table1\" FOREIGN KEY (\"COL_FK_1\", \"COL_FK_2\") REFERENCES \"table1\" (\"COL_PK_2\", \"COL_PK_1\");\n"+
            "ALTER TABLE \"table3\" ADD CONSTRAINT \"testfk\" FOREIGN KEY (\"COL_FK\") REFERENCES \"table2\" (\"COL_PK\");\n",
            createTestDatabase(TABLE_CONSTRAINT_TEST_SCHEMA));
    }

    /**
     * Tests the proper escaping of character sequences where Cloudscape requires it.
     */
    public void testCharacterEscaping() throws Exception
    {
        assertEqualsIgnoringWhitespaces(
            "SET quoted_identifier on;\n"+
            "SET quoted_identifier on;\n"+
            "IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = 'escapedcharacters')\n"+
            "BEGIN\n"+
            "    DROP TABLE \"escapedcharacters\"\n"+
            "END;\n"+
            "SET quoted_identifier on;\n"+
            "CREATE TABLE \"escapedcharacters\"\n"+
            "(\n"+
            "    \"COL_PK\"   INT NULL,\n"+
            "    \"COL_TEXT\" VARCHAR(128) DEFAULT '\'\'' NULL,\n"+
            "    PRIMARY KEY (\"COL_PK\")\n"+
            ");\n",
            createTestDatabase(COLUMN_CHAR_SEQUENCES_TO_ESCAPE));
    }
}
