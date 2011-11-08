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

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Types;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.IndexColumn;
import org.jumpmind.symmetric.ddl.model.ModelException;
import org.jumpmind.symmetric.ddl.model.Reference;
import org.jumpmind.symmetric.ddl.model.Table;

/*
 * Tests the database reading/writing via the {@link org.jumpmind.symmetric.ddl.io.DatabaseIO} class.
 * 
 * @version $Revision: 289996 $
 */
public class TestDatabaseIO extends TestCase
{
    /* The log for the tests. */
    private final Log _log = LogFactory.getLog(TestDatabaseIO.class);

    /*
     * Reads the database model from the given string.
     * 
     * @param modelAsXml The database model XML
     * @return The database model
     */
    private Database readModel(String modelAsXml)
    {
        DatabaseIO dbIO = new DatabaseIO();

        dbIO.setUseInternalDtd(true);
        dbIO.setValidateXml(false);
        return dbIO.read(new StringReader(modelAsXml));
    }

    /*
     * Writes the given database model to a string.
     * 
     * @param model The database model
     * @return The database model XML
     */
    private String writeModel(Database model)
    {
        StringWriter writer = new StringWriter();

        new DatabaseIO().write(model, writer);
        return StringUtils.replace(writer.toString(), "\r\n", "\n");
    }

    /*
     * Tests a simple database model.
     */
    public void testSimple() throws Exception
    {
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='SomeTable'\n" +
            "         description='Some table'>\n" +
            "    <column name='ID'\n" +
            "            type='INTEGER'\n" +
            "            primaryKey='true'\n" +
            "            required='true'\n" +
            "            description='The primary key'\n" +
            "            javaName='javaId'/>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(1,
                     model.getTableCount());
        
        Table table = model.getTable(0);

        assertEquals("SomeTable",
                     table.getName());
        assertEquals("Some table",
                     table.getDescription());
        assertEquals(0, table.getAutoIncrementColumns().length);
        assertEquals(1,
                     table.getColumnCount());
        assertEquals(0,
                     table.getForeignKeyCount());
        assertEquals(0,
                     table.getIndexCount());

        Column column = table.getColumn(0);

        assertEquals("ID",
                     column.getName());
        assertEquals("INTEGER",
                     column.getType());
        assertEquals(Types.INTEGER,
                     column.getTypeCode());
        assertTrue(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The primary key",
                     column.getDescription());
        assertEquals("javaId", column.getJavaName());
        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"SomeTable\" description=\"Some table\">\n" +
            "      <column name=\"ID\" primaryKey=\"true\" required=\"true\" type=\"INTEGER\" autoIncrement=\"false\" description=\"The primary key\" javaName=\"javaId\"/>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    /*
     * Tests a database model containing a foreignkey.
     */
    public void testForeignkey() throws Exception
    {
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='SomeTable'\n" +
            "         description='Some table'>\n" +
            "    <column name='ID'\n" +
            "            type='VARCHAR'\n" +
            "            size='16'\n" +
            "            primaryKey='true'\n" +
            "            required='true'\n" +
            "            description='The primary key'/>\n" +
            "  </table>\n" +
            "  <table name='AnotherTable'\n" +
            "         description='And another table'>\n" +
            "    <column name='Some_ID'\n" +
            "            type='VARCHAR'\n" +
            "            size='16'\n" +
            "            description='The foreign key'/>\n" +
            "    <foreign-key foreignTable='SomeTable'>\n" +
            "       <reference local='Some_ID' foreign='ID'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(2,
                     model.getTableCount());

        Table someTable = model.getTable(0);

        assertEquals("SomeTable",
                     someTable.getName());
        assertEquals("Some table",
                     someTable.getDescription());
        assertEquals(0, someTable.getAutoIncrementColumns().length);
        assertEquals(1,
                     someTable.getColumnCount());
        assertEquals(0,
                     someTable.getForeignKeyCount());
        assertEquals(0,
                     someTable.getIndexCount());

        Column pkColumn = someTable.getColumn(0);

        assertEquals("ID",
                     pkColumn.getName());
        assertEquals("VARCHAR",
                     pkColumn.getType());
        assertEquals(Types.VARCHAR,
                     pkColumn.getTypeCode());
        assertEquals(16,
                     pkColumn.getSizeAsInt());
        assertTrue(pkColumn.isPrimaryKey());
        assertTrue(pkColumn.isRequired());
        assertFalse(pkColumn.isAutoIncrement());
        assertNull(pkColumn.getDefaultValue());
        assertEquals("The primary key",
                     pkColumn.getDescription());

        Table anotherTable = model.getTable(1);

        assertEquals("AnotherTable",
                     anotherTable.getName());
        assertEquals("And another table",
                     anotherTable.getDescription());
        assertEquals(0, anotherTable.getAutoIncrementColumns().length);
        assertEquals(1,
                     anotherTable.getColumnCount());
        assertEquals(1,
                     anotherTable.getForeignKeyCount());
        assertEquals(0,
                     anotherTable.getIndexCount());

        Column fkColumn = anotherTable.getColumn(0);

        assertEquals("Some_ID",
                     fkColumn.getName());
        assertEquals("VARCHAR",
                     fkColumn.getType());
        assertEquals(Types.VARCHAR,
                     fkColumn.getTypeCode());
        assertEquals(16,
                     fkColumn.getSizeAsInt());
        assertFalse(fkColumn.isPrimaryKey());
        assertFalse(fkColumn.isRequired());
        assertFalse(fkColumn.isAutoIncrement());
        assertEquals("The foreign key",
                     fkColumn.getDescription());

        ForeignKey fk = anotherTable.getForeignKey(0);

        assertNull(fk.getName());
        assertEquals(someTable,
                     fk.getForeignTable());
        assertEquals(someTable.getName(),
                     fk.getForeignTableName());
        assertEquals(1,
                     fk.getReferenceCount());

        Reference ref = fk.getFirstReference();

        assertEquals(fkColumn,
                     ref.getLocalColumn());
        assertEquals("Some_ID",
                     ref.getLocalColumnName());
        assertEquals(pkColumn,
                     ref.getForeignColumn());
        assertEquals("ID",
                     ref.getForeignColumnName());

        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"SomeTable\" description=\"Some table\">\n" +
            "      <column name=\"ID\" primaryKey=\"true\" required=\"true\" type=\"VARCHAR\" size=\"16\" autoIncrement=\"false\" description=\"The primary key\"/>\n" +
            "    </table>\n" +
            "    <table name=\"AnotherTable\" description=\"And another table\">\n" +
            "      <column name=\"Some_ID\" primaryKey=\"false\" required=\"false\" type=\"VARCHAR\" size=\"16\" autoIncrement=\"false\" description=\"The foreign key\"/>\n" +
            "      <foreign-key foreignTable=\"SomeTable\">\n" +
            "        <reference local=\"Some_ID\" foreign=\"ID\"/>\n" +
            "      </foreign-key>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    /*
     * Tests a database model with indices.
     */
    public void testIndices1() throws Exception
    {
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='TableWidthIndex'>\n" +
            "    <column name='id'\n" +
            "            type='DOUBLE'\n" +
            "            primaryKey='true'\n" +
            "            required='true'/>\n" +
            "    <column name='when'\n" +
            "            type='TIMESTAMP'\n" +
            "            required='true'/>\n" +
            "    <column name='value'\n" +
            "            type='SMALLINT'\n" +
            "            default='1'/>\n" +
            "    <index name='test index'>\n" +
            "      <index-column name='value'/>\n" +
            "    </index>\n" +
            "    <index>\n" +
            "      <index-column name='when'/>\n" +
            "      <index-column name='id'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(1,
                     model.getTableCount());
        
        Table table = model.getTable(0);

        assertEquals("TableWidthIndex",
                     table.getName());
        assertNull(table.getDescription());
        assertEquals(0, table.getAutoIncrementColumns().length);
        assertEquals(3,
                     table.getColumnCount());
        assertEquals(0,
                     table.getForeignKeyCount());
        assertEquals(2,
                     table.getIndexCount());

        Column column = table.getColumn(0);

        assertEquals("id",
                     column.getName());
        assertEquals("DOUBLE",
                     column.getType());
        assertEquals(Types.DOUBLE,
                     column.getTypeCode());
        assertTrue(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());

        column = table.getColumn(1);

        assertEquals("when",
                     column.getName());
        assertEquals("TIMESTAMP",
                     column.getType());
        assertEquals(Types.TIMESTAMP,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());

        column = table.getColumn(2);

        assertEquals("value",
                     column.getName());
        assertEquals("SMALLINT",
                     column.getType());
        assertEquals(Types.SMALLINT,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertEquals("1",
                     column.getDefaultValue());
        assertNull(column.getDescription());

        Index index = table.getIndex(0);

        assertEquals("test index",
                     index.getName());
        assertFalse(index.isUnique());
        assertEquals(1,
                     index.getColumnCount());

        IndexColumn indexColumn = index.getColumn(0);

        assertEquals("value",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        index = table.getIndex(1);

        assertNull(index.getName());
        assertFalse(index.isUnique());
        assertEquals(2,
                     index.getColumnCount());

        indexColumn = index.getColumn(0);

        assertEquals("when",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        indexColumn = index.getColumn(1);

        assertEquals("id",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"TableWidthIndex\">\n" +
            "      <column name=\"id\" primaryKey=\"true\" required=\"true\" type=\"DOUBLE\" autoIncrement=\"false\"/>\n" +
            "      <column name=\"when\" primaryKey=\"false\" required=\"true\" type=\"TIMESTAMP\" autoIncrement=\"false\"/>\n" +
            "      <column name=\"value\" primaryKey=\"false\" required=\"false\" type=\"SMALLINT\" default=\"1\" autoIncrement=\"false\"/>\n" +
            "      <index name=\"test index\">\n" +
            "        <index-column name=\"value\"/>\n" +
            "      </index>\n" +
            "      <index>\n" +
            "        <index-column name=\"when\"/>\n" +
            "        <index-column name=\"id\"/>\n" +
            "      </index>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    /*
     * Tests a database model with indices, both uniques and non-uniques.
     */
    public void testIndices2() throws Exception
    {
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='TableWidthIndices'>\n" +
            "    <column name='id'\n" +
            "            type='SMALLINT'\n" +
            "            primaryKey='false'\n" +
            "            required='true'\n" +
            "            autoIncrement='true'/>\n" +
            "    <column name='when'\n" +
            "            type='DATE'/>\n" +
            "    <unique name='important column'>\n" +
            "      <unique-column name='id'/>\n" +
            "    </unique>\n" +
            "    <index>\n" +
            "      <index-column name='when'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(1,
                     model.getTableCount());
        
        Table table = model.getTable(0);

        assertEquals("TableWidthIndices",
                     table.getName());
        assertNull(table.getDescription());
        assertEquals(2,
                     table.getColumnCount());
        assertEquals(0,
                     table.getForeignKeyCount());
        assertEquals(2,
                     table.getIndexCount());

        Column column = table.getColumn(0);

        assertEquals("id",
                     column.getName());
        assertEquals("SMALLINT",
                     column.getType());
        assertEquals(Types.SMALLINT,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertTrue(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());
        
        assertEquals(1, table.getAutoIncrementColumns().length);
        assertEquals(column, table.getAutoIncrementColumns()[0]);

        column = table.getColumn(1);

        assertEquals("when",
                     column.getName());
        assertEquals("DATE",
                     column.getType());
        assertEquals(Types.DATE,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());

        Index index = table.getIndex(0);

        assertEquals("important column",
                     index.getName());
        assertTrue(index.isUnique());
        assertEquals(1,
                     index.getColumnCount());

        IndexColumn indexColumn = index.getColumn(0);

        assertEquals("id",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        index = table.getIndex(1);

        assertNull(index.getName());
        assertFalse(index.isUnique());
        assertEquals(1,
                     index.getColumnCount());

        indexColumn = index.getColumn(0);

        assertEquals("when",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"TableWidthIndices\">\n" +
            "      <column name=\"id\" primaryKey=\"false\" required=\"true\" type=\"SMALLINT\" autoIncrement=\"true\"/>\n" +
            "      <column name=\"when\" primaryKey=\"false\" required=\"false\" type=\"DATE\" autoIncrement=\"false\"/>\n" +
            "      <unique name=\"important column\">\n" +
            "        <unique-column name=\"id\"/>\n" +
            "      </unique>\n" +
            "      <index>\n" +
            "        <index-column name=\"when\"/>\n" +
            "      </index>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    /*
     * Tests a complex database model with multiple tables, foreign keys, indices and uniques.
     */
    public void testComplex() throws Exception
    {
        // A = id:INTEGER, parentId:INTEGER, name:VARCHAR(32); fk 'parent' -> A (parentId -> id), unique(name)
        // B = id:TIMESTAMP, aid:INTEGER, cid:CHAR(32) fk -> A (aid -> id), fk -> C (cid -> id), index(aid,cid)
        // C = id:CHAR(32), text:LONGVARCHAR; index 'byText' (text)
        
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='A'\n" +
            "         description='Table A'>\n" +
            "    <column name='id'\n" +
            "            type='INTEGER'\n" +
            "            autoIncrement='true'\n" +
            "            primaryKey='true'\n" +
            "            required='true'\n" +
            "            description='The primary key of table A'/>\n" +
            "    <column name='parentId'\n" +
            "            type='INTEGER'\n" +
            "            description='The field for the foreign key parent'/>\n" +
            "    <column name='name'\n" +
            "            type='VARCHAR'\n" +
            "            size='32'\n" +
            "            required='true'\n" +
            "            description='The name'/>\n" +
            "    <foreign-key name='parent' foreignTable='A'>\n" +
            "       <reference local='parentId' foreign='id'/>\n" +
            "    </foreign-key>\n" +
            "    <unique>\n" +
            "      <unique-column name='name'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "  <table name='B'\n" +
            "         description='Table B'>\n" +
            "    <column name='id'\n" +
            "            type='TIMESTAMP'\n" +
            "            primaryKey='true'\n" +
            "            required='true'\n" +
            "            description='The primary key of table B'/>\n" +
            "    <column name='aid'\n" +
            "            type='INTEGER'\n" +
            "            description='The field for the foreign key towards A'/>\n" +
            "    <column name='cid'\n" +
            "            type='CHAR'\n" +
            "            size='32'\n" +
            "            description='The field for the foreign key towards C'/>\n" +
            "    <foreign-key foreignTable='A'>\n" +
            "       <reference local='aid' foreign='id'/>\n" +
            "    </foreign-key>\n" +
            "    <foreign-key foreignTable='C'>\n" +
            "       <reference local='cid' foreign='id'/>\n" +
            "    </foreign-key>\n" +
            "    <index>\n" +
            "      <index-column name='aid'/>\n" +
            "      <index-column name='cid'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "  <table name='C'\n" +
            "         description='Table C'>\n" +
            "    <column name='id'\n" +
            "            type='CHAR'\n" +
            "            size='32'\n" +
            "            primaryKey='true'\n" +
            "            required='true'\n" +
            "            description='The primary key of table C'/>\n" +
            "    <column name='text'\n" +
            "            type='LONGVARCHAR'\n" +
            "            description='The text'/>\n" +
            "    <index name='byText'>\n" +
            "      <index-column name='text'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(3,
                     model.getTableCount());

        // table A

        Table table = model.getTable(0);

        assertEquals("A",
                     table.getName());
        assertEquals("Table A",
                     table.getDescription());
        assertEquals(3,
                     table.getColumnCount());
        assertEquals(1,
                     table.getForeignKeyCount());
        assertEquals(1,
                     table.getIndexCount());

        Column column = table.getColumn(0);

        assertEquals("id",
                     column.getName());
        assertEquals("INTEGER",
                     column.getType());
        assertEquals(Types.INTEGER,
                     column.getTypeCode());
        assertNull(column.getSize());
        assertEquals(0,
                     column.getSizeAsInt());
        assertTrue(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertTrue(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The primary key of table A",
                     column.getDescription());
        assertEquals(1, table.getAutoIncrementColumns().length);
        assertEquals(column,
                     table.getAutoIncrementColumns()[0]);

        column = table.getColumn(1);

        assertEquals("parentId",
                     column.getName());
        assertEquals("INTEGER",
                     column.getType());
        assertEquals(Types.INTEGER,
                     column.getTypeCode());
        assertNull(column.getSize());
        assertEquals(0,
                     column.getSizeAsInt());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The field for the foreign key parent",
                     column.getDescription());

        column = table.getColumn(2);

        assertEquals("name",
                     column.getName());
        assertEquals("VARCHAR",
                     column.getType());
        assertEquals(Types.VARCHAR,
                     column.getTypeCode());
        assertEquals("32",
                     column.getSize());
        assertEquals(32,
                     column.getSizeAsInt());
        assertFalse(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The name",
                     column.getDescription());

        ForeignKey fk = table.getForeignKey(0);

        assertEquals("parent",
                     fk.getName());
        assertEquals(table,
                     fk.getForeignTable());
        assertEquals("A",
                     fk.getForeignTableName());
        assertEquals(1,
                     fk.getReferenceCount());

        Reference ref = fk.getFirstReference();

        assertEquals(table.getColumn(1),
                     ref.getLocalColumn());
        assertEquals("parentId",
                     ref.getLocalColumnName());
        assertEquals(table.getColumn(0),
                     ref.getForeignColumn());
        assertEquals("id",
                     ref.getForeignColumnName());

        Index index = table.getIndex(0);

        assertNull(index.getName());
        assertTrue(index.isUnique());
        assertEquals(1,
                     index.getColumnCount());

        IndexColumn indexColumn = index.getColumn(0);

        assertEquals("name",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        // table B
        
        table = model.getTable(1);

        assertEquals("B",
                     table.getName());
        assertEquals("Table B",
                     table.getDescription());
        assertEquals(0, table.getAutoIncrementColumns().length);
        assertEquals(3,
                     table.getColumnCount());
        assertEquals(2,
                     table.getForeignKeyCount());
        assertEquals(1,
                     table.getIndexCount());

        column = table.getColumn(0);

        assertEquals("id",
                     column.getName());
        assertEquals("TIMESTAMP",
                     column.getType());
        assertEquals(Types.TIMESTAMP,
                     column.getTypeCode());
        assertNull(column.getSize());
        assertEquals(0,
                     column.getSizeAsInt());
        assertTrue(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The primary key of table B",
                     column.getDescription());

        column = table.getColumn(1);

        assertEquals("aid",
                     column.getName());
        assertEquals("INTEGER",
                     column.getType());
        assertEquals(Types.INTEGER,
                     column.getTypeCode());
        assertNull(column.getSize());
        assertEquals(0,
                     column.getSizeAsInt());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The field for the foreign key towards A",
                     column.getDescription());

        column = table.getColumn(2);

        assertEquals("cid",
                     column.getName());
        assertEquals("CHAR",
                     column.getType());
        assertEquals(Types.CHAR,
                     column.getTypeCode());
        assertEquals("32",
                     column.getSize());
        assertEquals(32,
                     column.getSizeAsInt());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The field for the foreign key towards C",
                     column.getDescription());

        fk = table.getForeignKey(0);

        assertNull(fk.getName());
        assertEquals(model.getTable(0),
                     fk.getForeignTable());
        assertEquals("A",
                     fk.getForeignTableName());
        assertEquals(1,
                     fk.getReferenceCount());

        ref = fk.getFirstReference();

        assertEquals(table.getColumn(1),
                     ref.getLocalColumn());
        assertEquals("aid",
                     ref.getLocalColumnName());
        assertEquals(model.getTable(0).getColumn(0),
                     ref.getForeignColumn());
        assertEquals("id",
                     ref.getForeignColumnName());

        fk = table.getForeignKey(1);

        assertNull(fk.getName());
        assertEquals(model.getTable(2),
                     fk.getForeignTable());
        assertEquals("C",
                     fk.getForeignTableName());
        assertEquals(1,
                     fk.getReferenceCount());

        ref = fk.getFirstReference();

        assertEquals(table.getColumn(2),
                     ref.getLocalColumn());
        assertEquals("cid",
                     ref.getLocalColumnName());
        assertEquals(model.getTable(2).getColumn(0),
                     ref.getForeignColumn());
        assertEquals("id",
                     ref.getForeignColumnName());

        index = table.getIndex(0);

        assertNull(index.getName());
        assertFalse(index.isUnique());
        assertEquals(2,
                     index.getColumnCount());

        indexColumn = index.getColumn(0);

        assertEquals("aid",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        indexColumn = index.getColumn(1);

        assertEquals("cid",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        // table C

        table = model.getTable(2);

        assertEquals("C",
                     table.getName());
        assertEquals("Table C",
                     table.getDescription());
        assertEquals(0, table.getAutoIncrementColumns().length);
        assertEquals(2,
                     table.getColumnCount());
        assertEquals(0,
                     table.getForeignKeyCount());
        assertEquals(1,
                     table.getIndexCount());

        column = table.getColumn(0);

        assertEquals("id",
                     column.getName());
        assertEquals("CHAR",
                     column.getType());
        assertEquals(Types.CHAR,
                     column.getTypeCode());
        assertEquals("32",
                     column.getSize());
        assertEquals(32,
                     column.getSizeAsInt());
        assertTrue(column.isPrimaryKey());
        assertTrue(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The primary key of table C",
                     column.getDescription());

        column = table.getColumn(1);

        assertEquals("text",
                     column.getName());
        assertEquals("LONGVARCHAR",
                     column.getType());
        assertEquals(Types.LONGVARCHAR,
                     column.getTypeCode());
        assertNull(column.getSize());
        assertEquals(0,
                     column.getSizeAsInt());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertEquals("The text",
                     column.getDescription());

        index = table.getIndex(0);

        assertEquals("byText",
                     index.getName());
        assertFalse(index.isUnique());
        assertEquals(1,
                     index.getColumnCount());

        indexColumn = index.getColumn(0);

        assertEquals("text",
                     indexColumn.getName());
        assertNull(indexColumn.getSize());

        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"A\" description=\"Table A\">\n" +
            "      <column name=\"id\" primaryKey=\"true\" required=\"true\" type=\"INTEGER\" autoIncrement=\"true\" description=\"The primary key of table A\"/>\n" +
            "      <column name=\"parentId\" primaryKey=\"false\" required=\"false\" type=\"INTEGER\" autoIncrement=\"false\" description=\"The field for the foreign key parent\"/>\n" +
            "      <column name=\"name\" primaryKey=\"false\" required=\"true\" type=\"VARCHAR\" size=\"32\" autoIncrement=\"false\" description=\"The name\"/>\n" +
            "      <foreign-key foreignTable=\"A\" name=\"parent\">\n" +
            "        <reference local=\"parentId\" foreign=\"id\"/>\n" +
            "      </foreign-key>\n" +
            "      <unique>\n" +
            "        <unique-column name=\"name\"/>\n" +
            "      </unique>\n" +
            "    </table>\n" +
            "    <table name=\"B\" description=\"Table B\">\n" +
            "      <column name=\"id\" primaryKey=\"true\" required=\"true\" type=\"TIMESTAMP\" autoIncrement=\"false\" description=\"The primary key of table B\"/>\n" +
            "      <column name=\"aid\" primaryKey=\"false\" required=\"false\" type=\"INTEGER\" autoIncrement=\"false\" description=\"The field for the foreign key towards A\"/>\n" +
            "      <column name=\"cid\" primaryKey=\"false\" required=\"false\" type=\"CHAR\" size=\"32\" autoIncrement=\"false\" description=\"The field for the foreign key towards C\"/>\n" +
            "      <foreign-key foreignTable=\"A\">\n" +
            "        <reference local=\"aid\" foreign=\"id\"/>\n" +
            "      </foreign-key>\n" +
            "      <foreign-key foreignTable=\"C\">\n" +
            "        <reference local=\"cid\" foreign=\"id\"/>\n" +
            "      </foreign-key>\n" +
            "      <index>\n" +
            "        <index-column name=\"aid\"/>\n" +
            "        <index-column name=\"cid\"/>\n" +
            "      </index>\n" +
            "    </table>\n" +
            "    <table name=\"C\" description=\"Table C\">\n" +
            "      <column name=\"id\" primaryKey=\"true\" required=\"true\" type=\"CHAR\" size=\"32\" autoIncrement=\"false\" description=\"The primary key of table C\"/>\n" +
            "      <column name=\"text\" primaryKey=\"false\" required=\"false\" type=\"LONGVARCHAR\" autoIncrement=\"false\" description=\"The text\"/>\n" +
            "      <index name=\"byText\">\n" +
            "        <index-column name=\"text\"/>\n" +
            "      </index>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    /*
     * Tests that an exception is generated when the database element has no name attribute.
     */
    public void testDatabaseWithoutName()
    {
        try
        {
            readModel(
                "<database>\n" +
                "  <table name='TestTable'>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when the table element has no name attribute.
     */
    public void testTableWithoutName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when two table elements have the same value in their name attributes.
     */
    public void testTwoTablesWithTheSameName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TestTable'>\n" +
                "    <column name='id1'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "  <table name='TestTable'>\n" +
                "    <column name='id2'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when the column element has no name attribute.
     */
    public void testColumnWithoutName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TestTable'>\n" +
                "    <column type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when two column elements within the same table
     * element have the same value in their name attributes.
     */
    public void testTwoColumnsWithTheSameName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TestTable'>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "    <column name='id'\n" +
                "            type='VARCHAR'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when the column element has no type attribute.
     */
    public void testColumnWithoutType()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TestTable'>\n" +
                "    <column name='id'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when two indices have the same value in their name attributes.
     */
    public void testTwoIndicesWithTheSameName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TableWidthIndex'>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "    <column name='value1'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <column name='value2'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <index name='the index'>\n" +
                "      <index-column name='value1'/>\n" +
                "    </index>\n" +
                "    <index name='the index'>\n" +
                "      <index-column name='value2'/>\n" +
                "    </index>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when two unique indices have the
     * same value in their name attributes.
     */
    public void testTwoUniqueIndicesWithTheSameName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TableWidthUnique'>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "    <column name='value1'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <column name='value2'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <unique name='the unique'>\n" +
                "      <unique-column name='value1'/>\n" +
                "    </unique>\n" +
                "    <unique name='the unique'>\n" +
                "      <unique-column name='value2'/>\n" +
                "    </unique>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Tests that an exception is generated when a unique and a normal index
     * have the same value in their name attributes.
     */
    public void testUniqueAndNormalIndexWithTheSameName()
    {
        try
        {
            readModel(
                "<database name='test'>\n" +
                "  <table name='TableWidthUnique'>\n" +
                "    <column name='id'\n" +
                "            type='INTEGER'\n" +
                "            primaryKey='true'\n" +
                "            required='true'/>\n" +
                "    <column name='value1'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <column name='value2'\n" +
                "            type='INTEGER'\n" +
                "            required='true'/>\n" +
                "    <index name='test'>\n" +
                "      <index-column name='value1'/>\n" +
                "    </index>\n" +
                "    <unique name='test'>\n" +
                "      <unique-column name='value2'/>\n" +
                "    </unique>\n" +
                "  </table>\n" +
                "</database>");

            fail();
        }
        catch (ModelException ex)
        {}
    }

    /*
     * Regression test ensuring that wrong XML is not read (regarding betwixt issue #37369).
     */
    public void testFaultReadOfTable() 
    {
        Database database = readModel(
                "<database name='db' >\n" +
                "  <index name='NotATable'/>\n" +
                "</database>");

        _log.debug("Table : " + Arrays.asList(database.getTables()));
        assertEquals(0, database.getTableCount());
    }

    /*
     * Tests the Torque/Turbine extensions BOOLEANINT & BOOLEANCHAR.
     */
    public void testTurbineExtension() throws Exception
    {
        Database model = readModel(
            "<database name='test'>\n" +
            "  <table name='SomeTable'>\n" +
            "    <column name='intField'\n" +
            "            type='BOOLEANINT'/>\n" +
            "    <column name='charField'\n" +
            "            type='BOOLEANCHAR'/>\n" +
            "  </table>\n" +
            "</database>");

        assertEquals("test",
                     model.getName());
        assertEquals(1,
                     model.getTableCount());
        
        Table table = model.getTable(0);

        assertEquals("SomeTable",
                     table.getName());
        assertNull(table.getDescription());
        assertEquals(0, table.getAutoIncrementColumns().length);
        assertEquals(2,
                     table.getColumnCount());
        assertEquals(0,
                     table.getForeignKeyCount());
        assertEquals(0,
                     table.getIndexCount());

        Column column = table.getColumn(0);

        assertEquals("intField",
                     column.getName());
        assertEquals("TINYINT",
                     column.getType());
        assertEquals(Types.TINYINT,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());

        column = table.getColumn(1);

        assertEquals("charField",
                     column.getName());
        assertEquals("CHAR",
                     column.getType());
        assertEquals(Types.CHAR,
                     column.getTypeCode());
        assertFalse(column.isPrimaryKey());
        assertFalse(column.isRequired());
        assertFalse(column.isAutoIncrement());
        assertNull(column.getDefaultValue());
        assertNull(column.getDescription());

        assertEquals(
            "<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n" +
            "  <database name=\"test\">\n" +
            "    <table name=\"SomeTable\">\n" +
            "      <column name=\"intField\" primaryKey=\"false\" required=\"false\" type=\"TINYINT\" autoIncrement=\"false\"/>\n" +
            "      <column name=\"charField\" primaryKey=\"false\" required=\"false\" type=\"CHAR\" autoIncrement=\"false\"/>\n" +
            "    </table>\n" +
            "  </database>\n",
            writeModel(model));
    }

    // TODO: Tests that include:
    // * foreign key references undefined table
    // * foreign key references undefined local column
    // * foreign key references undefined foreign column
    // * two foreign keys with the same name
}
