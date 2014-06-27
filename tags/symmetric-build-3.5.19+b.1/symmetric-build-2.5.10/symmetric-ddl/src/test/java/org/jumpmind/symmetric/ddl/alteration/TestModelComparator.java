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

import java.sql.Types;
import java.util.List;

import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.TestBase;
import org.jumpmind.symmetric.ddl.model.Database;

/*
 * Tests the model comparison.
 * 
 * @version $Revision: $
 */
public class TestModelComparator extends TestBase
{
    /*
     * Creates a new model comparator.
     *
     * @param caseSensitive Whether the comparison is case sensitive 
     * @return The model comparator
     */
    protected ModelComparator createModelComparator(boolean caseSensitive)
    {
        PlatformInfo platformInfo = new PlatformInfo();

        platformInfo.setHasSize(Types.DECIMAL, true);
        platformInfo.setHasSize(Types.NUMERIC, true);
        platformInfo.setHasSize(Types.CHAR, true);
        platformInfo.setHasSize(Types.VARCHAR, true);
        return new ModelComparator(platformInfo, caseSensitive);
    }

    /*
     * Tests the addition of a table.
     */
    public void testAddTable()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        AddTableChange change = (AddTableChange)changes.get(0);

        assertEquals("TABLEB",
                     change.getNewTable().getName());
    }

    /*
     * Tests the removal of a table.
     */
    public void testRemoveTable()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        RemoveTableChange change = (RemoveTableChange)changes.get(0);

        assertEquals("TableA",
                     change.getChangedTable().getName());
    }

    /*
     * Tests the addition and removal of a table.
     */
    public void testAddAndRemoveTable()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        AddTableChange    change1 = (AddTableChange)changes.get(0);
        RemoveTableChange change2 = (RemoveTableChange)changes.get(1);

        assertEquals("TABLEA",
                     change1.getNewTable().getName());
        assertEquals("TableA",
                     change2.getChangedTable().getName());
    }

    /*
     * Tests the addition of a foreign key.
     */
    public void testAddForeignKey()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='INTEGER'/>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TABLEB'>\n" +
            "      <reference local='COLFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        AddForeignKeyChange change = (AddForeignKeyChange)changes.get(0);

        assertEquals("TESTFK",
                     change.getNewForeignKey().getName());
    }

    /*
     * Tests the addition of two tables with foreign keys to each other .
     */
    public void testAddTablesWithForeignKeys()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFKB' foreignTable='TABLEB'>\n" +
            "      <reference local='COLFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFKA' foreignTable='TABLEA'>\n" +
            "      <reference local='COLFK' foreign='COLPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(4,
                     changes.size());

        AddTableChange      tableChange1 = (AddTableChange)changes.get(0);
        AddForeignKeyChange fkChange1    = (AddForeignKeyChange)changes.get(1);
        AddTableChange      tableChange2 = (AddTableChange)changes.get(2);
        AddForeignKeyChange fkChange2    = (AddForeignKeyChange)changes.get(3);

        assertEquals("TABLEA",
                     tableChange1.getNewTable().getName());
        assertEquals("TABLEB",
                     tableChange2.getNewTable().getName());
        assertEquals("TESTFKB",
                     fkChange1.getNewForeignKey().getName());
        assertEquals("TABLEA",
                     fkChange1.getChangedTable().getName());
        assertEquals("TESTFKA",
                     fkChange2.getNewForeignKey().getName());
        assertEquals("TABLEB",
                     fkChange2.getChangedTable().getName());
    }

    /*
     * Tests the removal of a foreign key.
     */
    public void testRemoveForeignKey()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableA'>\n" +
            "      <reference local='ColFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TABLEB'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COLFK' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        RemoveForeignKeyChange change = (RemoveForeignKeyChange)changes.get(0);

        assertEquals("TestFK",
                     change.getForeignKey().getName());
    }

    /*
     * Tests the addition and removal of a foreign key.
     */
    public void testAddAndRemoveForeignKey1()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableA'>\n" +
            "      <reference local='ColFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK' type='INTEGER'/>\n" +
            "    <foreign-key name='TESTFK' foreignTable='TableA'>\n" +
            "      <reference local='ColFK' foreign='ColPK'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveForeignKeyChange change1 = (RemoveForeignKeyChange)changes.get(0);
        AddForeignKeyChange    change2 = (AddForeignKeyChange)changes.get(1);

        assertEquals("TestFK",
                     change1.getForeignKey().getName());
        assertEquals("TESTFK",
                     change2.getNewForeignKey().getName());
    }

    /*
     * Tests the addition and removal of a foreign key because of a change of the references.
     */
    public void testAddAndRemoveForeignKey2()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK1' type='INTEGER'/>\n" +
            "    <column name='ColFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableB'>\n" +
            "      <reference local='ColFK1' foreign='ColPK1'/>\n" +
            "      <reference local='ColFK2' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK1' type='INTEGER'/>\n" +
            "    <column name='ColFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableB'>\n" +
            "      <reference local='ColFK1' foreign='ColPK2'/>\n" +
            "      <reference local='ColFK2' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveForeignKeyChange change1 = (RemoveForeignKeyChange)changes.get(0);
        AddForeignKeyChange    change2 = (AddForeignKeyChange)changes.get(1);

        assertEquals("TestFK",
                     change1.getForeignKey().getName());
        assertEquals("TestFK",
                     change2.getNewForeignKey().getName());
    }


    /*
     * Tests that the order of the references in a foreign key is not important.
     */
    public void testForeignKeyReferenceOrder()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK1' type='INTEGER'/>\n" +
            "    <column name='ColFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableB'>\n" +
            "      <reference local='ColFK1' foreign='ColPK1'/>\n" +
            "      <reference local='ColFK2' foreign='ColPK2'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColFK1' type='INTEGER'/>\n" +
            "    <column name='ColFK2' type='INTEGER'/>\n" +
            "    <foreign-key name='TestFK' foreignTable='TableB'>\n" +
            "      <reference local='ColFK2' foreign='ColPK2'/>\n" +
            "      <reference local='ColFK1' foreign='ColPK1'/>\n" +
            "    </foreign-key>\n" +
            "  </table>\n" +
            "  <table name='TableB'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertTrue(changes.isEmpty());
    }

    /*
     * Tests the addition of an index.
     */
    public void testAddIndex()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='INTEGER'/>\n" +
            "    <index name='TESTINDEX'>\n" +
            "      <index-column name='COL'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        AddIndexChange change = (AddIndexChange)changes.get(0);

        assertEquals("TESTINDEX",
                     change.getNewIndex().getName());
    }

    /*
     * Tests the removal of an index.
     */
    public void testRemoveIndex()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "    <unique name='TestIndex'>\n" +
            "      <unique-column name='Col'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        RemoveIndexChange change = (RemoveIndexChange)changes.get(0);

        assertEquals("TestIndex",
                     change.getIndex().getName());
    }

    /*
     * Tests the addition and removal of an index because of the change of type of the index.
     */
    public void testAddAndRemoveIndex()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "    <unique name='TestIndex'>\n" +
            "      <unique-column name='Col'/>\n" +
            "    </unique>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveIndexChange change1 = (RemoveIndexChange)changes.get(0);
        AddIndexChange    change2 = (AddIndexChange)changes.get(1);

        assertEquals("TestIndex",
                     change1.getIndex().getName());
        assertEquals("TestIndex",
                     change2.getNewIndex().getName());
    }

    /*
     * Tests the addition and removal of an index because of the change of column order.
     */
    public void testChangeIndexColumnOrder()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col2'/>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveIndexChange change1 = (RemoveIndexChange)changes.get(0);
        AddIndexChange    change2 = (AddIndexChange)changes.get(1);

        assertEquals("TestIndex",
                     change1.getIndex().getName());
        assertEquals("TestIndex",
                     change2.getNewIndex().getName());
    }

    /*
     * Tests the addition and removal of an index because of the addition of an index column.
     */
    public void testAddIndexColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveIndexChange change1 = (RemoveIndexChange)changes.get(0);
        AddIndexChange    change2 = (AddIndexChange)changes.get(1);

        assertEquals("TestIndex",
                     change1.getIndex().getName());
        assertEquals("TestIndex",
                     change2.getNewIndex().getName());
    }

    /*
     * Tests the addition and removal of an index because of the removal of an index column.
     */
    public void testRemoveIndexColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col1'/>\n" +
            "      <index-column name='Col2'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='INTEGER'/>\n" +
            "    <column name='Col2' type='DOUBLE'/>\n" +
            "    <index name='TestIndex'>\n" +
            "      <index-column name='Col1'/>\n" +
            "    </index>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(2,
                     changes.size());

        RemoveIndexChange change1 = (RemoveIndexChange)changes.get(0);
        AddIndexChange    change2 = (AddIndexChange)changes.get(1);

        assertEquals("TestIndex",
                     change1.getIndex().getName());
        assertEquals("TestIndex",
                     change2.getNewIndex().getName());
    }

    /*
     * Tests the addition of a primary key.
     */
    public void testAddPrimaryKey()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        AddPrimaryKeyChange change = (AddPrimaryKeyChange)changes.get(0);

        assertEquals(1,
                     change.getPrimaryKeyColumns().length);
        assertEquals("ColPK",
                     change.getPrimaryKeyColumns()[0].getName());
    }

    /*
     * Tests the removal of a primary key.
     */
    public void testRemovePrimaryKey()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        RemovePrimaryKeyChange change = (RemovePrimaryKeyChange)changes.get(0);

        assertEquals(1,
                     change.getPrimaryKeyColumns().length);
        assertEquals("ColPK",
                     change.getPrimaryKeyColumns()[0].getName());
    }

    /*
     * Tests the addition of a column to the primary key.
     */
    public void testAddPrimaryKeyColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        PrimaryKeyChange change = (PrimaryKeyChange)changes.get(0);

        assertEquals(1,
                     change.getOldPrimaryKeyColumns().length);
        assertEquals(2,
                     change.getNewPrimaryKeyColumns().length);
        assertEquals("ColPK1",
                     change.getOldPrimaryKeyColumns()[0].getName());
        assertEquals("ColPK1",
                     change.getNewPrimaryKeyColumns()[0].getName());
        assertEquals("ColPK2",
                     change.getNewPrimaryKeyColumns()[1].getName());
    }

    /*
     * Tests the removal of a column from the primary key.
     */
    public void testRemovePrimaryKeyColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK1' type='INTEGER' required='true'/>\n" +
            "    <column name='ColPK2' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        PrimaryKeyChange change = (PrimaryKeyChange)changes.get(0);

        assertEquals(2,
                     change.getOldPrimaryKeyColumns().length);
        assertEquals(1,
                     change.getNewPrimaryKeyColumns().length);
        assertEquals("ColPK1",
                     change.getOldPrimaryKeyColumns()[0].getName());
        assertEquals("ColPK2",
                     change.getOldPrimaryKeyColumns()[1].getName());
        assertEquals("ColPK2",
                     change.getNewPrimaryKeyColumns()[0].getName());
    }

    /*
     * Tests the addition a column.
     */
    public void testAddColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        AddColumnChange change = (AddColumnChange)changes.get(0);

        assertEquals("Col1",
                     change.getNewColumn().getName());
    }

    /*
     * Tests the removal of a column.
     */
    public void testRemoveColumn()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col1' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        RemoveColumnChange change = (RemoveColumnChange)changes.get(0);

        assertEquals("Col1",
                     change.getColumn().getName());
    }

    /*
     * Tests changing the data type of a column.
     */
    public void testChangeColumnDataType()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnDataTypeChange change = (ColumnDataTypeChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
        assertEquals(Types.INTEGER,
                     change.getNewTypeCode());
    }

    /*
     * Tests changing the size of a column.
     */
    public void testChangeColumnSize()
    {
        // note that we also have a size for the INTEGER column, but we don't
        // expect a change for it because the size is not relevant for this type
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' size='8' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='VARCHAR' size='16'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='VARCHAR' size='32'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnSizeChange change = (ColumnSizeChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
        assertEquals(32,
                     change.getNewSize());
        assertEquals(0,
                     change.getNewScale());
    }

    /*
     * Tests changing the scale of a column.
     */
    public void testChangeColumnScale()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='NUMERIC' size='32,0'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='NUMERIC' size='32,5'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnSizeChange change = (ColumnSizeChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
        assertEquals(32,
                     change.getNewSize());
        assertEquals(5,
                     change.getNewScale());
    }

    /*
     * Tests removing the size of a column. This test shows how the comparator
     * reacts in the common case of comparing a model read from a live database
     * (which usually returns sizes for every column) and a model from XML.
     * The model comparator will filter out these changes depending on the
     * platform info with which the comparator was created. 
     */
    public void testRemoveColumnSize()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' size='8'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertTrue(changes.isEmpty());
    }

    /*
     * Tests changing the default value of a column.
     */
    public void testChangeDefaultValue()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' default='1'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' default='2'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnDefaultValueChange change = (ColumnDefaultValueChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
        assertEquals("2",
                     change.getNewDefaultValue());
    }

    /*
     * Tests that shows that the same default value expressed differently does not
     * result in a change.
     */
    public void testSameDefaultValueExpressedDifferently()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='DOUBLE' default='10'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='DOUBLE' default='1e+1'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertTrue(changes.isEmpty());
    }

    /*
     * Tests adding a default value to a column.
     */
    public void testAddDefaultValue()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' default='0'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(true).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnDefaultValueChange change = (ColumnDefaultValueChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
        assertEquals("0",
                     change.getNewDefaultValue());
    }

    /*
     * Tests chainging the required-constraint of a column.
     */
    public void testChangeColumnRequired()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' required='false'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='INTEGER' required='true'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnRequiredChange change = (ColumnRequiredChange)changes.get(0);

        assertEquals("Col",
                     change.getChangedColumn().getName());
    }

    /*
     * Tests chainging the auto-increment-constraint of a column.
     */
    public void testChangeColumnAutoIncrement()
    {
        final String MODEL1 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TableA'>\n" +
            "    <column name='ColPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='Col' type='INTEGER' autoIncrement='true'/>\n" +
            "  </table>\n" +
            "</database>";
        final String MODEL2 = 
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
            "<database name='test'>\n" +
            "  <table name='TABLEA'>\n" +
            "    <column name='COLPK' type='INTEGER' primaryKey='true' required='true'/>\n" +
            "    <column name='COL' type='INTEGER' autoIncrement='false'/>\n" +
            "  </table>\n" +
            "</database>";

        Database model1  = parseDatabaseFromString(MODEL1);
        Database model2  = parseDatabaseFromString(MODEL2);
        List     changes = createModelComparator(false).compare(model1, model2);

        assertEquals(1,
                     changes.size());

        ColumnAutoIncrementChange change = (ColumnAutoIncrementChange)changes.get(0);

        assertEquals("Col",
                     change.getColumn().getName());
    }
}
