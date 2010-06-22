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

import java.util.Map;

import org.apache.commons.beanutils.DynaBean;
import org.jumpmind.symmetric.ddl.TestPlatformBase;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaBean;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaClass;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * Tests the {@link org.jumpmind.symmetric.ddl.PlatformImplBase} (abstract) class.
 * 
 * @version $Revision: 279421 $
 */
public class TestPlatformImplBase extends TestPlatformBase 
{
    /** The tested model. */
    private static final String TESTED_MODEL =
        "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
        "<database name='ddlutils'>\n"+
        "  <table name='TestTable'>\n"+
        "    <column name='id' autoIncrement='true' type='INTEGER' primaryKey='true'/>\n"+
        "    <column name='name' type='VARCHAR' size='15'/>\n"+
        "  </table>\n"+
        "</database>";

    /**
     * {@inheritDoc}
     */
    public void setUp()
    {
    }

    /**
     * {@inheritDoc}
     */
    protected String getDatabaseName()
    {
        return null;
    }

    /**
     * Test the toColumnValues method.
     */
    public void testToColumnValues()
    {
        Database         database = parseDatabaseFromString(TESTED_MODEL);
        PlatformImplBase platform = new TestPlatform();
        Table            table    = database.getTable(0);
        SqlDynaClass     clz      = SqlDynaClass.newInstance(table);
        DynaBean         db       = new SqlDynaBean(SqlDynaClass.newInstance(table));

        db.set("name", "name");

        Map map = platform.toColumnValues(clz.getSqlDynaProperties(), db);

        assertEquals("name",
                     map.get("name"));
        assertTrue(map.containsKey("id"));
    }
}
