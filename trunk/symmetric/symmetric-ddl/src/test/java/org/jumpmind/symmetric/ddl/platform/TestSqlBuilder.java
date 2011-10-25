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

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.ddl.TestBase;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;

/*
 * Test the base SqlBuilder class.
 * 
 * @version $Revision: $
 */
public class TestSqlBuilder extends TestBase
{
    /*
     * Tests the {@link SqlBuilder#getUpdateSql(Table, Map, boolean)} method.
     */
    public void testUpdateSql()
    {
        final String modelXml =
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"+
            "<database name='ddlutils'>\n"+
            "  <table name='TestTable'>\n"+
            "    <column name='id' autoIncrement='true' type='INTEGER' primaryKey='true'/>\n"+
            "    <column name='name' type='VARCHAR' size='15'/>\n"+
            "  </table>\n"+
            "</database>";

        TestPlatform platform   = new TestPlatform();
        SqlBuilder   sqlBuilder = platform.getSqlBuilder();
        Database     database   = parseDatabaseFromString(modelXml);
        Map          map        = new HashMap();

        map.put("name", "ddlutils");
        map.put("id", new Integer(0));

        platform.setDelimitedIdentifierModeOn(true);
        
        String sql = sqlBuilder.getUpdateSql(database.getTable(0), map, false);

        assertEquals("UPDATE \"TestTable\" SET \"name\" = 'ddlutils' WHERE \"id\" = '0'",
                     sql);
    }
}
