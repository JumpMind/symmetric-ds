package org.jumpmind.symmetric.ddl.task;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Specifies a parameter for the creation of the tables. These are usually platform specific.
 * Note that parameters are only applied when creating new tables, not when altering existing ones.
 * Note also that if no table name is specified, the parameter is used for all created tables.
 * 
 * @version $Revision: 231306 $
 * @ant.type name="parameter"
 */
public class TableSpecificParameter extends Parameter
{
    // TODO: Some wildcard/regular expression mechanism would be useful

    /** The tables for which this parameter is applicable. */
    private ArrayList _tables = new ArrayList();

    /**
     * Specifies the comma-separated list of table names in whose creation this parameter
     * shall be used. For every table not in this list, the parameter is ignored.
     * 
     * @param tableList The tables
     * @ant.not-required Use this or the <code>table</code> parameter. If neither is specified,
     *                   the parameter is applied in the creation of all tables.
     */
    public void setTables(String tableList)
    {
        StringTokenizer tokenizer = new StringTokenizer(tableList, ",");

        while (tokenizer.hasMoreTokens())
        {
            String tableName = tokenizer.nextToken().trim();

            // TODO: Quotation, escaped characters ?
            _tables.add(tableName);
        }
    }

    /**
     * Specifies the name of the table in whose creation this parameter shall be applied.
     * 
     * @param tableName The table
     * @ant.not-required Use this or the <code>tables</code> parameter. If neither is specified,
     *                   the parameter is applied in the creation of all tables.
     */
    public void setTable(String tableName)
    {
        _tables.add(tableName);
    }

    /**
     * Determines whether this parameter is applicable to the given table.
     * 
     * @param table         The table
     * @param caseSensitive Whether the case of the table name is relevant
     * @return <code>true</code> if this parameter is applicable to the table
     */
    public boolean isForTable(Table table, boolean caseSensitive)
    {
        if (_tables.isEmpty())
        {
            return true;
        }
        for (Iterator it = _tables.iterator(); it.hasNext();)
        {
            String tableName = (String)it.next();

            if ((caseSensitive  && tableName.equals(table.getName())) ||
                (!caseSensitive && tableName.equalsIgnoreCase(table.getName())))
            {
                return true;
            }
        }
        return false;
    }
}
