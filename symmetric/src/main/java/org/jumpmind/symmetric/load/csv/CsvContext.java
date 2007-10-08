/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.load.csv;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.TableTemplate;

public class CsvContext extends DataLoaderContext {

    private transient Map<String, TableTemplate> tableTemplateMap;

    private TableTemplate tableTemplate;

    public CsvContext() {
        this.tableTemplateMap = new HashMap<String, TableTemplate>();
    }
    
    public void setTableName(String tableName) {
        super.setTableName(tableName);
        this.tableTemplate = tableTemplateMap.get(tableName);
    }

    public void setKeyNames(String[] keyNames) {
        super.setKeyNames(keyNames);
        tableTemplate.setKeyNames(keyNames);
    }

    public void setColumnNames(String[] columnNames) {
        super.setColumnNames(columnNames);
        tableTemplate.setColumnNames(columnNames);
    }

    public Collection<TableTemplate> getAllTableTemplates() {
        return tableTemplateMap.values();
    }

    public TableTemplate getTableTemplate() {
        return tableTemplate;
    }

    public void setTableTemplate(TableTemplate tableTemplate) {
        this.tableTemplate = tableTemplate;
        tableTemplateMap.put(getTableName(), tableTemplate);
    }

}
