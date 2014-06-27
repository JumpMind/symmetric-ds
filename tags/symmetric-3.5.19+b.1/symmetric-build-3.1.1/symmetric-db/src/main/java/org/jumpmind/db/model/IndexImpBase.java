package org.jumpmind.db.model;

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

/**
 * Base class for indices.
 */
public abstract class IndexImpBase implements IIndex {

    private static final long serialVersionUID = 1L;

    /** The name of the index. */
    protected String name;

    /** The columns making up the index. */
    protected ArrayList<IndexColumn> columns = new ArrayList<IndexColumn>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public IndexColumn getColumn(int idx) {
        return (IndexColumn) columns.get(idx);
    }

    public IndexColumn[] getColumns() {
        return (IndexColumn[]) columns.toArray(new IndexColumn[columns.size()]);
    }

    public boolean hasColumn(Column column) {
        for (int idx = 0; idx < columns.size(); idx++) {
            IndexColumn curColumn = getColumn(idx);

            if (column.getName().equals(curColumn.getName())) {
                return true;
            }
        }
        return false;
    }

    public void addColumn(IndexColumn column) {
        if (column != null) {
            for (int idx = 0; idx < columns.size(); idx++) {
                IndexColumn curColumn = getColumn(idx);

                if (curColumn.getOrdinalPosition() > column.getOrdinalPosition()) {
                    columns.add(idx, column);
                    return;
                }
            }
            columns.add(column);
        }
    }

    public void removeColumn(IndexColumn column) {
        columns.remove(column);
    }

    public void removeColumn(int idx) {
        columns.remove(idx);
    }

    public abstract Object clone() throws CloneNotSupportedException;
    
    public boolean hasAllPrimaryKeys() {
        boolean hasAllPrimaryKeys = true;
        for (IndexColumn col : columns) {
            hasAllPrimaryKeys &= col.isPrimaryKey();
        }
        return hasAllPrimaryKeys;
    }
}
