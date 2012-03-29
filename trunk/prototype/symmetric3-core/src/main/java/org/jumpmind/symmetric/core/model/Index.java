package org.jumpmind.symmetric.core.model;

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
public abstract class Index {
    /** The name of the index. */
    protected String name;

    /** The columns making up the index. */
    protected ArrayList<IndexColumn> columns = new ArrayList<IndexColumn>();

    abstract public String toVerboseString();

    abstract public boolean isUnique();

    /**
     * {@inheritDoc}
     */
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn getColumn(int idx) {
        return (IndexColumn) columns.get(idx);
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn[] getColumns() {
        return (IndexColumn[]) columns.toArray(new IndexColumn[columns.size()]);
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasColumn(Column column) {
        for (int idx = 0; idx < columns.size(); idx++) {
            IndexColumn curColumn = getColumn(idx);

            if (column.equals(curColumn.getColumn())) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
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

    public boolean hasAllPrimaryKeys() {
        if (columns != null) {
            boolean allPks = true;
            for (IndexColumn column : columns) {
                allPks &= column.getColumn().isPrimaryKey();
            }
            return allPks;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(IndexColumn column) {
        columns.remove(column);
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(int idx) {
        columns.remove(idx);
    }

    /**
     * {@inheritDoc}
     */
    public abstract Object clone() throws CloneNotSupportedException;

    /**
     * Compares this index to the given one while ignoring the case of
     * identifiers.
     * 
     * @param otherIndex
     *            The other index
     * @return <code>true</code> if this index is equal (ignoring case) to the
     *         given one
     */
    public abstract boolean equalsIgnoreCase(Index otherIndex);
}
