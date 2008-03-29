/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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
package org.jumpmind.symmetric.admin.table;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.swing.DefaultListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellEditor;

import org.jumpmind.symmetric.admin.SymmetricDatabase;
import org.jumpmind.symmetric.model.Channel;

abstract public class ModelObjectTableModel<T> extends AbstractTableModel implements ListSelectionListener {

    protected int selectedRow = 0;

    protected transient List<T> list;

    protected transient Set<T> dirtyList = new HashSet<T>();

    protected transient SymmetricDatabase database;

    public ModelObjectTableModel() {
    }

    final public void setup(SymmetricDatabase db) {
        this.database = db;
        postSetup();
    }

    abstract protected void postSetup();

    public void valueChanged(ListSelectionEvent e) {
        if (!e.getValueIsAdjusting()) {
            DefaultListSelectionModel model = (DefaultListSelectionModel) e.getSource();
            selectedRow = model.getMinSelectionIndex();
        }
    }

    abstract public String getColumnName(int column);

    public boolean isCellEditable(int row, int column) {
        if (row == selectedRow) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    abstract public Class getColumnClass(int column);

    public void save() {
        if (dirtyList.size() > 0) {
            for (Iterator<T> i = dirtyList.iterator(); i.hasNext();) {
                T c = i.next();
                if (isRowSaveable(c)) {
                    saveRow(c);
                    if (!list.contains(c)) {
                        list.add(c);
                    }
                    i.remove();
                }
            }
        }
    }

    abstract protected boolean isRowSaveable(T rowObject);

    abstract protected void saveRow(T rowObject);

    public void delete() {
        if (selectedRow < list.size()) {
            deleteRow(list.get(selectedRow));
            list.remove(selectedRow);
        }
    }

    abstract protected void deleteRow(T rowObject);

    /**
     * This method is an optional override.  Use it to specify table cell editors
     * for the current model.
     */
    public TableCellEditor getCellEditorForColumn(int column) {
        return null;
    }

    abstract public Object getValueAt(int row, int column);

    abstract List<T> getRows();

    public void setValueAt(Object value, int row, int column) {
        List<T> rows = getRows();
        setColumnValue(column, rows.get(row), value);
        fireTableCellUpdated(row, column);
    }

    abstract void setColumnValue(int index, T object, Object value);

    abstract int getNumberOfColumns();

    abstract boolean newRow();

    public int getRowCount() {
        return getRows().size();
    }

    public int getColumnCount() {
        return getNumberOfColumns() + 1;
    }

    public boolean hasEmptyRow() {
        List<T> dataList = getRows();
        if (dataList.size() == 0)
            return false;
        Channel audioRecord = (Channel) dataList.get(dataList.size() - 1);
        if (audioRecord.getId() == null) {
            return true;
        } else
            return false;
    }

    public boolean addEmptyRow() {
        if (newRow()) {
            int index = getRowCount() - 1;
            fireTableRowsInserted(index, index);
            return true;
        } else {
            return false;
        }
    }
}
