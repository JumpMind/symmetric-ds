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

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.TableCellEditor;

import org.jumpmind.symmetric.model.Channel;

public class ChannelTableModel extends ModelObjectTableModel<Channel> {

    private static final long serialVersionUID = -5154253989768004844L;

    transient List<Channel> dirtyList = new ArrayList<Channel>();

    @Override
    public String getColumnName(int index) {
        switch (index) {
        case 0:
            return "Channel Name";
        case 1:
            return "Process Order";
        case 2:
            return "Max Events Per Batch";
        case 3:
            return "Max Batches Per Sync";
        case 4:
            return "Enabled";
        }

        return "";
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class getColumnClass(int column) {
        switch (column) {
        case 0:
            return String.class;
        case 1:
            return Integer.class;
        case 2:
            return Integer.class;
        case 3:
            return Integer.class;
        case 4:
            return Boolean.class;
        }

        return null;
    }

    @Override
    public Object getValueAt(int row, int column) {
        List<Channel> rows = getRows();
        Channel object = rows.get(row);
        if (object != null) {
            switch (column) {
            case 0:
                return object.getId();
            case 1:
                return object.getProcessingOrder();
            case 2:
                return object.getMaxBatchSize();
            case 3:
                return object.getMaxBatchToSend();
            case 4:
                return object.isEnabled();
            }
        }

        return null;
    }

    @Override
    void setColumnValue(int index, Channel object, Object value) {
        if (object != null) {

            this.dirtyList.add(object);

            switch (index) {
            case 0:
                object.setId((String) value);
                break;
            case 1:
                object.setProcessingOrder((Integer) value);
                break;
            case 2:
                object.setMaxBatchSize((Integer) value);
                break;
            case 3:
                object.setMaxBatchToSend((Integer) value);
                break;
            case 4:
                object.setEnabled((Boolean) value);
                break;
            }
        }

    }

    @Override
    public TableCellEditor getCellEditorForColumn(int column) {
        switch (column) {
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        }

        return null;
    }

    @Override
    int getNumberOfColumns() {
        return 5;
    }

    List<Channel> list = null;

    @Override
    List<Channel> getRows() {
        if (list == null) {
            list = new ArrayList<Channel>();
            list.add(new Channel("test", 10));
        }

        return list;
    }

    @Override
    public void save() throws ValidationException {
        System.out.println("saved " + dirtyList.size() + " objects ");
        if (dirtyList.size() > 0) {
            dirtyList.clear();
            throw new ValidationException();
        }
        dirtyList.clear();
    }

    @Override
    Channel newRow() {
        Channel newChannel = new Channel();
        this.list.add(newChannel);
        return newChannel;
    }
}
