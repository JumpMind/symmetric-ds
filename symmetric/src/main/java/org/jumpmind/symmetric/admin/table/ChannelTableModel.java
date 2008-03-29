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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.NodeChannel;

public class ChannelTableModel extends ModelObjectTableModel<Channel> {

    private static final long serialVersionUID = -5154253989768004844L;

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
    void setColumnValue(int index, Channel c, Object value) {
        if (c != null) {
            this.dirtyList.add(c);

            switch (index) {
            case 0:
                if (!list.contains(c)) {
                    c.setId((String) value);
                }
                break;
            case 1:
                c.setProcessingOrder((Integer) value);
                break;
            case 2:
                c.setMaxBatchSize((Integer) value);
                break;
            case 3:
                c.setMaxBatchToSend((Integer) value);
                break;
            case 4:
                c.setEnabled((Boolean) value);
                break;
            }
        }

    }

    @Override
    int getNumberOfColumns() {
        return 5;
    }

    @Override
    List<Channel> getRows() {
        List<Channel> all = new ArrayList<Channel>();
        if (list != null) {
            all.addAll(list);
        }

        if (dirtyList != null) {
            List<Channel> newList = new ArrayList<Channel>();
            newList.addAll(dirtyList);
            if (list != null) {
                newList.removeAll(list);
            }
            all.addAll(newList);
        }
        return all;
    }

    @Override
    protected void postSetup() {
        List<NodeChannel> nc = this.database.getChannels();
        list = new ArrayList<Channel>(nc.size());
        for (NodeChannel nodeChannel : nc) {
            list.add(nodeChannel);
        }
    }

    @Override
    protected void deleteRow(Channel channel) {
        database.delete(channel);
    }

    @Override
    protected boolean isRowSaveable(Channel rowObject) {
        return !StringUtils.isBlank(rowObject.getId());
    }

    @Override
    protected void saveRow(Channel rowObject) {
        database.save(rowObject);
    }

    @Override
    boolean newRow() {
        Channel newChannel = new Channel();
        this.dirtyList.add(newChannel);
        return true;
    }

}
