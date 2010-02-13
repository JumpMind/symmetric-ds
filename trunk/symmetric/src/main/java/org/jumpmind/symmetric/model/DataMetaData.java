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
package org.jumpmind.symmetric.model;

import org.apache.ddlutils.model.Table;

public class DataMetaData {

    private Data data;
    private Table table;
    private TriggerRouter triggerRouter;
    private NodeChannel nodeChannel;

    public DataMetaData(Data data, Table table, TriggerRouter trigger, NodeChannel nodeChannel) {
        this.data = data;
        this.table = table;
        this.triggerRouter = trigger;
        this.nodeChannel = nodeChannel;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TriggerRouter getTriggerRouter() {
        return triggerRouter;
    }

    public void setTriggerRouter(TriggerRouter trigger) {
        this.triggerRouter = trigger;
    }

    public NodeChannel getNodeChannel() {
        return nodeChannel;
    }

    public void setNodeChannel(NodeChannel nodeChannel) {
        this.nodeChannel = nodeChannel;
    }

    public TriggerHistory getTriggerHistory() {
        return data != null ? data.getTriggerHistory() : null;
    }

}
