/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.mock;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IDataService;

public class MockDataService implements IDataService {

    public void addReloadListener(IReloadListener listener) {

    }

    public Data createData(String tableName) {
        return null;
    }

    public Data createData(String tableName, String whereClause) {
        return null;
    }

    public Map<String, String> getRowDataAsMap(Data data) {
        return null;
    }

    public void insertCreateEvent(Node targetNode, Trigger trigger, String xml) {

    }

    public long insertData(Data data) {
        return 0;
    }

    public void insertDataEvent(DataEvent dataEvent) {

    }

    public void insertDataEvent(Data data, String channelId, List<Node> nodes) {

    }

    public void insertDataEvent(Data data, String channelId, String transactionId, List<Node> nodes) {

    }

    public void insertDataEvent(Data data, String channelId, String nodeId) {

    }

    public void insertHeartbeatEvent(Node node) {

    }

    public void insertPurgeEvent(Node targetNode, Trigger trigger) {

    }

    public void insertReloadEvent(Node targetNode) {

    }

    public void insertReloadEvent(Node targetNode, Trigger trigger) {

    }

    public void insertSqlEvent(Node targetNode, Trigger trigger, String sql) {

    }

    public String reloadNode(String nodeId) {

        return null;
    }

    public String reloadTable(String nodeId, String tableName) {

        return null;
    }

    public void removeReloadListener(IReloadListener listener) {

    }

    public void setReloadListeners(List<IReloadListener> listeners) {

    }

    public void setRowDataFromMap(Data data, Map<String, String> map) {

    }

    public String[] tokenizeCsvData(String csvData) {

        return null;
    }

    public String reloadTable(String nodeId, String tableName, String overrideInitialLoadSelect) {
        return null;
    }

    public String sendSQL(String nodeId, String tableName, String sql) {
        return null;
    }

}
