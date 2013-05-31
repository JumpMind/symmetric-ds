package org.jumpmind.symmetric.model;

import org.jumpmind.db.model.Table;

public class DataMetaData {

    private Data data;
    private Table table;
    private Router router;
    private NodeChannel nodeChannel;

    public DataMetaData(Data data, Table table, Router router, NodeChannel nodeChannel) {
        this.data = data;
        this.table = table;
        this.router = router;
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
    
    public Router getRouter() {
        return router;
    }
    
    public void setRouter(Router router) {
        this.router = router;
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