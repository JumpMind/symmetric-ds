package org.jumpmind.symmetric.model;

import org.apache.ddlutils.model.Table;

public class DataMetaData {

    private Data data;
    private Table table;
    private Trigger trigger;
    private Channel channel;

    public DataMetaData(Data data, Table table, Trigger trigger, Channel channel) {
        this.data = data;
        this.table = table;
        this.trigger = trigger;
        this.channel = channel;
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

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public TriggerHistory getTriggerHistory() {
        return data != null ? data.getTriggerHistory() : null;
    }

}
