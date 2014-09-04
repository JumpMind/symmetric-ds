package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SendSchemaRequest {

    protected String nodeGroupIdToSendTo;
    
    protected List<String> nodeIdsToSendTo;
    
    protected List<TableName> tablesToSend;
    
    public SendSchemaRequest() {
    }
    
    public void setNodeIdsToSendTo(List<String> nodeIds) {
        this.nodeIdsToSendTo = nodeIds;
    }
    
    public List<String> getNodeIdsToSendTo() {
        return nodeIdsToSendTo;
    }
    
    public void setTablesToSend(List<TableName> tableNames) {
        this.tablesToSend = tableNames;
    }
    
    public List<TableName> getTablesToSend() {
        return tablesToSend;
    }
    
    public void setNodeGroupIdToSendTo(String nodeGroupIdToSendTo) {
        this.nodeGroupIdToSendTo = nodeGroupIdToSendTo;
    }
    
    public String getNodeGroupIdToSendTo() {
        return nodeGroupIdToSendTo;
    }
    
}
