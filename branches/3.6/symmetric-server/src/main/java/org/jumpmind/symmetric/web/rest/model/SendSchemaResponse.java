package org.jumpmind.symmetric.web.rest.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SendSchemaResponse {
   
    protected Map<String, List<TableName>> nodeIdsSentTo = new HashMap<String, List<TableName>>();

    public Map<String, List<TableName>> getNodeIdsSentTo() {
        return nodeIdsSentTo;
    }

    public void setNodeIdsSentTo(Map<String, List<TableName>> nodeIdsSentTo) {
        this.nodeIdsSentTo = nodeIdsSentTo;
    }    

}
