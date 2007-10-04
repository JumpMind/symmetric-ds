package org.jumpmind.symmetric.model;

public class NodeGroupLink  {

    private static final long serialVersionUID = 1L;

    private String sourceGroupId;
    
    private String targetGroupId;
    
    private DataEventAction dataEventAction = DataEventAction.WAIT_FOR_POLL;

    public DataEventAction getDataEventAction() {
        return dataEventAction;
    }

    public void setDataEventAction(DataEventAction dataEventAction) {
        this.dataEventAction = dataEventAction;
    }

    public String getSourceGroupId() {
        return sourceGroupId;
    }

    public void setSourceGroupId(String domainName) {
        this.sourceGroupId = domainName;
    }

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetDomainName) {
        this.targetGroupId = targetDomainName;
    }
}
