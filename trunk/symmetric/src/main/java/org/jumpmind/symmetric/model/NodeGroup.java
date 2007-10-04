package org.jumpmind.symmetric.model;

public class NodeGroup  {

    private static final long serialVersionUID = -8244845505598568994L;

    private String groupId;

    private String description;
    
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String name) {
        this.groupId = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
