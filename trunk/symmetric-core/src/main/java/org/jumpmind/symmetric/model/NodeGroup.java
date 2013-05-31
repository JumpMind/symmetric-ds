
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class NodeGroup implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String nodeGroupId;

    private String description;
        
    private Date createTime;
    
    private Date lastUpdateTime;
    
    private String lastUpdateBy;

    public NodeGroup(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }
    
    public NodeGroup() {
    }
    
    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String name) {
        this.nodeGroupId = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
    
    public Date getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public String getLastUpdateBy() {
        return lastUpdateBy;
    }
    
    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }
    
    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    @Override
    public int hashCode() {
        if (nodeGroupId != null) {
            return nodeGroupId.hashCode();
        } else {
            return super.hashCode();
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (nodeGroupId != null) {
            if (obj instanceof NodeGroup) {
                return nodeGroupId.equals(((NodeGroup) obj).nodeGroupId);
            } else {
                return false;
            }
        } else {
            return super.equals(obj);
        }
    }

    @Override
    public String toString() {
        if (nodeGroupId != null) {
            return nodeGroupId;
        } else {
            return super.toString();
        }        
    }

}