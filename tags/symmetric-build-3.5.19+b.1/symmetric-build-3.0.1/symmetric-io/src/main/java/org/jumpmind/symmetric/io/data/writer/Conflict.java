package org.jumpmind.symmetric.io.data.writer;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;

public class Conflict implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum DetectConflict {
        USE_PK_DATA, USE_OLD_DATA, USE_CHANGED_DATA, USE_TIMESTAMP, USE_VERSION
    };

    public enum ResolveConflict {
        NEWER_WINS, MANUAL, IGNORE, FALLBACK
    };
    
    public enum PingBack {
        OFF, SINGLE_ROW, REMAINING_ROWS
    }

    private String conflictId;
    private String targetChannelId;
    private String targetCatalogName;
    private String targetSchemaName;
    private String targetTableName;
    private DetectConflict detectType = DetectConflict.USE_PK_DATA;
    private String detectExpression;
    private ResolveConflict resolveType = ResolveConflict.FALLBACK;
    private boolean resolveChangesOnly = true;
    private boolean resolveRowOnly = true;
    private Date createTime = new Date();
    private String lastUpdateBy = "symmetricds";
    private Date lastUpdateTime = new Date();
    private PingBack pingBack = PingBack.OFF;

    public String toQualifiedTableName() {
        if (StringUtils.isNotBlank(targetTableName)) {
            return Table.getFullyQualifiedTableName(targetCatalogName, targetSchemaName,
                    targetTableName);
        } else {
            return null;
        }
    }

    public String getConflictId() {
        return conflictId;
    }

    public void setConflictId(String conflictId) {
        this.conflictId = conflictId;
    }

    public String getTargetChannelId() {
        return targetChannelId;
    }

    public void setTargetChannelId(String targetChannelId) {
        this.targetChannelId = targetChannelId;
    }

    public String getTargetCatalogName() {
        return targetCatalogName;
    }

    public void setTargetCatalogName(String targetCatalogName) {
        this.targetCatalogName = targetCatalogName;
    }

    public String getTargetSchemaName() {
        return targetSchemaName;
    }

    public void setTargetSchemaName(String targetSchemaName) {
        this.targetSchemaName = targetSchemaName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }
    
    public DetectConflict getDetectType() {
        return detectType;
    }

    public void setDetectType(DetectConflict detectUpdateType) {
        this.detectType = detectUpdateType;
    }

    public ResolveConflict getResolveType() {
        return resolveType;
    }

    public void setResolveType(ResolveConflict resolveUpdateType) {
        this.resolveType = resolveUpdateType;
    }

    public boolean isResolveChangesOnly() {
        return resolveChangesOnly;
    }
    
    public void setResolveChangesOnly(boolean resolveChangesOnly) {
        this.resolveChangesOnly = resolveChangesOnly;
    }
    
    public boolean isResolveRowOnly() {
        return resolveRowOnly;
    }
    
    public void setResolveRowOnly(boolean resolveRowOnly) {
        this.resolveRowOnly = resolveRowOnly;
    }

    public String getDetectExpression() {
        return detectExpression;
    }

    public void setDetectExpression(String conflictColumnName) {
        this.detectExpression = conflictColumnName;
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

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public void setPingBack(PingBack pingBack) {
        this.pingBack = pingBack;
    }
   
    public PingBack getPingBack() {
        return pingBack;
    }

}
