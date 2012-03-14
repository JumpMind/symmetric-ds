package org.jumpmind.symmetric.io.data.writer;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;

public class ConflictSetting implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum DetectUpdateConflict {
        USE_PK_DATA, USE_OLD_DATA, USE_CHANGED_DATA, USE_TIMESTAMP, USE_VERSION
    };

    public enum ResolveUpdateConflict {
        NEWER_WINS, MANUAL, IGNORE, FALLBACK
    };

    public enum DetectDeleteConflict {
        USE_PK_DATA, USE_OLD_DATA, USE_TIMESTAMP, USE_VERSION
    };
    
    public enum ResolveDeleteConflict {
        MANUAL, IGNORE, NEWER_WINS
    };
    
    public enum DetectInsertConflict {
        USE_PK_DATA, USE_TIMESTAMP, USE_VERSION
    }

    public enum ResolveInsertConflict {
        NEWER_WINS, MANUAL, IGNORE, FALLBACK
    };

    private String conflictSettingId = "default";
    private String targetChannelId;
    private String targetCatalogName;
    private String targetSchemaName;
    private String targetTableName;
    private DetectUpdateConflict detectUpdateType = DetectUpdateConflict.USE_PK_DATA;
    private DetectInsertConflict detectInsertType = DetectInsertConflict.USE_PK_DATA;
    private DetectDeleteConflict detectDeleteType = DetectDeleteConflict.USE_PK_DATA;
    private String detectExpresssion;
    private ResolveUpdateConflict resolveUpdateType = ResolveUpdateConflict.FALLBACK;
    private ResolveInsertConflict resolveInsertType = ResolveInsertConflict.FALLBACK;
    private ResolveDeleteConflict resolveDeleteType = ResolveDeleteConflict.IGNORE;
    private boolean resolveChangesOnly = true;
    private boolean resolveRowOnly = true;
    private Date createTime = new Date();
    private String lastUpdateBy = "symmetricds";
    private Date lastUpdateTime = new Date();

    public String toQualifiedTableName() {
        if (StringUtils.isNotBlank(targetTableName)) {
            return Table.getFullyQualifiedTableName(targetCatalogName, targetSchemaName,
                    targetTableName);
        } else {
            return null;
        }
    }

    public String getConflictSettingId() {
        return conflictSettingId;
    }

    public void setConflictSettingId(String conflictId) {
        this.conflictSettingId = conflictId;
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
    
    public DetectUpdateConflict getDetectUpdateType() {
        return detectUpdateType;
    }

    public void setDetectUpdateType(DetectUpdateConflict detectUpdateType) {
        this.detectUpdateType = detectUpdateType;
    }
    
    public void setDetectInsertType(DetectInsertConflict detectInsertType) {
        this.detectInsertType = detectInsertType;
    }
    
    public DetectInsertConflict getDetectInsertType() {
        return detectInsertType;
    }

    public DetectDeleteConflict getDetectDeleteType() {
        return detectDeleteType;
    }

    public void setDetectDeleteType(DetectDeleteConflict detectDeleteType) {
        this.detectDeleteType = detectDeleteType;
    }

    public ResolveUpdateConflict getResolveUpdateType() {
        return resolveUpdateType;
    }

    public void setResolveUpdateType(ResolveUpdateConflict resolveUpdateType) {
        this.resolveUpdateType = resolveUpdateType;
    }

    public ResolveInsertConflict getResolveInsertType() {
        return resolveInsertType;
    }

    public void setResolveInsertType(ResolveInsertConflict resolveInsertType) {
        this.resolveInsertType = resolveInsertType;
    }

    public ResolveDeleteConflict getResolveDeleteType() {
        return resolveDeleteType;
    }

    public void setResolveDeleteType(ResolveDeleteConflict resolveDeleteType) {
        this.resolveDeleteType = resolveDeleteType;
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

    public String getDetectExpresssion() {
        return detectExpresssion;
    }

    public void setDetectExpresssion(String conflictColumnName) {
        this.detectExpresssion = conflictColumnName;
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

}
