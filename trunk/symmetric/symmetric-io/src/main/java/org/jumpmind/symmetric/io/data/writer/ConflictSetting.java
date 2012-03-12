package org.jumpmind.symmetric.io.data.writer;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;

public class ConflictSetting implements Serializable {

    public enum DetectUpdateConflict {
        USE_PK_DATA, USE_OLD_DATA, USE_CHANGED_DATA, USE_TIMESTAMP, USE_VERSION
    };

    public enum ResolveUpdateConflict {
        NEWER_WINS_ROW, MANUAL, IGNORE_ROW, IGNORE_BATCH, FALLBACK_ALL
    };

    public enum DetectDeleteConflict {
        USE_PK_DATA, USE_OLD_DATA, USE_TIMESTAMP, USE_VERSION
    };

    public enum ResolveDeleteConflict {
        MANUAL, IGNORE_ROW, IGNORE_BATCH, NEWER_WINS_ROW
    };

    public enum ResolveInsertConflict {
        NEWER_WINS_ROW, MANUAL, IGNORE_ROW, IGNORE_BATCH, FALLBACK_ALL, FALLBACK_CHANGES
    };

    private static final long serialVersionUID = 1L;

    private String conflictSettingId = "default";
    private String targetChannelId;
    private String targetCatalogName;
    private String targetSchemaName;
    private String targetTableName;
    private DetectUpdateConflict detectUpdateType = DetectUpdateConflict.USE_PK_DATA;
    private DetectDeleteConflict detectDeleteType = DetectDeleteConflict.USE_PK_DATA;
    private String detectExpresssion;
    private ResolveUpdateConflict resolveUpdateType = ResolveUpdateConflict.FALLBACK_ALL;
    private ResolveInsertConflict resolveInsertType = ResolveInsertConflict.FALLBACK_CHANGES;
    private ResolveDeleteConflict resolveDeleteType = ResolveDeleteConflict.IGNORE_ROW;
    private boolean resolveAuditEnabled;
    private int retryCount = -1;
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

    public String getDetectExpresssion() {
        return detectExpresssion;
    }

    public void setDetectExpresssion(String conflictColumnName) {
        this.detectExpresssion = conflictColumnName;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
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

    public boolean isResolveAuditEnabled() {
        return resolveAuditEnabled;
    }

    public void setResolveAuditEnabled(boolean resolveAuditEnabled) {
        this.resolveAuditEnabled = resolveAuditEnabled;
    }
}
