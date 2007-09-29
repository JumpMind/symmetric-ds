package org.jumpmind.symmetric.model;

import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.db.IDbDialect;

/**
 * This class represents a node who has registered for sync updates.
 */
public class Node extends BaseEntity {

    private static final long serialVersionUID = 5228552569658130763L;

    private String nodeId;

    private String groupId;

    private String externalId;

    private String syncURL;

    /**
     * Record the version of the schema.  This is recorded and managed by the sync software.
     */
    private String schemaVersion;

    /**
     * Record the type of database the node hosts.
     */
    private String databaseType;
    
    private String symmetricVersion = Version.VERSION;

    /**
     * Get the version of the database the node hosts.
     */
    private String databaseVersion;

    private boolean syncEnabled;

    public Node() {
    }

    public Node(IRuntimeConfig runtimeConfig, IDbDialect dbDialect) {
        setGroupId(runtimeConfig.getNodeGroupId());
        setExternalId(runtimeConfig.getExternalId());
        setDatabaseType(dbDialect.getName());
        setDatabaseVersion(dbDialect.getVersion());
        setSyncURL(runtimeConfig.getMyUrl());        
        setSchemaVersion(runtimeConfig.getSchemaVersion());
    }

    public Node(String clientId, String syncURL, String version) {
        this.nodeId = clientId;
        this.syncURL = syncURL;
        this.schemaVersion = version;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String clientId) {
        this.nodeId = clientId;
    }

    public String getSyncURL() {
        return syncURL;
    }

    public void setSyncURL(String syncURL) {
        this.syncURL = syncURL;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String version) {
        this.schemaVersion = version;
    }

    public boolean isSyncEnabled() {
        return syncEnabled;
    }

    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    public void setDatabaseVersion(String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String domainId) {
        this.externalId = domainId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String domainName) {
        this.groupId = domainName;
    }

    public String getSymmetricVersion() {
        return symmetricVersion;
    }

    public void setSymmetricVersion(String symmetricVersion) {
        this.symmetricVersion = symmetricVersion;
    }
    
    public String toString() {
        return groupId + ":" + externalId + ":" + (nodeId == null ? "?" : nodeId);
    }
}
