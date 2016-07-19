package org.jumpmind.db.model;

import java.util.HashMap;
import java.util.Map;

public class Trigger {
	
	public enum TriggerType { INSERT, UPDATE, DELETE };

	String triggerName;
	
	String catalogName;
	
	String schemaName;
	
	String tableName;
	
	String source;
	
	TriggerType triggerType;
	
	boolean enabled;
	
	Map<String, Object> metaData = new HashMap<String, Object>();
	
	public Trigger(String name, String catalogName, String schemaName, String tableName, TriggerType triggerType) {
		this(name, catalogName, schemaName, tableName, triggerType, true);
	}
	
	public Trigger(String name, String catalogName, String schemaName, String tableName, TriggerType triggerType, boolean enabled) {
		this.triggerName = name;
		this.catalogName = catalogName;
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.triggerType = triggerType;
		this.enabled = enabled;
	}
	
	public Trigger() {
	}

	public String getName() {
		return triggerName;
	}

	public void setName(String name) {
		this.triggerName = name;
	}

	public String getCatalogName() {
		return catalogName;
	}

	public void setCatalogName(String catalogName) {
		this.catalogName = catalogName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public Map<String, Object> getMetaData() {
		return metaData;
	}

	public void setMetaData(Map<String, Object> metaData) {
		this.metaData = metaData;
	}
	

	public TriggerType getTriggerType() {
		return triggerType;
	}
	
	public void setTriggerType(TriggerType triggerType) {
		this.triggerType = triggerType;
	}
	
	public String getFullyQualifiedName() {
		return getFullyQualifiedName(catalogName, schemaName, tableName, triggerName);
	}
	
	public static String getFullyQualifiedName(String catalog, String schema, String tableName, String triggerName) {
		String fullName = "";
		if (catalog != null) fullName += catalog+".";
		if (schema != null) fullName += schema+".";
		fullName += tableName+"."+triggerName;
		return fullName;
	}
}
