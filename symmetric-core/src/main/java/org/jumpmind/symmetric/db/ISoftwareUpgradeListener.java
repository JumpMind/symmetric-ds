package org.jumpmind.symmetric.db;

import org.jumpmind.extension.IBuiltInExtensionPoint;

public interface ISoftwareUpgradeListener extends IBuiltInExtensionPoint {
	
	public void upgrade(String databaseVersion, String softwareVersion);
}
