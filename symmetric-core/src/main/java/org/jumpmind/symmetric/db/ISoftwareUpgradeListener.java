package org.jumpmind.symmetric.db;

import org.jumpmind.extension.IExtensionPoint;

public interface ISoftwareUpgradeListener extends IExtensionPoint {
	
	public void upgrade(String databaseVersion, String softwareVersion);
}
