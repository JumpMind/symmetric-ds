package org.jumpmind.symmetric.security;

import org.jumpmind.extension.IExtensionPoint;

/**
 * Used to intercept the saving and rendering of the node password.
 */
public interface INodePasswordFilter extends IExtensionPoint {

	/**
	 * Called on when the node security password is being saved
	 * to the DB.
	 * @param password - The password being saved
	 */
	public String onNodeSecuritySave(String password);
	
	/**
	 * Called on when the password has been
	 * selected from the DB.
	 * @param password - The password to be used
	 */
	public String onNodeSecurityRender(String password);
}