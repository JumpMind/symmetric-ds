package org.jumpmind.symmetric.security;

import org.jumpmind.extension.IExtensionPoint;

public interface ISmtpPasswordFilter extends IExtensionPoint {
    /**
     * Called on when the node security password is being saved to the DB.
     * 
     * @param password
     *            - The password being saved
     */
    public String onSmtpPasswordSave(String password);

    /**
     * Called on when the password has been selected from the DB.
     * 
     * @param password
     *            - The password to be used
     */
    public String onSmtpPasswordRender(String password);
}
