package org.jumpmind.symmetric.ext;

import org.jumpmind.extension.IExtensionPoint;

public interface INodeRegistrationListener extends IExtensionPoint {
    
    public void registrationUrlUpdated(String url);
    
    public void registrationNextAttemptUpdated(int seconds);
    
    public void registrationStarting();
    
    public void registrationFailed(String message);
    
    public void registrationSyncTriggers();
    
    public void registrationSuccessful();
}