package org.jumpmind.symmetric.load;

import org.jumpmind.extension.IExtensionPoint;

public interface IClientReloadListener extends IExtensionPoint {

    void reloadStarted();

    void reloadCompleted();

}
