package org.jumpmind.symmetric.ext;

import org.jumpmind.extension.IExtensionPoint;

public interface ICached extends IExtensionPoint {
    public void flushCache();
}
