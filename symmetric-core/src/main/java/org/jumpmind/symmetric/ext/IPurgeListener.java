package org.jumpmind.symmetric.ext;

import org.jumpmind.extension.IExtensionPoint;

public interface IPurgeListener extends IExtensionPoint {

    public long purgeOutgoing(boolean force);
    
    public long purgeIncoming(boolean force);
}