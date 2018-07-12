package org.jumpmind.symmetric.file;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.FileSnapshot;

public interface IFileConflictResolver extends IExtensionPoint {

    public String getName();
    
    public String getResolveCommand(FileSnapshot snapshot);
}
