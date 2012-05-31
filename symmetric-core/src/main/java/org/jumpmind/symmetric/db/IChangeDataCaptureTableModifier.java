package org.jumpmind.symmetric.db;

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IExtensionPoint;

public interface IChangeDataCaptureTableModifier extends IExtensionPoint {
    
    public void modify(Table table);

}
