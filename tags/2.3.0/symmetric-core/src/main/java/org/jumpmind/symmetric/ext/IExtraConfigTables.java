package org.jumpmind.symmetric.ext;

import java.util.List;

public interface IExtraConfigTables extends IExtensionPoint {

    public List<String> provideTableNames();
    
}
