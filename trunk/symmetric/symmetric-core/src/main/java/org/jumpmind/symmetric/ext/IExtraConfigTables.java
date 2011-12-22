package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;

public interface IExtraConfigTables extends IExtensionPoint {

    public List<String> provideTableNames();
    
}
