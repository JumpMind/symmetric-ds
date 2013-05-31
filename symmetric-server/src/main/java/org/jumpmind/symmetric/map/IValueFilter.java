package org.jumpmind.symmetric.map;

import org.jumpmind.util.Context;

public interface IValueFilter {

    public String filter (String tableName, String columnName, String originalValue, Context context);
    
}