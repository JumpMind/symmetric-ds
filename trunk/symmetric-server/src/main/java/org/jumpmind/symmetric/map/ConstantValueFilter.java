package org.jumpmind.symmetric.map;

import org.jumpmind.util.Context;

/**
 * Use this value filter if you want a constant value to always be inserted as a
 * column value.
 */
public class ConstantValueFilter implements IValueFilter {

    private String constant;

    public String filter(String tableName, String columnName, String originalValue, Context context) {
        return constant;
    }

    public void setConstant(String constant) {
        this.constant = constant;
    }

}