package org.jumpmind.symmetric.core.db;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;

import org.jumpmind.symmetric.core.common.DateUtils;

public class Row extends HashMap<String, Object> {

    private static final long serialVersionUID = 1L;

    public Row(int numberOfColumns) {
        super(numberOfColumns);
    }
    
    public Row(String columnName, Object value) {
        super(1);
        put(columnName, value);
    }


    public String stringValue() {
        Object obj = this.values().iterator().next();
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }
    
    public String getString(String columnName) {
        Object obj = this.get(columnName);
        if (obj != null) {
            return obj.toString();
        } else {
            checkForColumn(columnName);
            return null;
        }
    }
    
    public int getInt(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            return ((Number)obj).intValue();
        } else if (obj instanceof String){
            return Integer.parseInt(obj.toString());
        } else {
            checkForColumn(columnName);
            return 0;
        }
    }
    
    public long getLong(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            return ((Number)obj).longValue();
        } else if (obj instanceof String){
            return Long.parseLong(obj.toString());
        } else {
            checkForColumn(columnName);
            return 0;
        }
    }
    
    public boolean getBoolean(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            int value = ((Number)obj).intValue();
            return value > 0 ? true : false;
        } else if (obj instanceof Boolean){
            return (Boolean)obj;
        } else if (obj instanceof String) {
            return Boolean.parseBoolean((String)obj);
        } else {
            checkForColumn(columnName);
            return false;
        }
    }
    
    public Date getDateTime(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            long value = ((Number)obj).longValue();
            return new Date(value);
        } else if (obj instanceof Date) {
            return (Date)obj;
        } else if (obj instanceof String) {
            return getDate((String)obj, AbstractDbDialect.TIMESTAMP_PATTERNS);
        } else {
            checkForColumn(columnName);
            return null;
        }
    }
    
    protected void checkForColumn(String columnName) {
        if (!containsKey(columnName)) {
            throw new ColumnNotFoundException(columnName);
        }
    }

    final private java.util.Date getDate(String value, String[] pattern) {
        try {
            return DateUtils.parseDate(value, pattern);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
