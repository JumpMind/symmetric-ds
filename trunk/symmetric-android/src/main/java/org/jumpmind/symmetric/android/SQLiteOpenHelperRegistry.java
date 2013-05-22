package org.jumpmind.symmetric.android;

import java.util.HashMap;
import java.util.Map;

import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteOpenHelperRegistry {
    
    static private Map<String, SQLiteOpenHelper> sqliteOpenHelpers = new HashMap<String, SQLiteOpenHelper>();
    
    public static void register(String name, SQLiteOpenHelper helper) {
        sqliteOpenHelpers.put(name, helper);
    }
    
    public static SQLiteOpenHelper lookup(String name) {
        return sqliteOpenHelpers.get(name);
    }

}
