package org.jumpmind.symmetric.android.db;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

public class SQLiteSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected SQLiteSqlTemplate sqlTemplate;

    protected Cursor cursor;

    protected ISqlRowMapper<T> mapper;

    protected int rowNumber = 0;

    public SQLiteSqlReadCursor(String sql, String[] selectionArgs, ISqlRowMapper<T> mapper,
            SQLiteSqlTemplate sqlTemplate) {
        try {
            this.mapper = mapper;
            this.sqlTemplate = sqlTemplate;
            SQLiteDatabase database = sqlTemplate.getDatabase();
            this.cursor = database.rawQuery(sql, selectionArgs);
        } catch (Exception ex) {
            throw this.sqlTemplate.translate(ex);
        }
    }

    public T next() {
        try {
            if (this.cursor.moveToNext()) {
                Map<String, Object> row = getMapForRow();
                rowNumber++;
                if (this.mapper != null) {
                    return this.mapper.mapRow(row);
                }
            }
            return null;
        } catch (Exception ex) {
            throw this.sqlTemplate.translate(ex);
        }
    }

    public void close() {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception ex) {
        }
    }

    protected Map<String, Object> getMapForRow() {
        int columnCount = this.cursor.getColumnCount();
        Map<String, Object> mapOfColValues = new HashMap<String, Object>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String name = this.cursor.getColumnName(i);
            mapOfColValues.put(name, this.cursor.getString(i));
        }
        return mapOfColValues;
    }
}
