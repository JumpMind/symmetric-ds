package org.jumpmind.symmetric.android;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;

import android.database.sqlite.SQLiteDatabase;

public class AndroidSqlTransaction implements ISqlTransaction {

    protected AndroidSqlTemplate sqlTemplate;

    protected SQLiteDatabase database;

    protected boolean oldAutoCommitValue;

    protected String sql;
    
    protected boolean needsRolledback = false;

    public AndroidSqlTransaction(AndroidSqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
        this.database = sqlTemplate.getDatabaseHelper().getWritableDatabase();
        this.database.beginTransaction();
    }

    public void setInBatchMode(boolean batchMode) {
    }

    public boolean isInBatchMode() {
        return false;
    }
    
    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Map<String, Object> namedParams) {
        return sqlTemplate.query(sql, mapper, namedParams);
    }
    
    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object[] args, int[] types) {
        return sqlTemplate.query(sql, mapper, args, types);
    }

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args) {
        return this.sqlTemplate.queryForObject(database, sql, clazz, args);
    }

    public void commit() {
        try {
            this.database.setTransactionSuccessful();
        } catch (Exception ex) {
            throw this.sqlTemplate.translate(ex);
        } finally {
            this.database.endTransaction();
            this.database.beginTransaction();
        }
    }

    public void rollback() {
        needsRolledback = true;
    }

    public void close() {
        if (!needsRolledback) {
            this.database.setTransactionSuccessful();
        }
        this.database.endTransaction();
        this.database = null;
    }

    public void prepare(String sql) {
        this.sql = sql;
    }

    public <T> int addRow(T marker, Object[] values, int[] types) {
        return this.sqlTemplate.update(database, sql, values, types);
    }

    public int flush() {
        return 0;
    }

    public int queryForInt(String sql, Object... args) {
        return sqlTemplate.queryForObject(database, sql, Integer.class, args);
    }

    public long queryForLong(String sql, Object... args) {
        return sqlTemplate.queryForObject(database, sql, Long.class, args);
    }
    
    public int execute(String sql) {
        return sqlTemplate.update(database, sql, null, null);
    }

    public int prepareAndExecute(String sql, Object[] args, int[] types) {
        return sqlTemplate.update(database, sql, args, types);
    }

    public int prepareAndExecute(String sql, Object... args) {
        return sqlTemplate.update(database, sql, args, null);
    }

    public List<Object> getUnflushedMarkers(boolean clear) {
        return new ArrayList<Object>(0);
    }

    public void allowInsertIntoAutoIncrementColumns(boolean value, Table table, String quote) {
    }

    public long insertWithGeneratedKey(String sql, String column, String sequenceName,
            Object[] args, int[] types) {
        return sqlTemplate.insertWithGeneratedKey(database, sql, column, sequenceName, args, null);
    }

}