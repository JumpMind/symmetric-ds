package org.jumpmind.symmetric.android.db;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.db.ISqlTransaction;

import android.database.sqlite.SQLiteDatabase;

public class SQLiteSqlTransaction implements ISqlTransaction {

    protected SQLiteSqlTemplate sqlTemplate;

    protected SQLiteDatabase database;

    protected boolean inBatchMode = true;

    protected int numberOfRowsBeforeBatchFlush = 10;

    protected boolean oldAutoCommitValue;

    protected String sql;

    protected List<Object> markers = new ArrayList<Object>();

    public SQLiteSqlTransaction(SQLiteSqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
        this.database = sqlTemplate.getDatabase();
    }

    public boolean isInBatchMode() {
        return this.inBatchMode;
    }

    public void setInBatchMode(boolean batchMode) {
        this.inBatchMode = batchMode;

    }

    public void setNumberOfRowsBeforeBatchFlush(int numberOfRowsBeforeBatchFlush) {
        this.numberOfRowsBeforeBatchFlush = numberOfRowsBeforeBatchFlush;
    }

    public int getNumberOfRowsBeforeBatchFlush() {
        return this.numberOfRowsBeforeBatchFlush;
    }

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args) {
        return this.sqlTemplate.queryForObject(sql, clazz, args);
    }

    public void commit() {
        if (inBatchMode) {
            try {
                this.database.setTransactionSuccessful();
                this.markers.clear();
            } catch (Exception ex) {
                throw this.sqlTemplate.translate(ex);
            } finally {
                this.database.endTransaction();
            }
        }
    }

    public void rollback() {
        if (inBatchMode) {
            this.markers.clear();
            this.database.endTransaction();
        }
    }

    public void close() {
    }

    public void prepare(String sql) {
        this.sql = sql;
        if (inBatchMode) {
            this.database.beginTransaction();
        }
    }

    public <T> int update(T marker) {
        if (inBatchMode) {
            markers.add(marker);
        }
        return this.sqlTemplate.update(sql);
    }

    public <T> int update(T marker, Object[] values, int[] types) {
        if (inBatchMode) {
            markers.add(marker);
        }
        return this.sqlTemplate.update(sql, values, types);
    }

    public int flush() {        
        return 0;
    }

    public List<Object> getUnflushedMarkers(boolean clear) {
        List<Object> ret = new ArrayList<Object>(markers);
        if (clear) {
            markers.clear();
        }
        return ret;
    }
    
    public Object createSavepoint() {
        return null;
    }
    
    public void releaseSavepoint(Object savePoint) {
    }
    
    public void rollback(Object savePoint) {
    }

}
