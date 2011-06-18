package org.jumpmind.symmetric.android.db;

import org.jumpmind.symmetric.core.common.ArrayUtils;
import org.jumpmind.symmetric.core.db.AbstractSqlTemplate;
import org.jumpmind.symmetric.core.db.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.db.SqlException;

import android.database.Cursor;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteSqlTemplate extends AbstractSqlTemplate implements ISqlTemplate {

    protected SQLiteDatabase database;
    protected IDbDialect dbDialect;

    public SQLiteSqlTemplate(SQLiteOpenHelper helper, IDbDialect dbDialect) {
        this.database = helper.getWritableDatabase();
        this.dbDialect = dbDialect;
    }

    public SQLiteDatabase getDatabase() {
        return database;
    }

    public IDbDialect getDbDialect() {
        return this.dbDialect;
    }

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args) {
        Cursor cursor = null;
        try {
            T result = null;
            cursor = database.rawQuery(sql, ArrayUtils.toStringArray(args));
            if (cursor.moveToFirst()) {
                result = get(cursor, clazz, 1);
            }
            return result;
        } catch (Exception ex) {
            throw translate(ex);
        } finally {
            close(cursor);
        }
    }

    protected void close(Cursor cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception ex) {
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T get(Cursor cursor, Class<T> clazz, int columnIndex) {
        Object result = null;
        if (clazz.equals(String.class)) {
            result = (String) cursor.getString(columnIndex);
        } else if (clazz.equals(Integer.class)) {
            result = (Integer) cursor.getInt(columnIndex);
        } else if (clazz.equals(Integer.class)) {
            result = (Double) cursor.getDouble(columnIndex);
        } else if (clazz.equals(Float.class)) {
            result = (Float) cursor.getFloat(columnIndex);
        } else if (clazz.equals(Long.class)) {
            result = (Long) cursor.getLong(columnIndex);
        } else if (clazz.equals(Short.class)) {
            result = (Short) cursor.getShort(columnIndex);
        } else if (clazz.equals(byte[].class)) {
            result = (byte[]) cursor.getBlob(columnIndex);
        } else {
            throw new IllegalArgumentException("Unsupported class: " + clazz.getName());
        }
        return (T) result;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types) {
        return new SQLiteSqlReadCursor<T>(sql, ArrayUtils.toStringArray(values), mapper, this);
    }

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
        int row = 0;
        try {
            if (!autoCommit) {
                this.database.beginTransaction();
            }

            for (String statement : sql) {
                update(statement);
                row++;
                if (!autoCommit && row % commitRate == 0) {
                    this.database.setTransactionSuccessful();
                    this.database.endTransaction();
                    if (sql.length > row) {
                        this.database.beginTransaction();
                    }
                }
            }

            if (!autoCommit) {
                this.database.setTransactionSuccessful();
            }
        } finally {
            if (!autoCommit) {
                this.database.endTransaction();
            }
        }

        return row;
    }

    public int update(String sql, Object[] values, int[] types) {
        try {
            if (values != null) {
                this.database.execSQL(sql, ArrayUtils.toStringArray(values));
            } else {
                this.database.execSQL(sql);
            }
            return 1;
        } catch (Exception ex) {
            throw translate(ex);
        }
    }

    public void testConnection() {
        queryForInt("select 1");
    }

    public SqlException translate(Exception ex) {
        if (ex instanceof SQLiteConstraintException) {
            return new DataIntegrityViolationException(ex);
        } else if (ex instanceof SqlException) {
            return (SqlException) ex;
        } else {
            return new SqlException(ex);
        }
    }

    public ISqlTransaction startSqlTransaction() {
        return new SQLiteSqlTransaction(this);
    }

    public int getDatabaseMajorVersion() {
        return database.getVersion();
    }
    
    public int getDatabaseMinorVersion() {
        return 0;
    }
    
    
    public String getDatabaseProductName() {
     
        return "SQLite";
    }
}
