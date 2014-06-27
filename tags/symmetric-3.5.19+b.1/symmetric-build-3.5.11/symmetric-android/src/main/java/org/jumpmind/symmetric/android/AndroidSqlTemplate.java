/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.android;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.db.sql.AbstractSqlTemplate;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlResultsListener;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlStatementSource;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.ListSqlStatementSource;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;

import android.content.Context;
import android.content.pm.PackageManager.NameNotFoundException;
import android.database.Cursor;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class AndroidSqlTemplate extends AbstractSqlTemplate {

    protected SQLiteOpenHelper databaseHelper;
    protected Context androidContext;

    public AndroidSqlTemplate(SQLiteOpenHelper databaseHelper, Context androidContext) {
        this.databaseHelper = databaseHelper;
        this.androidContext = androidContext;
    }

    public SQLiteOpenHelper getDatabaseHelper() {
        return databaseHelper;
    }
    
    public byte[] queryForBlob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... params) {
        return queryForBlob(sql, params);
    }

    public byte[] queryForBlob(String sql, Object... params) {
        return queryForObject(sql, byte[].class, params);
    }

    public String queryForClob(String sql, Object... params) {
        return queryForString(sql, params);
    }

    public <T> T queryForObject(String sql, Class<T> clazz, Object... params) {
        SQLiteDatabase database = databaseHelper.getWritableDatabase();
        try {
            return queryForObject(database, sql, clazz, params);
        } catch (Exception ex) {
            throw translate(ex);
        } finally {
            close(database);
        }
    }

    protected <T> T queryForObject(SQLiteDatabase database, String sql, Class<T> clazz,
            Object... params) {
        Cursor cursor = null;
        try {
            T result = null;
            cursor = database.rawQuery(sql, toStringArray(params));
            if (cursor.moveToFirst()) {
                result = get(cursor, clazz, 0);
            }
            return result;
        } catch (Exception ex) {
            throw translate(ex);
        } finally {
            close(cursor);
        }
    }

    public Map<String, Object> queryForMap(final String sql, final Object... args) {
        return queryForObject(sql, new ISqlRowMapper<Map<String, Object>>() {

            public Map<String, Object> mapRow(Row rs) {
                return rs;
            }
        }, args);
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] params, int[] types) {
        return new AndroidSqlReadCursor<T>(sql, toStringArray(params), mapper, this);
    }

    public int update(boolean autoCommit, boolean failOnError, int commitRate,
            ISqlResultsListener resultsListener, String... sql) {
        return this.update(autoCommit, failOnError, true, true,
                commitRate, resultsListener, new ListSqlStatementSource(sql));
    }

    public int update(final boolean autoCommit, final boolean failOnError, boolean failOnDrops,
            boolean failOnSequenceCreate, final int commitRate, final ISqlResultsListener resultsListener, final ISqlStatementSource source) {
        int row = 0;
        SQLiteDatabase database = this.databaseHelper.getWritableDatabase();
        String currentStatement = null;
        try {
            if (!autoCommit) {
                database.beginTransaction();
            }

            for (String statement = source.readSqlStatement(); statement != null; statement = source
                    .readSqlStatement()) {
                currentStatement = statement;
                update(statement);
                row++;
                if (!autoCommit && row % commitRate == 0) {
                    database.setTransactionSuccessful();
                    database.endTransaction();
                    database.beginTransaction();
                }

                if (resultsListener != null) {
                    resultsListener.sqlApplied(statement, row, 0, row);
                }
            }

            if (!autoCommit) {
                database.setTransactionSuccessful();
            }
        } catch (RuntimeException ex) {
            if (resultsListener != null) {
                resultsListener.sqlErrored(currentStatement, translate(currentStatement, ex), row, false, false);
            }
            throw ex;
        } finally {
            if (!autoCommit) {
                database.endTransaction();
            }

            close(database);
        }

        return row;
    }

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
        return update(autoCommit, failOnError, commitRate, (ISqlResultsListener) null, sql);
    }

    public int update(String sql, Object[] values, int[] types) {
        SQLiteDatabase database = this.databaseHelper.getWritableDatabase();
        try {
            return update(database, sql, values, types);
        } finally {
            close(database);
        }
    }

    protected int update(SQLiteDatabase database, String sql, Object[] values, int[] types) {
        try {
            if (values != null) {
                database.execSQL(sql, toStringArray(values));
            } else {
                database.execSQL(sql);
            }
            return queryForObject(database, "select changes()", Integer.class);
        } catch (Exception ex) {
            throw translate(ex);
        }
    }

    /**
     * Translate an array of {@link Object} to an array of {@link String} by
     * creating a new array of {@link String} and putting each of the objects
     * into the array by calling {@link Object#toString()}
     * 
     * @param orig
     *            the original array
     * @return a newly constructed string array
     */
    public static String[] toStringArray(Object[] orig) {
        String[] array = null;
        if (orig != null) {
            array = new String[orig.length];
            for (int i = 0; i < orig.length; i++) {
                if (orig[i] != null) {
                    if (orig[i] instanceof Date) {
                        array[i] = new Timestamp(((Date) orig[i]).getTime()).toString();
                    } else {
                        array[i] = orig[i].toString();
                    }
                }
            }
        }
        return array;
    }

    public void testConnection() {
        SQLiteDatabase database = this.databaseHelper.getWritableDatabase();
        close(database);
    }

    public boolean isUniqueKeyViolation(Throwable ex) {
        return ex instanceof SQLiteConstraintException || ex instanceof UniqueKeyException;
    }

    public boolean isForeignKeyViolation(Throwable ex) {
        return false;
    }

    public ISqlTransaction startSqlTransaction() {
        return new AndroidSqlTransaction(this);
    }

    public int getDatabaseMajorVersion() {
        SQLiteDatabase database = this.databaseHelper.getWritableDatabase();
        try {
            return database.getVersion();
        } catch (Exception ex) {
            throw translate(ex);
        } finally {
            close(database);
        }
    }

    public int getDatabaseMinorVersion() {
        return 0;
    }

    public String getDatabaseProductName() {
        return "sqlite";
    }

    public String getDatabaseProductVersion() {
        return Integer.toString(getDatabaseMajorVersion());
    }

    public String getDriverName() {
        return "android";
    }

    public String getDriverVersion() {
        try {
            return androidContext.getPackageManager().getPackageInfo(
                    androidContext.getPackageName(), 0).versionName;
        } catch (NameNotFoundException e) {
            return "?";
        }
    }

    public Set<String> getSqlKeywords() {
        throw new NotImplementedException();
    }

    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    public boolean isStoresUpperCaseIdentifiers() {
        return false;
    }
    
    public boolean isStoresLowerCaseIdentifiers() {
        return true;
    }

    public boolean isStoresMixedCaseQuotedIdentifiers() {
        return false;
    }

    public long insertWithGeneratedKey(String sql, String column, String sequenceName,
            Object[] params, int[] types) {
        SQLiteDatabase database = databaseHelper.getWritableDatabase();
        try {
            return insertWithGeneratedKey(database, sql, column, sequenceName, params, types);
        } finally {
            close(database);
        }
    }

    protected long insertWithGeneratedKey(SQLiteDatabase database, String sql, String column,
            String sequenceName, Object[] params, int[] types) {
        int updateCount = update(database, sql, params, types);
        if (updateCount > 0) {
            long rowId = queryForObject(database, "SELECT last_insert_rowid()", Integer.class);
            return rowId;
        } else {
            return -1;
        }
    }

    protected void close(SQLiteDatabase database) {
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
        } else if (clazz.equals(Date.class)) {
            String dateString = cursor.getString(columnIndex);
            if (dateString.contains("-")) {
                result = Timestamp.valueOf(dateString);
            } else {
                result = new Date(Long.parseLong(dateString));
            }
        } else if (clazz.equals(Short.class)) {
            result = (Short) cursor.getShort(columnIndex);
        } else if (clazz.equals(byte[].class)) {
            result = (byte[]) cursor.getBlob(columnIndex);
        } else {
            throw new IllegalArgumentException("Unsupported class: " + clazz.getName());
        }
        return (T) result;
    }

}
