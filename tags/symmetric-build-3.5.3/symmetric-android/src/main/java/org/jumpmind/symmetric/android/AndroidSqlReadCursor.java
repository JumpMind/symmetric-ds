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

import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

public class AndroidSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected AndroidSqlTemplate sqlTemplate;

    protected Cursor cursor;

    protected ISqlRowMapper<T> mapper;

    protected int rowNumber = 0;
    
    protected SQLiteDatabase database;

    public AndroidSqlReadCursor(String sql, String[] selectionArgs, ISqlRowMapper<T> mapper,
            AndroidSqlTemplate sqlTemplate) {
        try {
            this.mapper = mapper;
            this.sqlTemplate = sqlTemplate;
            this.database = sqlTemplate.getDatabaseHelper().getWritableDatabase();
            this.cursor = database.rawQuery(sql, selectionArgs);
        } catch (Exception ex) {
            throw this.sqlTemplate.translate(ex);
        }
    }

    public T next() {
        try {
            if (this.cursor.moveToNext()) {
                Row row = getMapForRow();
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

    protected Row getMapForRow() {
        int columnCount = this.cursor.getColumnCount();
        Row mapOfColValues = new Row(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String name = this.cursor.getColumnName(i);
            mapOfColValues.put(name, this.cursor.getString(i));
        }
        return mapOfColValues;
    }
}