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

import java.math.BigDecimal;

import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.sqlite.SqliteDdlBuilder;
import org.jumpmind.db.platform.sqlite.SqliteDdlReader;
import org.jumpmind.db.sql.ISqlTemplate;

import android.content.Context;
import android.database.sqlite.SQLiteOpenHelper;

public class AndroidDatabasePlatform extends AbstractDatabasePlatform {

    protected SQLiteOpenHelper database;

    protected AndroidSqlTemplate sqlTemplate;
    
    protected Context androidContext;

    public AndroidDatabasePlatform(SQLiteOpenHelper database, Context androidContext) {

        this.database = database;
        this.androidContext = androidContext;
        sqlTemplate = new AndroidSqlTemplate(database, androidContext);
        ddlReader = new SqliteDdlReader(this);
        ddlBuilder = new SqliteDdlBuilder();

    }

    public String getName() {
        return DatabaseNamesConstants.SQLITE;
    }

    public String getDefaultSchema() {
        return null;
    }

    public String getDefaultCatalog() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T) this.database;
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }
    
    @Override
    protected Number parseIntegerObjectValue(String value) {
        return new BigDecimal(value);
    }

}
