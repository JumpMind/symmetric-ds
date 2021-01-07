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
