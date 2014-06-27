/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.db.platform.mariadb;

import javax.sql.DataSource;

import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MariaDBDatabasePlatform extends MySqlDatabasePlatform {

    public static final String SQL_GET_MARIADB_NAME = "select variable_value from information_schema.global_variables where variable_name='VERSION'";

	public MariaDBDatabasePlatform(DataSource dataSource,
			SqlTemplateSettings settings) {
		super(dataSource, settings);
	}

}
