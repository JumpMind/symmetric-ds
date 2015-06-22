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
package org.jumpmind.db.platform;


import java.util.List;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

public interface IDdlReader {

    public Database readTables(String catalog, String schema, String[] tableTypes);

    public Table readTable(String catalog, String schema, String tableName);
    
    public List<String> getCatalogNames();
    
    public List<String> getSchemaNames(String catalog);
    
    public List<String> getTableNames(String catalog, String schema, String[] tableTypes);
    
    public List<String> getColumnNames(String catalog, String schema, String tableName);
    
}