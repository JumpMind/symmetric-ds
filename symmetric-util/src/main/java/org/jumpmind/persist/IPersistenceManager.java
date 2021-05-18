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
package org.jumpmind.persist;

import java.util.List;
import java.util.Map;

public interface IPersistenceManager {
    
    public <T> int count(Class<T> clazz, Map<String, Object> conditions);
    
    public <T> int count(Class<T> clazz, String catalogName,
            String schemaName, String tableName, Map<String, Object> conditions);
    
    public int count(String catalogName, String schemaName, String tableName);
    
    public <T> T map(Map<String, Object> row, Class<T> clazz, String catalogName, String schemaName, String tableName);

    public void refresh(Object object, String catalogName, String schemaName, String tableName);

    public <T> List<T> find(Class<T> clazz);

    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions);

    public <T> List<T> find(Class<T> clazz, String catalogName, String schemaName, String tableName);

    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions, String catalogName,
            String schemaName, String tableName);

    public boolean save(Object object, String catalogName, String schemaName, String tableName);

    public boolean save(Object object);

    public boolean delete(Object object, String catalogName, String schemaName, String tableName);

    public boolean delete(Object object);

    public void insert(Object object, String catalogName, String schemaName, String tableName);

    public int update(Object object, String catalogName, String schemaName, String tableName);

}
