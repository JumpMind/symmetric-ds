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
package org.jumpmind.symmetric.io;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.jumpmind.db.model.Table;

public class SimpleDBObjectMapper implements IDBObjectMapper {

    String defaultDatabaseName = "default";

    public Document mapToDocument(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData, boolean mapKeyOnly)
    {
        if (mapKeyOnly) {
            return buildDocumentWithKey(table, newData, oldData, pkData);
        } else {
            return buildDocumentWithKeyAndData(table, newData, oldData, pkData);
        }
    }

    public String mapToCollection(Table table) {
        return table.getName();
    }

    public String mapToDatabase(Table table) {
        String mongoDatabaseName = table.getCatalog();
        if (isNotBlank(mongoDatabaseName) && isNotBlank(table.getSchema())) {
            mongoDatabaseName += ".";
        } else {
            mongoDatabaseName = "";
        }

        if (isNotBlank(table.getSchema())) {
            mongoDatabaseName += table.getSchema();
        }

        if (isBlank(mongoDatabaseName)) {
            mongoDatabaseName = defaultDatabaseName;
        }

        return mongoDatabaseName;
    }

    protected Document buildDocumentWithKey(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData)
    {
        if (oldData == null || oldData.size() == 0) {
            oldData = pkData;
        }
        if (oldData == null || oldData.size() == 0) {
            oldData = newData;
        }

        String[] keyNames = table.getPrimaryKeyColumnNames();
        
        Document document = new Document();
        if (keyNames != null && keyNames.length > 0) {
            for (String keyName : keyNames) {
                document.put(keyName, oldData.get(keyName));
            }
        }
        return document;
    }

    protected Document buildDocumentWithKeyAndData(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData)
    {
        Document document = buildDocumentWithKey(table, newData, oldData, pkData);

        Set<String> newDataKeys = newData.keySet();
        for (String newDataKey : newDataKeys) {
            document.put(newDataKey, newData.get(newDataKey));
        }

        return document;
    }

    public void setDefaultDatabaseName(String defaultDatabaseName) {
        this.defaultDatabaseName = defaultDatabaseName;
    }

}
