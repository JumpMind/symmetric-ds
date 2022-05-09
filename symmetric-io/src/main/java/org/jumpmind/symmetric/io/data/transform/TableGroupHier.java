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
package org.jumpmind.symmetric.io.data.transform;

import java.io.StringReader;
import java.util.Date;

import org.jumpmind.exception.ParseException;
import org.jumpmind.symmetric.csv.CsvReader;

public class TableGroupHier implements Cloneable {
    private String id;
    private String tableGroupId;
    private String sourceCatalogName;
    private String sourceSchemaName;
    private String sourceTableName;
    private String parentId;
    private RelationType parentRelationType;
    private String pkColumnNames;
    private String fkColumnNames;
    private String[] parsedPkColumnNames;
    private String[] parsedFkColumnNames;
    private Date createTime;
    private Date lastUpdateTime;
    private String lastUpdateBy;
    private transient TableGroupHier parent;
    private transient String groupSql;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableGroupHier && id != null) {
            return id.equals(((TableGroupHier) obj).id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (id != null) {
            return id.hashCode();
        }
        return super.hashCode();
    }

    @Override
    public String toString() {
        if (id != null) {
            return id;
        }
        return super.toString();
    }

    protected String[] parseColumnNames(String argColumnNames) {
        if (argColumnNames.indexOf('"') == -1) {
            return argColumnNames.split(",");
        }
        try {
            CsvReader reader = new CsvReader(new StringReader(argColumnNames), ',');
            if (reader.readRecord()) {
                return reader.getValues();
            } else {
                throw new ParseException("Failed to read a record from CsvReader.");
            }
        } catch (Exception ex) {
            throw new ParseException("Failed to parse columns [" + argColumnNames + "]", ex);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public String getSourceCatalogName() {
        return sourceCatalogName;
    }

    public void setSourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String sourceSchemaName) {
        this.sourceSchemaName = sourceSchemaName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public RelationType getParentRelationType() {
        return parentRelationType;
    }

    public void setParentRelationType(RelationType parentRelationType) {
        this.parentRelationType = parentRelationType;
    }

    public String getPkColumnNames() {
        return pkColumnNames;
    }

    public String[] getParsedPkColumnNames() {
        if (parsedPkColumnNames == null && pkColumnNames != null) {
            parsedPkColumnNames = parseColumnNames(pkColumnNames);
        }
        return parsedPkColumnNames;
    }

    public void setPkColumnNames(String pkColumnNames) {
        this.pkColumnNames = pkColumnNames;
        this.parsedPkColumnNames = null;
    }

    public String getFkColumnNames() {
        return fkColumnNames;
    }

    public String[] getParsedFkColumnNames() {
        if (parsedFkColumnNames == null && fkColumnNames != null) {
            parsedFkColumnNames = parseColumnNames(fkColumnNames);
        }
        return parsedFkColumnNames;
    }

    public void setFkColumnNames(String fkColumnNames) {
        this.fkColumnNames = fkColumnNames;
        this.parsedFkColumnNames = null;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setCreateTime(Date createdOn) {
        this.createTime = createdOn;
    }

    public void setLastUpdateTime(Date lastModifiedOn) {
        this.lastUpdateTime = lastModifiedOn;
    }

    public void setLastUpdateBy(String updatedBy) {
        this.lastUpdateBy = updatedBy;
    }

    public TableGroupHier getParent() {
        return parent;
    }

    public void setParent(TableGroupHier parent) {
        this.parent = parent;
    }

    public String getGroupSql() {
        return groupSql;
    }

    public void setGroupSql(String groupSql) {
        this.groupSql = groupSql;
    }

    public enum RelationType {
        OBJECT("O"), ARRAY("A");

        private String code;

        RelationType(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public static RelationType getRelationType(String s) {
            if (s.equals(ARRAY.getCode())) {
                return ARRAY;
            } else {
                return OBJECT;
            }
        }
    }
}