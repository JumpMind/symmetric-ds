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

import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.model.Data;

public class VariableColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "variable";

    final String SOURCE_NODE_KEY = String.format("%d.SourceNode", hashCode());

    protected static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    protected static final String DATE_PATTERN = "yyyy-MM-dd";

    protected static final String OPTION_TIMESTAMP = "system_timestamp";

    protected static final String OPTION_DATE = "system_date";

    protected static final String OPTION_SOURCE_NODE_ID = "source_node_id";
    
    protected static final String OPTION_TARGET_NODE_ID = "target_node_id";
    
    protected static final String OPTION_NULL = "null";

    protected static final String OPTION_OLD_VALUE = "old_column_value";
    
    protected static final String OPTION_SOURCE_TABLE_NAME = "source_table_name";
    
    protected static final String OPTION_SOURCE_CATALOG_NAME = "source_catalog_name";
    
    protected static final String OPTION_SOURCE_SCHEMA_NAME = "source_schema_name";

    private static final String[] OPTIONS = new String[] { OPTION_TIMESTAMP, OPTION_DATE,
            OPTION_SOURCE_NODE_ID, OPTION_TARGET_NODE_ID, OPTION_NULL, OPTION_OLD_VALUE, OPTION_SOURCE_CATALOG_NAME,
            OPTION_SOURCE_SCHEMA_NAME, OPTION_SOURCE_TABLE_NAME };

    public String getName() {
        return NAME;
    }

    public boolean isExtractColumnTransform() {
        return true;
    }

    public boolean isLoadColumnTransform() {
        return true;
    }

    public static String[] getOptions() {
        return OPTIONS;
    }

    public String transform(IDatabasePlatform platform,
            DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        String varName = column.getTransformExpression();
        if (varName != null) {
            if (varName.equalsIgnoreCase(OPTION_TIMESTAMP)) {
                return DateFormatUtils.format(System.currentTimeMillis(), TS_PATTERN);
            } else if (varName.equalsIgnoreCase(OPTION_DATE)) {
                return DateFormatUtils.format(System.currentTimeMillis(), DATE_PATTERN);
            } else if (varName.equalsIgnoreCase(OPTION_SOURCE_NODE_ID)) {
                return context.getBatch().getSourceNodeId();
            } else if (varName.equalsIgnoreCase(OPTION_TARGET_NODE_ID)) {
                return context.getBatch().getTargetNodeId();   
            } else if (varName.equalsIgnoreCase(OPTION_OLD_VALUE)) {
                return oldValue;   
            } else if (varName.equals(OPTION_NULL)) {
                return null;
            } else if (varName.equals(OPTION_SOURCE_TABLE_NAME)) {
                Data csvData = (Data)context.get(Constants.DATA_CONTEXT_CURRENT_CSV_DATA);
                if (csvData != null && csvData.getTriggerHistory() != null) {
                    return csvData.getTriggerHistory().getSourceTableName();
                }
            } else if (varName.equals(OPTION_SOURCE_CATALOG_NAME)) {
                Data csvData = (Data)context.get(Constants.DATA_CONTEXT_CURRENT_CSV_DATA);
                if (csvData != null && csvData.getTriggerHistory() != null) {
                    return csvData.getTriggerHistory().getSourceCatalogName();
                }
                
            } else if (varName.equals(OPTION_SOURCE_SCHEMA_NAME)) {
                Data csvData = (Data)context.get(Constants.DATA_CONTEXT_CURRENT_CSV_DATA);
                if (csvData != null && csvData.getTriggerHistory() != null) {
                    return csvData.getTriggerHistory().getSourceSchemaName();
                }                
            }
        }
        return null;
    }

}