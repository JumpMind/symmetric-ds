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
package org.jumpmind.db.platform.redshift;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

public class RedshiftDdlReader extends AbstractJdbcDdlReader {

    public RedshiftDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
    }
    
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);
        
        if (column.getJdbcTypeCode() == Types.VARCHAR && column.getSizeAsInt() == 65535) {
            column.setJdbcTypeCode(Types.LONGVARCHAR);
            column.setMappedTypeCode(Types.LONGVARCHAR);
            column.setSize(null);
        }

        String defaultValue = column.getDefaultValue();
        if ((defaultValue != null) && (defaultValue.length() > 0)) {
            // If the default value looks like "identity"(102643, 0, '1,1'::text)
            // then it is an auto-increment column
            if (defaultValue.startsWith("\"identity\"")) {
                column.setAutoIncrement(true);
                defaultValue = null;
            } else {
                // PostgreSQL returns default values in the forms
                // "-9000000000000000000::bigint" or
                // "'some value'::character varying" or "'2000-01-01'::date"
                switch (column.getMappedTypeCode()) {
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    defaultValue = extractUndelimitedDefaultValue(defaultValue);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    defaultValue = extractDelimitedDefaultValue(defaultValue);
                    break;
                }
                if (TypeMap.isTextType(column.getMappedTypeCode())) {
                    // We assume escaping via double quote (see also the
                    // backslash_quote setting:
                    // http://www.postgresql.org/docs/7.4/interactive/runtime-config.html#RUNTIME-CONFIG-COMPATIBLE)
                    defaultValue = unescape(defaultValue, "'", "''");
                }
            }
            column.setDefaultValue(defaultValue);
        }
        return column;
    }

    /*
     * Extracts the default value from a default value spec of the form
     * "'some value'::character varying" or "'2000-01-01'::date".
     * 
     * @param defaultValue The default value spec
     * 
     * @return The default value
     */
    private String extractDelimitedDefaultValue(String defaultValue) {
        if (defaultValue.startsWith("'")) {
            int valueEnd = defaultValue.indexOf("'::");

            if (valueEnd > 0) {
                return defaultValue.substring("'".length(), valueEnd);
            }
        }
        return defaultValue;
    }

    /*
     * Extracts the default value from a default value spec of the form
     * "-9000000000000000000::bigint".
     * 
     * @param defaultValue The default value spec
     * 
     * @return The default value
     */
    private String extractUndelimitedDefaultValue(String defaultValue) {
        int valueEnd = defaultValue.indexOf("::");

        if (valueEnd > 0) {
            defaultValue = defaultValue.substring(0, valueEnd);
        } else {
            if (defaultValue.startsWith("(") && defaultValue.endsWith(")")) {
                defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
            }
        }
        return defaultValue;
    }

}
