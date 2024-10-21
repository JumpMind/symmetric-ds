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
package org.jumpmind.symmetric.db.mssql;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.Types;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.util.FormatUtils;

public class MsSql2016TriggerTemplate extends MsSql2008TriggerTemplate {
    protected String varcharMaxColumnTemplate;

    public MsSql2016TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar(40), $(tableAlias).\"$(columnName)\", 3) + '\"') end";
        moneyColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar(40), $(tableAlias).\"$(columnName)\", 2) + '\"') end";
        varcharMaxColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert(" +
                (this.castToNVARCHAR ? "n" : "")
                + "varchar(MAX),$(tableAlias).\"$(columnName)\") $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end";
    }

    @Override
    protected String getCreateTriggerString() {
        if (symmetricDialect.getPlatform().getDatabaseInfo().isTriggersCreateOrReplaceSupported()) {
            return "create or alter trigger";
        }
        return super.getCreateTriggerString();
    }

    /***
     * SQL Server 2016-and up specific determination of NVARCHAR(MAX) columns as regular strings, not LOBs.
     */
    @Override
    protected ColumnString fillOutColumnTemplate(String origTableAlias, String tableAlias,
            String columnPrefix, Table table, Column column, DataEventType dml, boolean isOld, Channel channel,
            Trigger trigger, boolean ignoreStreamLobs) {
        // boolean isVarCharMax = false;
        boolean isLob = symmetricDialect.getPlatform().isLob(column.getMappedTypeCode());
        String templateToUse = null;
        if (column.getJdbcTypeName() != null
                && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY))
                && StringUtils.isNotBlank(geometryColumnTemplate)) {
            templateToUse = geometryColumnTemplate;
        } else if (column.getJdbcTypeName() != null
                && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOGRAPHY))
                && StringUtils.isNotBlank(geographyColumnTemplate)) {
            templateToUse = geographyColumnTemplate;
        } else {
            switch (column.getMappedTypeCode()) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    templateToUse = numberColumnTemplate;
                    if (moneyColumnTemplate != null && column.getJdbcTypeName() != null && column.getJdbcTypeName().toUpperCase().contains("MONEY")) {
                        templateToUse = moneyColumnTemplate;
                    }
                    break;
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case ColumnTypes.NVARCHAR:
                    templateToUse = stringColumnTemplate;
                    break;
                case ColumnTypes.SQLXML:
                    templateToUse = xmlColumnTemplate;
                    break;
                case Types.ARRAY:
                    templateToUse = arrayColumnTemplate;
                    break;
                case Types.LONGVARCHAR:
                case ColumnTypes.LONGNVARCHAR:
                case ColumnTypes.MSSQL_VARCHARMAX:
                case ColumnTypes.MSSQL_NVARCHARMAX:
                    if (column.getJdbcTypeCode() == ColumnTypes.MSSQL_VARCHARMAX || column.getJdbcTypeCode() == ColumnTypes.MSSQL_NVARCHARMAX) {
                        templateToUse = varcharMaxColumnTemplate;
                        isLob = false;
                        // isVarCharMax = true;
                        break;
                    } else
                    if (column.getJdbcTypeName().equalsIgnoreCase("LONG") && isNotBlank(longColumnTemplate)) {
                        templateToUse = longColumnTemplate;
                        isLob = false;
                        break;
                    } else if (!isLob) {
                        templateToUse = stringColumnTemplate;
                        break;
                    }
                case Types.CLOB:
                case Types.NCLOB:
                    if (isOld && symmetricDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = clobColumnTemplate;
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    if (isNotBlank(binaryColumnTemplate)) {
                        templateToUse = binaryColumnTemplate;
                        break;
                    }
                case Types.BLOB:
                    if (requiresWrappedBlobTemplateForBlobType()) {
                        templateToUse = wrappedBlobColumnTemplate;
                        break;
                    }
                case Types.LONGVARBINARY:
                case ColumnTypes.MSSQL_NTEXT:
                    if (column.getJdbcTypeName() != null
                            && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.IMAGE))
                            && StringUtils.isNotBlank(imageColumnTemplate)) {
                        if (isOld) {
                            templateToUse = emptyColumnTemplate;
                        } else {
                            templateToUse = imageColumnTemplate;
                        }
                    } else if (isOld && symmetricDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = blobColumnTemplate;
                    }
                    break;
                case Types.DATE:
                    if (noDateColumnTemplate()) {
                        templateToUse = datetimeColumnTemplate;
                        break;
                    }
                    templateToUse = dateColumnTemplate;
                    break;
                case Types.TIME:
                    if (noTimeColumnTemplate()) {
                        templateToUse = datetimeColumnTemplate;
                        break;
                    }
                    templateToUse = timeColumnTemplate;
                    break;
                case Types.TIMESTAMP:
                    templateToUse = datetimeColumnTemplate;
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    templateToUse = booleanColumnTemplate;
                    break;
                case Types.ROWID:
                    templateToUse = stringColumnTemplate;
                    break;
                default:
                    if (column.getJdbcTypeName() != null) {
                        if (column.getJdbcTypeName().toUpperCase().equals(TypeMap.INTERVAL)) {
                            templateToUse = numberColumnTemplate;
                            break;
                        } else if (column.getMappedType().equals(TypeMap.TIMESTAMPTZ)
                                && StringUtils.isNotBlank(this.dateTimeWithTimeZoneColumnTemplate)) {
                            templateToUse = this.dateTimeWithTimeZoneColumnTemplate;
                            break;
                        } else if (column.getMappedType().equals(TypeMap.TIMESTAMPLTZ)
                                && StringUtils
                                        .isNotBlank(this.dateTimeWithLocalTimeZoneColumnTemplate)) {
                            templateToUse = this.dateTimeWithLocalTimeZoneColumnTemplate;
                            break;
                        }
                    }
                    if (StringUtils.isBlank(templateToUse)
                            && StringUtils.isNotBlank(this.otherColumnTemplate)) {
                        templateToUse = this.otherColumnTemplate;
                        break;
                    }
                    throw new NotImplementedException(column.getName() + " is of type "
                            + column.getMappedType() + " with JDBC type of "
                            + column.getJdbcTypeName());
            }
        }
        if (dml == DataEventType.DELETE && isLob && requiresEmptyLobTemplateForDeletes()) {
            templateToUse = emptyColumnTemplate;
        } else if (isLob && trigger.isUseStreamLobs() && !ignoreStreamLobs) {
            templateToUse = emptyColumnTemplate;
        }
        if (templateToUse != null) {
            templateToUse = adjustColumnTemplate(templateToUse, column.getMappedTypeCode());
            templateToUse = templateToUse.trim();
        } else {
            throw new NotImplementedException("Table " + table + " column " + column);
        }
        String formattedColumnText = FormatUtils.replace("columnSizeOrMax",
                (trigger.isUseCaptureLobs()) ? "max" : "$(columnSize)", templateToUse);
        formattedColumnText = FormatUtils.replace("columnName",
                String.format("%s%s", columnPrefix, column.getName()), formattedColumnText);
        formattedColumnText = FormatUtils.replace("columnSize",
                getColumnSize(table, column), formattedColumnText);
        formattedColumnText = FormatUtils.replace("masterCollation",
                symmetricDialect.getMasterCollation(), formattedColumnText);
        if (isLob) {
            formattedColumnText = symmetricDialect.massageForLob(formattedColumnText, channel != null ? channel.isContainsBigLob() : true);
        }
        formattedColumnText = FormatUtils.replace("origTableAlias", origTableAlias,
                formattedColumnText);
        formattedColumnText = FormatUtils.replace("tableAlias", tableAlias, formattedColumnText);
        formattedColumnText = FormatUtils.replace("prefixName", symmetricDialect.getTablePrefix(),
                formattedColumnText);
        return new ColumnString(formattedColumnText, isLob);
    }
}
