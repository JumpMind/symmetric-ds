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
package org.jumpmind.symmetric.extract.csv;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

abstract class AbstractStreamDataCommand implements IStreamDataCommand {

    protected static final String DELIMITER = CsvUtils.DELIMITER;

    protected final ILog log = LogFactory.getLog(getClass());

    protected ITriggerRouterService triggerRouterService;

    protected IDbDialect dbDialect;

    protected void selectAndEnhanceWithLobsIfEnabled(Data data, DataExtractorContext context) {
        TriggerHistory triggerHistory = data.getTriggerHistory();
        if (triggerHistory != null) {
            Trigger trigger = findTrigger(triggerHistory);
            if (trigger != null) {
                Table table = dbDialect.getTable(trigger, true);
                if (table != null) {
                    if (trigger.isUseStreamLobs()) {
                        final List<Column> lobColumns = getLobColumns(table);
                        if (lobColumns.size() > 0) {
                            try {
                                final String[] columnNames = triggerHistory.getParsedColumnNames();
                                final String[] rowData = data.toParsedRowData();
                                Column[] orderedColumns = dbDialect
                                        .orderColumns(columnNames, table);
                                Object[] objectValues = dbDialect.getObjectValues(
                                        dbDialect.getBinaryEncoding(), rowData, orderedColumns);
                                Map<String, Object> columnDataMap = AppUtils.toMap(columnNames,
                                        objectValues);
                                Column[] pkColumns = table.getPrimaryKeyColumns();
                                String sql = buildSelect(table, lobColumns, pkColumns);
                                JdbcTemplate template = context.getJdbcTemplate();
                                Object[] args = new Object[pkColumns.length];
                                int[] types = new int[pkColumns.length];
                                for (int i = 0; i < pkColumns.length; i++) {
                                    args[i] = columnDataMap.get(pkColumns[i].getName());
                                    types[i] = pkColumns[i].getTypeCode();
                                }
                                template.query(sql, args, types, new RowMapper<Object>() {
                                    public Object mapRow(ResultSet rs, int rowNum)
                                            throws SQLException {
                                        LobHandler lobHandler = dbDialect.getLobHandler();
                                        if (lobHandler == null) {
                                            // If there isn't a lob handler already defined for a platform
                                            // then use the default.  
                                            lobHandler = new DefaultLobHandler();
                                        }
                                        for (Column col : lobColumns) {
                                            String valueForCsv = null;
                                            if (dbDialect.isBlob(col.getTypeCode())) {
                                                byte[] blobBytes = lobHandler.getBlobAsBytes(rs,
                                                        col.getName());
                                                valueForCsv = dbDialect.encodeForCsv(blobBytes);
                                            } else {
                                                String clobText = lobHandler.getClobAsString(rs,
                                                        col.getName());
                                                valueForCsv = clobText;
                                            }
                                            int index = ArrayUtils.indexOf(columnNames,
                                                    col.getName());
                                            rowData[index] = valueForCsv;
                                        }
                                        return null;
                                    }
                                });
                                data.setRowData(CsvUtils.escapeCsvData(rowData));
                            } catch (IncorrectResultSizeDataAccessException ex) {
                                // Row could have been deleted by the time we
                                // get around to extracting
                                log.warn("DataExtractorRowMissingCannotGetLobData",
                                        data.getRowData());
                            } catch (Exception ex) {
                                throw new SymmetricException("DataExtractorTroubleExtractingLobData", ex, data.getRowData());
                            }

                        }
                    }
                }
            }
        }
    }

    protected Trigger findTrigger(TriggerHistory triggerHistory) {
        Trigger trigger = null;
        if (triggerHistory != null && triggerRouterService != null) {
            Map<String, List<TriggerRouter>> cache = triggerRouterService
                    .getTriggerRoutersForCurrentNode(false);
            if (cache != null) {
                List<TriggerRouter> trList = cache.get(triggerHistory.getTriggerId());
                if (trList != null && trList.size() > 0) {
                    trigger = trList.get(0).getTrigger();
                }
            }
        }
        return trigger;
    }

    protected String buildSelect(Table table, List<Column> lobColumns, Column[] pkColumns) {
        StringBuilder sql = new StringBuilder("select ");
        String quote = dbDialect.getIdentifierQuoteString();
        for (Column col : lobColumns) {
            sql.append(quote);
            sql.append(col.getName());
            sql.append(quote);
            sql.append(",");
        }
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" from ");
        sql.append(table.getFullyQualifiedTableName());
        sql.append(" where ");
        for (Column col : pkColumns) {
            sql.append(quote);
            sql.append(col.getName());
            sql.append(quote);
            sql.append("=? and ");
        }
        sql.delete(sql.length() - 5, sql.length());
        return sql.toString();
    }

    protected List<Column> getLobColumns(Table table) {
        List<Column> lobColumns = new ArrayList<Column>(1);
        Column[] allColumns = table.getColumns();
        for (Column column : allColumns) {
            if (dbDialect.isLob(column.getTypeCode())) {
                lobColumns.add(column);
            }
        }
        return lobColumns;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerRouterService) {
        this.triggerRouterService = triggerRouterService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}