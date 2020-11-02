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
package org.jumpmind.symmetric.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.platform.IAlterDatabaseInterceptor;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.DatabaseConstants;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.BigQueryDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.CassandraDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.PingBack;
import org.jumpmind.symmetric.io.data.writer.Conflict.ResolveConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DefaultTransformWriterConflictResolver;
import org.jumpmind.symmetric.io.data.writer.DynamicDefaultDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.KafkaWriter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory, IBuiltInExtensionPoint, ISymmetricEngineAware {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ISymmetricEngine engine;
    
    public DefaultDataLoaderFactory() {
    }

    public DefaultDataLoaderFactory(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
    }

    public String getTypeName() {
        return "default";
    }

    public IDataWriter getDataWriter(final String sourceNodeId, final ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, List<? extends Conflict> conflictSettings,
            List<ResolvedData> resolvedData) {

        if (symmetricDialect.getTargetPlatform().getClass().getSimpleName().equals("CassandraPlatform")) {
            try {
                // TODO: Evaluate if ConflictResolver will work for Cassandra and if so remove duplicate code.
                return new CassandraDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
                        symmetricDialect.getTablePrefix(), new DefaultTransformWriterConflictResolver(transformWriter),
                        buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));

            } catch (Exception e) {
                log.warn(
                        "Failed to create the cassandra database writer.  Check to see if all of the required jars have been added",
                        e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        if (symmetricDialect.getTargetPlatform().getClass().getSimpleName().equals("BigQueryPlatform")) {
            try {
                return new BigQueryDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
                        symmetricDialect.getTablePrefix(), new DefaultTransformWriterConflictResolver(transformWriter),
                        buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData), 
                        parameterService.getInt(ParameterConstants.GOOGLE_BIG_QUERY_MAX_ROWS_PER_RPC, 100));

            } catch (Exception e) {
                log.warn(
                        "Failed to create the big query database writer.",
                        e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
        
        if (symmetricDialect.getTargetPlatform().getClass().getSimpleName().equals("KafkaPlatform")) {
            try {
                if (filters == null) {
                    filters = new ArrayList<IDatabaseWriterFilter>();
                }
                filters.add(new KafkaWriterFilter(this.parameterService));

                return new KafkaWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
                        symmetricDialect.getTablePrefix(), new DefaultTransformWriterConflictResolver(transformWriter),
                        buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));

            } catch (Exception e) {
                log.warn("Failed to create the kafka writer.", e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        DynamicDefaultDatabaseWriter writer = new DynamicDefaultDatabaseWriter(symmetricDialect.getPlatform(),
                symmetricDialect.getTargetPlatform(), symmetricDialect.getTablePrefix(),
                new DefaultTransformWriterConflictResolver(transformWriter) {
                    @Override
                    protected void beforeResolutionAttempt(CsvData csvData, Conflict conflict) {
                        if (conflict.getPingBack() != PingBack.OFF) {
                            DynamicDefaultDatabaseWriter writer = transformWriter
                                    .getNestedWriterOfType(DynamicDefaultDatabaseWriter.class);
                            ISqlTransaction transaction = writer.getTransaction();
                            if (transaction != null) {
                                symmetricDialect.enableSyncTriggers(transaction);
                            }
                        }
                    }

                    @Override
                    protected void afterResolutionAttempt(CsvData csvData, Conflict conflict) {
                        if (conflict.getPingBack() == PingBack.SINGLE_ROW) {
                            DynamicDefaultDatabaseWriter writer = transformWriter.getNestedWriterOfType(DynamicDefaultDatabaseWriter.class);
                            ISqlTransaction transaction = writer.getTransaction();
                            if (transaction != null) {
                                symmetricDialect.disableSyncTriggers(transaction, sourceNodeId);
                            }
                        }
                        if (conflict.getResolveType() == ResolveConflict.NEWER_WINS &&
                                conflict.getDetectType() != DetectConflict.USE_TIMESTAMP &&
                                conflict.getDetectType() != DetectConflict.USE_VERSION) {
                            DynamicDefaultDatabaseWriter writer = transformWriter.getNestedWriterOfType(DynamicDefaultDatabaseWriter.class);
                            Boolean isWinner = (Boolean) writer.getContext().get(DatabaseConstants.IS_CONFLICT_WINNER);
                            if (isWinner != null && isWinner == true) {
                                writer.getContext().remove(DatabaseConstants.IS_CONFLICT_WINNER);
                                ISqlTransaction transaction = writer.getTransaction();
                                if (transaction != null) {
                                    handleWinnerForNewerCaptureWins(transaction, csvData);
                                }
                            }        
                        }
                    }
                    
                    /**
                     * When using new captured row wins, the winning row is saved to sym_data so other conflicts can see it.
                     * When two nodes are in conflict, they race to update the third node, but the first node will get no conflict,
                     * so we send a script back to all but winning node to ask if they have a newer row. 
                     */
                    protected void handleWinnerForNewerCaptureWins(ISqlTransaction transaction, CsvData csvData) {
                        String tableName = csvData.getAttribute(CsvData.ATTRIBUTE_TABLE_NAME);
                        List<TriggerHistory> hists = engine.getTriggerRouterService().getActiveTriggerHistories(tableName);
                        if (hists != null && hists.size() > 0) {
                            TriggerHistory hist = hists.get(0);
                            Data data = new Data(tableName, csvData.getDataEventType(),
                                    csvData.getCsvData(CsvData.ROW_DATA), csvData.getCsvData(CsvData.PK_DATA), hist, 
                                    csvData.getAttribute(CsvData.ATTRIBUTE_CHANNEL_ID), null, csvData.getAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID));
                            data.setTableName(tableName);
                            data.setOldData(csvData.getCsvData(CsvData.OLD_DATA));
                            data.setPreRouted(true);
                            data.setCreateTime(csvData.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME));
                            engine.getDataService().insertData(transaction, data);
    
                            String channelId = csvData.getAttribute(CsvData.ATTRIBUTE_CHANNEL_ID);
                            if (channelId != null && !channelId.equals(Constants.CHANNEL_RELOAD)) {
                                String pkCsvData = CsvUtils.escapeCsvData(getPkCsvData(csvData, hist));
                                if (pkCsvData != null) {
                                    String sourceNodeId = csvData.getAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID);
                                    long createTime = data.getCreateTime() != null ? data.getCreateTime().getTime() : 0;
                                    String script = "if (context != void && context != null) { " +
                                        "engine.getDataService().sendNewerDataToNode(context.findTransaction(), SOURCE_NODE_ID, \"" +
                                        tableName + "\", " + pkCsvData + ", new Date(" +
                                        createTime +"L), \"" + sourceNodeId + "\"); }";
                                    Data scriptData = new Data(tableName, DataEventType.BSH,
                                            CsvUtils.escapeCsvData(script), null, hist, Constants.CHANNEL_RELOAD, null, null);
                                    scriptData.setSourceNodeId(sourceNodeId);
                                    engine.getDataService().insertData(transaction, scriptData);
                                }
                            }
                        }
                    }
                    
                    protected String getPkCsvData(CsvData csvData, TriggerHistory hist) {
                        String pkCsvData = csvData.getCsvData(CsvData.PK_DATA);
                        if (pkCsvData == null) {
                            if (hist.getParsedPkColumnNames() != null && hist.getParsedPkColumnNames().length > 0) {
                                String[] pkData = new String[hist.getParsedPkColumnNames().length];
                                Map<String, String> values = csvData.toColumnNameValuePairs(hist.getParsedPkColumnNames(), CsvData.ROW_DATA);
                                int i = 0;
                                for (String name : hist.getParsedPkColumnNames()) {
                                    pkData[i++] = values.get(name);
                                }
                                pkCsvData = CsvUtils.escapeCsvData(pkData);
                            } else {
                                pkCsvData = csvData.getCsvData(CsvData.ROW_DATA);
                            }
                        }
                        return pkCsvData;
                    }
                }, buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));

        return writer;
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings(List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, List<? extends Conflict> conflictSettings,
            List<ResolvedData> resolvedDatas) {
        DatabaseWriterSettings settings = buildParameterDatabaseWritterSettings();
        settings.setDatabaseWriterFilters(filters);
        settings.setDatabaseWriterErrorHandlers(errorHandlers);
        
        
        Map<String, Conflict> byChannel = new HashMap<String, Conflict>();
        Map<String, Conflict> byTable = new HashMap<String, Conflict>();
        boolean multipleDefaultSettingsFound = false;
        if (conflictSettings != null) {
            for (Conflict conflictSetting : conflictSettings) {
                String qualifiedTableName = conflictSetting.toQualifiedTableName();
                if (StringUtils.isNotBlank(qualifiedTableName)) {
                    byTable.put(qualifiedTableName, conflictSetting);
                } else if (StringUtils.isNotBlank(conflictSetting.getTargetChannelId())) {
                    byChannel.put(conflictSetting.getTargetChannelId(), conflictSetting);
                } else {
                    if (settings.getDefaultConflictSetting() != null) {
                        multipleDefaultSettingsFound = true;
                    }
                    settings.setDefaultConflictSetting(conflictSetting);
                }
            }
        }

        if (multipleDefaultSettingsFound) {
            log.warn("There were multiple default conflict settings found.  Using '{}' as the default",
                    settings.getDefaultConflictSetting().getConflictId());
        }
        settings.setConflictSettingsByChannel(byChannel);
        settings.setConflictSettingsByTable(byTable);
        settings.setResolvedData(resolvedDatas);
        
        List<IAlterDatabaseInterceptor> alterDatabaseInterceptors = engine.getExtensionService()
                .getExtensionPointList(IAlterDatabaseInterceptor.class);
        IAlterDatabaseInterceptor[] interceptors = alterDatabaseInterceptors
                .toArray(new IAlterDatabaseInterceptor[alterDatabaseInterceptors.size()]);
        settings.setAlterDatabaseInterceptors(interceptors);
        
        return settings;
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
    }
    
}
