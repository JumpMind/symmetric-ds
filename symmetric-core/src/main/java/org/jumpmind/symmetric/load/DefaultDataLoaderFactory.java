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

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.platform.IAlterDatabaseInterceptor;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.cassandra.CassandraPlatform;
import org.jumpmind.db.platform.kafka.KafkaPlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.DatabaseConstants;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.AbstractDatabaseWriter;
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
    protected Set<String> conflictLosingParentRows = new HashSet<String>();

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
        IDatabasePlatform targetPlatform = symmetricDialect.getTargetPlatform();
        try {
            if (targetPlatform instanceof CassandraPlatform) {
                // TODO: Evaluate if ConflictResolver will work for Cassandra and if so remove duplicate code.
                return new CassandraDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
                        symmetricDialect.getTablePrefix(), new DefaultTransformWriterConflictResolver(transformWriter),
                        buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));
            } else if (targetPlatform instanceof KafkaPlatform) {
//                if (filters == null) {
//                    filters = new ArrayList<IDatabaseWriterFilter>();
//                }
                String url;
                String producer;
                String externalNodeID;
                String outputFormat;
                String topicBy;
                String messageBy;
                String confluentUrl;
                String schemaPackage;
                String loadOnlyPrefix;
                TypedProperties props;
                String runtimeConfigTablePrefix;
                String channelReload;
                // filters.add(new KafkaWriterFilter(this.parameterService));
                producer = this.parameterService.getString(ParameterConstants.KAFKA_PRODUCER, "SymmetricDS");
                outputFormat = parameterService.getString(ParameterConstants.KAFKA_FORMAT, KafkaWriter.KAFKA_FORMAT_JSON);
                topicBy = parameterService.getString(ParameterConstants.KAFKA_TOPIC_BY, KafkaWriter.KAFKA_TOPIC_BY_CHANNEL);
                messageBy = parameterService.getString(ParameterConstants.KAFKA_MESSAGE_BY, KafkaWriter.KAFKA_MESSAGE_BY_BATCH);
                confluentUrl = parameterService.getString(ParameterConstants.KAFKA_CONFLUENT_REGISTRY_URL);
                schemaPackage = parameterService.getString(ParameterConstants.KAFKA_AVRO_JAVA_PACKAGE);
                externalNodeID = parameterService.getExternalId();
                loadOnlyPrefix = ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX;
                props = parameterService.getAllParameters();
                url = parameterService.getString(ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX + "db.url");
                runtimeConfigTablePrefix = parameterService.getString(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX);
                channelReload = Constants.CHANNEL_RELOAD;
                return new KafkaWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
                        symmetricDialect.getTablePrefix(), new DefaultTransformWriterConflictResolver(transformWriter),
                        buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData), producer, outputFormat, topicBy,
                        messageBy, confluentUrl, schemaPackage, externalNodeID, url, loadOnlyPrefix, props, runtimeConfigTablePrefix, channelReload);
            }
        } catch (Exception e) {
            log.warn("Failed to create writer for platform " + targetPlatform.getClass().getSimpleName(), e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
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
                        DynamicDefaultDatabaseWriter writer = transformWriter.getNestedWriterOfType(DynamicDefaultDatabaseWriter.class);
                        if (Boolean.TRUE.equals(writer.getContext().get(AbstractDatabaseWriter.TRANSACTION_ABORTED))) {
                            return;
                        }
                        if (conflict.getPingBack() == PingBack.SINGLE_ROW) {
                            ISqlTransaction transaction = writer.getTransaction();
                            if (transaction != null) {
                                symmetricDialect.disableSyncTriggers(transaction, sourceNodeId);
                            }
                        }
                        if (conflict.getResolveType() == ResolveConflict.NEWER_WINS &&
                                conflict.getDetectType() != DetectConflict.USE_TIMESTAMP &&
                                conflict.getDetectType() != DetectConflict.USE_VERSION) {
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
                     * When using new captured row wins, the winning row is saved to sym_data so other conflicts can see it. When two nodes are in conflict,
                     * they race to update the third node, but the first node will get no conflict, so we send a script back to all but winning node to ask if
                     * they have a newer row.
                     */
                    protected void handleWinnerForNewerCaptureWins(ISqlTransaction transaction, CsvData csvData) {
                        String tableName = csvData.getAttribute(CsvData.ATTRIBUTE_TABLE_NAME);
                        Timestamp loadingTs = csvData.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME);
                        List<TriggerHistory> hists = engine.getTriggerRouterService().getActiveTriggerHistories(tableName);
                        if (hists != null && hists.size() > 0 && loadingTs != null) {
                            TriggerHistory hist = hists.get(0);
                            Data data = new Data(hist.getSourceTableName(), csvData.getDataEventType(),
                                    csvData.getCsvData(CsvData.ROW_DATA), csvData.getCsvData(CsvData.PK_DATA), hist,
                                    csvData.getAttribute(CsvData.ATTRIBUTE_CHANNEL_ID), null, csvData.getAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID));
                            data.setOldData(csvData.getCsvData(CsvData.OLD_DATA));
                            data.setPreRouted(true);
                            data.setCreateTime(csvData.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME));
                            engine.getDataService().insertData(transaction, data);
                            String channelId = csvData.getAttribute(CsvData.ATTRIBUTE_CHANNEL_ID);
                            if (channelId != null && !channelId.equals(Constants.CHANNEL_RELOAD)) {
                                String pkCsvData = CsvUtils.escapeCsvData(getPkCsvData(csvData, hist));
                                String nodeTableName = TableConstants.getTableName(parameterService.getTablePrefix(), TableConstants.SYM_NODE);
                                List<TriggerHistory> nodeHists = engine.getTriggerRouterService().getActiveTriggerHistories(nodeTableName);
                                if (nodeHists != null && nodeHists.size() > 0 && pkCsvData != null) {
                                    String sourceNodeId = csvData.getAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID);
                                    long createTime = data.getCreateTime() != null ? data.getCreateTime().getTime() : 0;
                                    String script = "if (context != void && context != null && org.jumpmind.symmetric.Version.isOlderVersion(\"3.12.4\")) { " +
                                            "engine.getDataService().sendNewerDataToNode(context.findTransaction(), SOURCE_NODE_ID, \"" +
                                            tableName + "\", " + pkCsvData + ", new Date(" +
                                            createTime + "L), \"" + sourceNodeId + "\"); }";
                                    Data scriptData = new Data(nodeTableName, DataEventType.BSH,
                                            CsvUtils.escapeCsvData(script), null, nodeHists.get(0), Constants.CHANNEL_RELOAD, null, null);
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
                        if (pkCsvData != null) {
                            pkCsvData = pkCsvData.replace("\n", "\\n").replace("\r", "\\r");
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
        DatabaseWriterSettings settings = buildParameterDatabaseWriterSettings(conflictSettings);
        settings.setLoadOnlyNode(engine.getParameterService().is(ParameterConstants.NODE_LOAD_ONLY));
        settings.setDatabaseWriterFilters(filters);
        settings.setDatabaseWriterErrorHandlers(errorHandlers);
        settings.setResolvedData(resolvedDatas);
        settings.setConflictLosingParentRows(conflictLosingParentRows);
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
