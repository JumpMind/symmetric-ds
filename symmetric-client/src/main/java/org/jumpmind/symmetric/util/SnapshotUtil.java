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
package org.jumpmind.symmetric.util;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.db.model.CatalogSchema;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.db.firebird.FirebirdSymmetricDialect;
import org.jumpmind.symmetric.db.mysql.MySqlSymmetricDialect;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.symmetric.job.IJob;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.UpdateService;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.SymRollingFileAppender;
import org.jumpmind.util.ZipBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@IgnoreJRERequirement
public class SnapshotUtil {

    protected static final Logger log = LoggerFactory.getLogger(SnapshotUtil.class);

    protected static final int THREAD_INDENT_SPACE = 50;

    public static File getSnapshotDirectory(ISymmetricEngine engine) {
        File snapshotsDir = new File(engine.getParameterService().getTempDirectory(), "snapshots");
        snapshotsDir.mkdirs();
        return snapshotsDir;
    }

    public static File createSnapshot(ISymmetricEngine engine) {

        String dirName = engine.getEngineName().replaceAll(" ", "-") + "-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        IParameterService parameterService = engine.getParameterService();
        File tmpDir = new File(parameterService.getTempDirectory(), dirName);
        tmpDir.mkdirs();
        log.info("Creating snapshot file in " + tmpDir.getAbsolutePath());

        try(FileWriter fwriter = new FileWriter(new File(tmpDir, "config-export.csv"))) {
            engine.getDataExtractorService().extractConfigurationStandalone(engine.getNodeService().findIdentity(),
                    fwriter, TableConstants.SYM_NODE, TableConstants.SYM_NODE_SECURITY,
                    TableConstants.SYM_NODE_IDENTITY, TableConstants.SYM_NODE_HOST,
                    TableConstants.SYM_NODE_CHANNEL_CTL,TableConstants.SYM_CONSOLE_ROLE,
                    TableConstants.SYM_CONSOLE_USER,TableConstants.SYM_CONSOLE_ROLE_PRIVILEGE,
                    TableConstants.SYM_MONITOR_EVENT, TableConstants.SYM_CONSOLE_EVENT,
                    TableConstants.SYM_CONSOLE_USER_HIST);
        } catch (Exception e) {
            log.warn("Failed to export symmetric configuration", e);
        }

        File serviceConfFile = new File("conf/sym_service.conf");
        try {
            if (serviceConfFile.exists()) {
                FileUtils.copyFileToDirectory(serviceConfFile, tmpDir);
            }
        } catch (Exception e) {
            log.warn("Failed to copy " + serviceConfFile.getName() + " to the snapshot directory", e);
        }

        FileOutputStream fos = null;
        try {
            HashMap<CatalogSchema, List<Table>> catalogSchemas = new HashMap<CatalogSchema, List<Table>>();
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
            List<TriggerHistory> triggerHistories = triggerRouterService.getActiveTriggerHistories();
            for (TriggerHistory triggerHistory : triggerHistories) {
                Table table = engine.getDatabasePlatform().getTableFromCache(triggerHistory.getSourceCatalogName(),
                        triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(), false);
                if (table != null && !table.getName().toUpperCase().startsWith(engine.getSymmetricDialect().getTablePrefix().toUpperCase())) {
                    addTableToMap(catalogSchemas, new CatalogSchema(table.getCatalog(), table.getSchema()), table);
                }
            }

            List<String> catalogNames = engine.getDatabasePlatform().getDdlReader().getCatalogNames();
            List<Trigger> triggers = triggerRouterService.getTriggers();
            for (Trigger trigger : triggers) {
                if (StringUtils.isBlank(trigger.getSourceCatalogName()) || catalogNames.contains(trigger.getSourceCatalogName())) {
                    Table table = engine.getDatabasePlatform().getTableFromCache(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName(), false);
                    if (table != null) {
                        addTableToMap(catalogSchemas, new CatalogSchema(table.getCatalog(), table.getSchema()), table);
                    }
                }
            }

            for (CatalogSchema catalogSchema : catalogSchemas.keySet()) {
                DbExport export = new DbExport(engine.getDatabasePlatform());
                boolean isDefaultCatalog = StringUtils.equalsIgnoreCase(catalogSchema.getCatalog(), engine.getDatabasePlatform().getDefaultCatalog());
                boolean isDefaultSchema = StringUtils.equalsIgnoreCase(catalogSchema.getSchema(), engine.getDatabasePlatform().getDefaultSchema());

                try {
                    if (isDefaultCatalog && isDefaultSchema) {
                        fos = new FileOutputStream(new File(tmpDir, "table-definitions.xml"));
                    } else {
                        String extra = "";
                        if (!isDefaultCatalog && catalogSchema.getCatalog() != null) {
                            extra += catalogSchema.getCatalog();
                            export.setCatalog(catalogSchema.getCatalog());
                        }
                        if (!isDefaultSchema && catalogSchema.getSchema() != null) {
                            if (!extra.equals("")) {
                                extra += "-";
                            }
                            extra += catalogSchema.getSchema();
                            export.setSchema(catalogSchema.getSchema());
                        }
                        fos = new FileOutputStream(new File(tmpDir, "table-definitions-" + extra + ".xml"));
                    }
                 
                    List<Table> tables = catalogSchemas.get(catalogSchema);
                    export.setFormat(Format.XML);
                    export.setNoData(true);
                    export.exportTables(fos, tables.toArray(new Table[tables.size()]));
                } finally {
                    if(fos != null) {
                        try {
                            fos.close();
                        } catch(IOException e) { }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            if(fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }

        String tablePrefix = engine.getTablePrefix();

        DbExport export = new DbExport(engine.getDatabasePlatform());
        export.setFormat(Format.CSV);
        export.setNoCreateInfo(true);

        extract(export, new File(tmpDir, "sym_identity.csv"), TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_IDENTITY));

        extract(export, new File(tmpDir, "sym_node.csv"), TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE));

        extract(export, new File(tmpDir, "sym_node_security.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_SECURITY));

        extract(export, new File(tmpDir, "sym_node_host.csv"),  
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_HOST));
        
        extract(export, new File(tmpDir, "sym_trigger_hist.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_TRIGGER_HIST));
        
        extract(export, new File(tmpDir, "sym_node_channel_ctl.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_CHANNEL_CTL));

        try {
            if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
                engine.getNodeCommunicationService().persistToTableForSnapshot();
                engine.getClusterService().persistToTableForSnapshot();
            }
        } catch (Exception e) {
            log.warn("Unable to add SYM_NODE_COMMUNICATION to the snapshot.", e);
        }

        extract(export, new File(tmpDir, "sym_lock.csv"), TableConstants.getTableName(tablePrefix, TableConstants.SYM_LOCK));

        extract(export, new File(tmpDir, "sym_node_communication.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_COMMUNICATION));

        extract(export, 50000, "where status = 'OK' order by batch_id desc", new File(tmpDir, "sym_outgoing_batch_ok.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_OUTGOING_BATCH));

        extract(export, 10000, "where status != 'OK' order by batch_id desc", new File(tmpDir, "sym_outgoing_batch_not_ok.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_OUTGOING_BATCH));

        extract(export, 10000, "where status = 'OK' order by create_time desc", new File(tmpDir, "sym_incoming_batch_ok.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_INCOMING_BATCH));

        extract(export, 10000, "where status != 'OK' order by create_time", new File(tmpDir, "sym_incoming_batch_not_ok.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_INCOMING_BATCH));

        extract(export, 10000, "order by start_id, end_id desc", new File(tmpDir, "sym_data_gap.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_DATA_GAP));

        try {
            outputSymDataForBatchesInError(engine, tmpDir);
        } catch (Exception e) {
            log.warn("Failed to export data from batch in error", e);
        }
        
        extract(export, new File(tmpDir, "sym_table_reload_request.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_TABLE_RELOAD_REQUEST));

        extract(export, new File(tmpDir, "sym_table_reload_status.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_TABLE_RELOAD_STATUS));

        extract(export, 5000, "order by relative_dir, file_name", new File(tmpDir, "sym_file_snapshot.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));

        export.setIgnoreMissingTables(true);
        extract(export, new File(tmpDir, "sym_console_event.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_CONSOLE_EVENT));

        extract(export, new File(tmpDir, "sym_monitor_event.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_MONITOR_EVENT));

        extract(export, new File(tmpDir, "sym_extract_request.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_EXTRACT_REQUEST));

        extract(export, new File(tmpDir, "sym_context.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_CONTEXT));

        extract(export, new File(tmpDir, "sym_node_host_channel_stats.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_HOST_CHANNEL_STATS));

        if (engine.getSymmetricDialect() instanceof FirebirdSymmetricDialect) {
            final String[] monTables = { "mon$database", "mon$attachments", "mon$transactions", "mon$statements", "mon$io_stats",
                    "mon$record_stats", "mon$memory_usage", "mon$call_stack", "mon$context_variables" };
            for (String table : monTables) {
                extract(export, new File(tmpDir, "firebird-" + table + ".csv"), table);
            }
        }
        
        if (engine.getSymmetricDialect() instanceof MySqlSymmetricDialect) {
            extractQuery(engine.getSqlTemplate(), tmpDir + File.separator + "mysql-processlist.csv",
                    "show processlist");
            extractQuery(engine.getSqlTemplate(), tmpDir + File.separator + "mysql-global-variables.csv",
                    "show global variables");
            extractQuery(engine.getSqlTemplate(), tmpDir + File.separator + "mysql-session-variables.csv",
                    "show session variables");
        }

        if (!engine.getParameterService().is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
            try {
                List<DataGap> gaps = engine.getRouterService().getDataGaps();
                SimpleDateFormat dformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                fos = new FileOutputStream(new File(tmpDir, "sym_data_gap_cache.csv"));
                fos.write("start_id,end_id,create_time\n".getBytes());
                if (gaps != null) {
                    for (DataGap gap : gaps) {
                        fos.write((gap.getStartId() + "," + gap.getEndId() + ",\"" + dformat.format(gap.getCreateTime()) + "\",\"" + "\"\n").getBytes());
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to export data gap information", e);
            } finally {
                if(fos != null) {
                    try {
                        fos.close();
                    } catch(IOException e) { }
                }
            }            
        }

        createThreadsFile(tmpDir.getPath(), false);
        createThreadsFile(tmpDir.getPath(), true);

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "parameters.properties"));
            Properties effectiveParameters = engine.getParameterService().getAllParameters();
            SortedProperties parameters = new SortedProperties();
            parameters.putAll(effectiveParameters);
            parameters.remove("db.password");
            parameters.store(fos, "parameters.properties");
        } catch (IOException e) {
            log.warn("Failed to export parameter information", e);
        } finally {
            if(fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "parameters-changed.properties"));
            Properties defaultParameters = new Properties();
            InputStream in = SnapshotUtil.class.getResourceAsStream("/symmetric-default.properties");
            defaultParameters.load(in);
            if(in != null) {
                try {
                    in.close();
                } catch(IOException e) { }
            }
            in = SnapshotUtil.class.getResourceAsStream("/symmetric-console-default.properties");
            if (in != null) {
                defaultParameters.load(in);
                try {
                    in.close();
                } catch(IOException e) { }
            }
            Properties effectiveParameters = engine.getParameterService().getAllParameters();
            Properties changedParameters = new SortedProperties();
            Map<String, ParameterMetaData> parameters = ParameterConstants.getParameterMetaData();
            for (String key : parameters.keySet()) {
                String defaultValue = defaultParameters.getProperty((String) key);
                String currentValue = effectiveParameters.getProperty((String) key);
                if (defaultValue == null && currentValue != null || (defaultValue != null && !defaultValue.equals(currentValue))) {
                    changedParameters.put(key, currentValue == null ? "" : currentValue);
                }
            }
            changedParameters.remove("db.password");
            changedParameters.store(fos, "parameters-changed.properties");
        } catch (Exception e) {
            log.warn("Failed to export parameters-changed information", e);
        } finally {
            if(fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }

        writeRuntimeStats(engine, tmpDir);
        writeJobsStats(engine, tmpDir);

        if ("true".equals(System.getProperty(SystemConstants.SYSPROP_STANDALONE_WEB))) {
            writeDirectoryListing(engine, tmpDir);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "system.properties"));
            SortedProperties props = new SortedProperties();
            props.putAll(System.getProperties());
            props.store(fos, "system.properties");
        } catch (Exception e) {
            log.warn("Failed to export thread information", e);
        } finally {
            if(fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }

        File logDir = null;

        String parameterizedLogDir = parameterService.getString("server.log.dir");
        if (isNotBlank(parameterizedLogDir)) {
            logDir = new File(parameterizedLogDir);
        }

        if (logDir != null && logDir.exists()) {
            log.info("Using server.log.dir setting as the location of the log files");
        } else {
            logDir = new File("logs");
        }

        Map<File, Layout<?>> matches = null;
        if (!logDir.exists()) {
            matches = findSymmetricLogFile();
            if (matches != null && matches.size() == 1) {
                logDir = matches.keySet().iterator().next().getParentFile();
            }
        }

        if (!logDir.exists()) {
            logDir = new File("../logs");
        }

        if (!logDir.exists()) {
            logDir = new File("target");
        }

        if (logDir.exists()) {
            log.info("Copying log files into snapshot file");
            File[] files = logDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    String lowerCaseFileName = file.getName().toLowerCase();
                    if (
                            (lowerCaseFileName.contains(".log")
                                    && (lowerCaseFileName.contains("symmetric") || lowerCaseFileName.contains("wrapper"))
                            )
                            ||
                            compareLogFileName(lowerCaseFileName, matches)) {
                        try {
                            FileUtils.copyFileToDirectory(file, tmpDir);
                        } catch (IOException e) {
                            log.warn("Failed to copy " + file.getName() + " to the snapshot directory", e);
                        }
                    }
                }
            }
        }

        File jarFile = null;
        try {
            String filename = tmpDir.getName() + ".zip";
            if (parameterService.is(ParameterConstants.SNAPSHOT_FILE_INCLUDE_HOSTNAME))
                filename = AppUtils.getHostName() + "_" + filename;
            jarFile = new File(getSnapshotDirectory(engine), filename); 
            ZipBuilder builder = new ZipBuilder(tmpDir, jarFile, new File[] { tmpDir });
            builder.build();
            FileUtils.deleteDirectory(tmpDir);
        } catch (Exception e) {
            throw new IoException("Failed to package snapshot files into archive", e);
        }
        
        log.info("Done creating snapshot file");
        return jarFile;
    }
    
    private static boolean compareLogFileName(String fileName, Map<File, Layout<?>> matches) {
        boolean ret = false;
        if(fileName != null && fileName.length() > 0 && matches != null && matches.size() > 0) {
            for(File f : matches.keySet()) {
                if(fileName.toLowerCase().contains(f.getName().toLowerCase())) {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    protected static void extract(DbExport export, File file, String... tables) {
        extract(export, Integer.MAX_VALUE, null, file, tables);
    }

    protected static void extract(DbExport export, int maxRows, String whereClause, File file, String... tables) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            export.setMaxRows(maxRows);
            export.setWhereClause(whereClause);
            export.exportTables(fos, tables);
        } catch (Exception e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }
    }

    protected static void extractQuery(ISqlTemplate sqlTemplate, String fileName, String sql) {
        CsvWriter writer = null;
        try {
            List<Row> rows = sqlTemplate.query(sql);
            writer = new CsvWriter(fileName);
            boolean isFirstRow = true;
            for (Row row : rows) {
                if (isFirstRow) {
                    for (String key : row.keySet()) {
                        writer.write(key);
                    }
                    writer.endRecord();
                    isFirstRow = false;
                }
                for (String key : row.keySet()) {
                    writer.write(row.getString(key));
                }
                writer.endRecord();
            }
        } catch (Exception e) {
            log.warn("Failed to run extract query " + sql, e);
        } finally {
            if(writer != null) {
                writer.close();
            }
        }
    }

    protected static void writeDirectoryListing(ISymmetricEngine engine, File tmpDir) {
        try {
            File home = new File(System.getProperty("user.dir"));
            if (home.getName().equalsIgnoreCase("bin")) {
                home = home.getParentFile();
            }

            StringBuilder output = new StringBuilder();
            Comparator<File> fileComparator = new Comparator<File>() {
                @Override
                public int compare(File o1, File o2) {
                    return o1.getPath().compareToIgnoreCase(o2.getPath());
                }
            };
            printDirectoryContents(home, output, fileComparator);
            FileUtils.write(new File(tmpDir, "directory-listing.txt"), output, Charset.defaultCharset(), false);
        } catch (Exception ex) {
            log.warn("Failed to output the direcetory listing", ex);
        }
    }

    protected static void printDirectoryContents(File dir, StringBuilder output, Comparator<File> fileComparator) throws IOException {
        output.append("\n");
        output.append(dir.getCanonicalPath());
        output.append("\n");

        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.parallelSort(files, fileComparator);
            for (File file : files) {
                output.append("  ");
                output.append(file.canRead() ? "r" : "-");
                output.append(file.canWrite() ? "w" : "-");
                output.append(file.canExecute() ? "x" : "-");
                output.append(StringUtils.leftPad(file.length() + "", 11));
                output.append(" ");
                output.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(file.lastModified())));
                output.append(" ");
                output.append(file.getName());
                output.append("\n");
            }
    
            for (File file : files) {
                if (file.isDirectory()) {
                    printDirectoryContents(file, output, fileComparator);
                }
            }
        }

    }

    protected static void writeRuntimeStats(ISymmetricEngine engine, File tmpDir) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-stats.properties"));
            Properties runtimeProperties = new Properties() {
                private static final long serialVersionUID = 1L;

                public synchronized Enumeration<Object> keys() {
                    return Collections.enumeration(new TreeSet<Object>(super.keySet()));
                }
            };

            DataSource dataSource = engine.getDatabasePlatform().getDataSource();
            if (dataSource instanceof BasicDataSource) {
                BasicDataSource dbcp = (BasicDataSource) dataSource;
                runtimeProperties.setProperty("connections.idle", String.valueOf(dbcp.getNumIdle()));
                runtimeProperties.setProperty("connections.used", String.valueOf(dbcp.getNumActive()));
                runtimeProperties.setProperty("connections.max", String.valueOf(dbcp.getMaxActive()));
            }

            Runtime rt = Runtime.getRuntime();
            runtimeProperties.setProperty("memory.free", String.valueOf(rt.freeMemory()));
            runtimeProperties.setProperty("memory.used", String.valueOf(rt.totalMemory() - rt.freeMemory()));
            runtimeProperties.setProperty("memory.max", String.valueOf(rt.maxMemory()));

            List<MemoryPoolMXBean> memoryPools = new ArrayList<MemoryPoolMXBean>(ManagementFactory.getMemoryPoolMXBeans());
            long usedHeapMemory = 0;
            for (MemoryPoolMXBean memoryPool : memoryPools) {
                if (memoryPool.getType().equals(MemoryType.HEAP)) {
                    MemoryUsage memoryUsage = memoryPool.getCollectionUsage();
                    runtimeProperties.setProperty("memory.heap." + memoryPool.getName().toLowerCase().replaceAll(" ", "."),
                            Long.toString(memoryUsage.getUsed()));
                    usedHeapMemory += memoryUsage.getUsed();
                }
            }
            runtimeProperties.setProperty("memory.heap.total", Long.toString(usedHeapMemory));

            OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            runtimeProperties.setProperty("os.name", System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ")");
            runtimeProperties.setProperty("os.processors", String.valueOf(osBean.getAvailableProcessors()));
            runtimeProperties.setProperty("os.load.average", String.valueOf(osBean.getSystemLoadAverage()));

            runtimeProperties.setProperty("engine.is.started", Boolean.toString(engine.isStarted()));
            runtimeProperties.setProperty("engine.last.restart", engine.getLastRestartTime() != null ? 
                    engine.getLastRestartTime().toString() : "");

            runtimeProperties.setProperty("time.server", new Date().toString());
            runtimeProperties.setProperty("time.database", new Date(engine.getSymmetricDialect().getDatabaseTime()).toString());
            runtimeProperties.setProperty("batch.unrouted.data.count", Long.toString(engine.getRouterService().getUnroutedDataCount()));
            runtimeProperties.setProperty("batch.outgoing.errors.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesInError()));
            runtimeProperties.setProperty("batch.outgoing.tosend.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesUnsent()));
            runtimeProperties.setProperty("batch.incoming.errors.count",
                    Long.toString(engine.getIncomingBatchService().countIncomingBatchesInError()));

            List<DataGap> gaps = engine.getDataService().findDataGapsUnchecked();
            runtimeProperties.setProperty("data.gap.count", Long.toString(gaps.size()));
            if (gaps.size() > 0) {
                runtimeProperties.setProperty("data.gap.start.id", Long.toString(gaps.get(0).getStartId()));
                runtimeProperties.setProperty("data.gap.end.id", Long.toString(gaps.get(gaps.size() - 1).getEndId()));
            }

            runtimeProperties.setProperty("data.id.min", Long.toString(engine.getDataService().findMinDataId()));
            runtimeProperties.setProperty("data.id.max", Long.toString(engine.getDataService().findMaxDataId()));

            String jvmTitle = Runtime.class.getPackage().getImplementationTitle();
            runtimeProperties.put("jvm.title", jvmTitle != null ? jvmTitle : "Unknown");
            String jvmVendor = Runtime.class.getPackage().getImplementationVendor();
            runtimeProperties.put("jvm.vendor", jvmVendor != null ? jvmVendor : "Unknown");
            String jvmVersion = Runtime.class.getPackage().getImplementationVersion();
            runtimeProperties.put("jvm.version", jvmVersion != null ? jvmVersion : "Unknown");
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> arguments = runtimeMxBean.getInputArguments();
            runtimeProperties.setProperty("jvm.arguments", arguments.toString());
            runtimeProperties.setProperty("hostname", AppUtils.getHostName());
            runtimeProperties.setProperty("instance.id", engine.getClusterService().getInstanceId());
            runtimeProperties.setProperty("server.id", engine.getClusterService().getServerId());

            try {
                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName oName = new ObjectName("java.lang:type=OperatingSystem");
                runtimeProperties.setProperty("file.descriptor.open.count", mbeanServer.getAttribute(oName, "OpenFileDescriptorCount").toString());
                runtimeProperties.setProperty("file.descriptor.max.count", mbeanServer.getAttribute(oName, "MaxFileDescriptorCount").toString());
            } catch (Exception e) {
            }
            
            runtimeProperties.store(fos, "runtime-stats.properties");
        } catch (Exception e) {
            log.warn("Failed to export runtime-stats information", e);
        } finally {
            if(fos != null) {
                try {
                    fos.close();
                } catch(IOException e) { }
            }
        }
    }

    protected static void writeJobsStats(ISymmetricEngine engine, File tmpDir) {
        try(FileWriter writer = new FileWriter(new File(tmpDir, "jobs.txt"))) {
            IJobManager jobManager = engine.getJobManager();
            IClusterService clusterService = engine.getClusterService();
            INodeService nodeService = engine.getNodeService();
            writer.write("Clustering is " + (clusterService.isClusteringEnabled() ? "" : "not ") + "enabled and there are "
                    + nodeService.findNodeHosts(nodeService.findIdentityNodeId()).size() + " instances in the cluster\n\n");
            writer.write(StringUtils.rightPad("Job Name", 30) + StringUtils.rightPad("Schedule", 20) + StringUtils.rightPad("Status", 10)
                    + StringUtils.rightPad("Server Id", 30) + StringUtils.rightPad("Last Server Id", 30)
                    + StringUtils.rightPad("Last Finish Time", 30) + StringUtils.rightPad("Last Run Period", 20)
                    + StringUtils.rightPad("Avg. Run Period", 20) + "\n");
            List<IJob> jobs = jobManager.getJobs();
            Map<String, Lock> locks = clusterService.findLocks();
            for (IJob job : jobs) {
                Lock lock = locks.get(job.getName());
                String status = getJobStatus(job, lock);
                String runningServerId = lock != null ? lock.getLockingServerId() : "";
                String lastServerId = clusterService.getServerId();
                if (lock != null) {
                    lastServerId = lock.getLastLockingServerId();
                }
                
                String schedule = job.getSchedule();

                String lastFinishTime = getLastFinishTime(job, lock);
    
                writer.write(StringUtils.rightPad(job.getName().replace("_", " "), 30)+ 
                        StringUtils.rightPad(schedule, 20) + StringUtils.rightPad(status, 10) + 
                        StringUtils.rightPad(runningServerId == null ? "" : runningServerId, 30) +
                        StringUtils.rightPad(lastServerId == null ? "" : lastServerId, 30) + 
                        StringUtils.rightPad(lastFinishTime == null ? "" : lastFinishTime, 30) + 
                        StringUtils.rightPad(job.getLastExecutionTimeInMs() + "", 20) + 
                        StringUtils.rightPad(job.getAverageExecutionTimeInMs() + "", 20) + "\n");
            }
        } catch (Exception e) {
            log.warn("Failed to write jobs information", e);
        }
    }

    protected static String getJobStatus(IJob job, Lock lock) {
        String status = "IDLE";
        if (lock != null) {
            if (lock.isStopped()) {
                status = "STOPPED";
            } else if (lock.getLockTime() != null) {
                status = "RUNNING";
            }
        } else {
            status = job.isRunning() ? "RUNNING" : job.isPaused() ? "PAUSED" : job.isStarted() ? "IDLE" : "STOPPED";
        }
        return status;
    }

    protected static String getLastFinishTime(IJob job, Lock lock) {
        if (lock != null && lock.getLastLockTime() != null) {
            return DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(lock.getLastLockTime());
        } else {
            return job.getLastFinishTime() == null ? null : DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(job.getLastFinishTime());
        }
    }

    public static Map<File, Layout<?>> findSymmetricLogFile() {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        Map<String, Appender> appenderMap = lc.getRootLogger().getAppenders();
        for (Appender appender : appenderMap.values()) {
            String fileName = null;
            Layout<?> layout = null;
            if (appender instanceof FileAppender) {
                fileName = ((FileAppender) appender).getFileName();
                layout = ((FileAppender) appender).getLayout();
            } else if (appender instanceof RollingFileAppender) {
                fileName = ((RollingFileAppender) appender).getFileName();
                layout = ((RollingFileAppender) appender).getLayout();
            } else if (appender instanceof SymRollingFileAppender) {
                fileName = ((SymRollingFileAppender) appender).getFileName();
                layout = ((SymRollingFileAppender) appender).getLayout();
            }
            if (fileName != null) {
                File file = new File(fileName);
                if (file.exists()) {
                    Map<File, Layout<?>> matches = new HashMap<File, Layout<?>>();
                    matches.put(file, layout);
                    return matches;
                }
            }
        }
        return null;
    }

    public static File createThreadsFile(String parent, boolean isFiltered) {
        File file = new File(parent, isFiltered ? "threads-filtered.txt" : "threads.txt");
        try(FileWriter fwriter = new FileWriter(file)) {
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadBean.getAllThreadIds();
            for (long l : threadIds) {
                ThreadInfo info = threadBean.getThreadInfo(l, 100);
                if (info != null) {
                    String threadName = info.getThreadName();

                    boolean skip = isFiltered;
                    if (isFiltered) {
                        for (StackTraceElement element : info.getStackTrace()) {
                            String name = element.getClassName();
                            if (name.startsWith("com.jumpmind.") || name.startsWith("org.jumpmind.")) {
                                skip = false;
                            }
                            if (name.equals(SnapshotUtil.class.getName()) || name.startsWith(UpdateService.class.getName())) {
                                skip = true;
                                break;
                            }
                        }
                    }

                    if (!skip) {
                        fwriter.append(StringUtils.rightPad(threadName, THREAD_INDENT_SPACE));
                        fwriter.append(AppUtils.formatStackTrace(info.getStackTrace(), THREAD_INDENT_SPACE, false));
                        fwriter.append("\n");
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to export thread information", e);
        }
        return file;
    }

    private static void addTableToMap(HashMap<CatalogSchema, List<Table>> catalogSchemas, CatalogSchema catalogSchema, Table table) {
        List<Table> tables = catalogSchemas.get(catalogSchema);
        if (tables == null) {
            tables = new ArrayList<Table>();
            catalogSchemas.put(catalogSchema, tables);
        }
        tables.add(table);
    }

    static class SortedProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Enumeration<Object> keys() {
            return Collections.enumeration(new TreeSet<Object>(super.keySet()));
        }
    };
    
    public static void outputSymDataForBatchesInError(ISymmetricEngine engine, File tmpDir) {
        String tablePrefix = engine.getTablePrefix();
        DbExport export = new DbExport(engine.getDatabasePlatform());
        export.setFormat(Format.CSV_DQUOTE);
        export.setNoCreateInfo(true);
        
        // Create files for each batch in error
        for (OutgoingBatch batch : engine.getOutgoingBatchService().getOutgoingBatchErrors(10000).getBatches()) {
            if (batch.getFailedDataId() > 0) {
                Data data = engine.getDataService().findData(batch.getFailedDataId());
                if (data != null) {
                    // Write sym_data to file
                    String filenameCaptured = batch.getBatchId() + "_captured.csv";
                    String whereClause = "where data_id = " + data.getDataId();
                    extract(export, 10000, whereClause, new File(tmpDir, filenameCaptured), 
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_DATA));                    
                    
                    // Write parsed row data to file
                    String filenameParsed = tmpDir + File.separator + batch.getBatchId() + "_parsed.csv";
                    CsvWriter writer = null;
                    try {
                        writer = new CsvWriter(filenameParsed);
                        writer.writeRecord(data.getTriggerHistory().getParsedColumnNames());
                        writer.writeRecord(data.toParsedRowData());
                        writer.writeRecord(data.toParsedOldData());
                    } catch (IOException e) {
                        log.warn("Failed to write parsed row data from sym_data to file " + filenameParsed, e);
                    } finally {
                        if (writer != null) {
                            writer.close();
                        }
                    }
                } else {
                    log.warn("Could not find data ID: " + batch.getFailedDataId() + " for batch ID: " + batch.getBatchId() + " in error");
                }
            }
        }
    }

}
